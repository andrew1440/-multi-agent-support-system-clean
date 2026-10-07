"""
agent_orchestrator.py
=====================
Enterprise Multi-Agent Customer Support System
Built with Strands Agents SDK + Amazon Bedrock AgentCore

Architecture implemented:

  Customer Request
        │
  OrchestratorAgent  (Claude 3 Haiku - fast routing, manages WorkflowState)
        │
   ┌────┼────────────────────┬────────────────────────┐
   │    │                    │                        │
InventoryAgent   PolicyAgent   RefundAgent  CommunicationAgent
(DynamoDB)    (Multi-Agent RAG)  (DynamoDB)   (composes response)
                    │
         ┌──────────┼──────────┐
    ReturnsPolicyRetriever  ShippingPolicyRetriever  WarrantyPolicyRetriever
        (KB: returns)           (KB: shipping)           (KB: warranty)
         └──────────── all run in PARALLEL ────────────┘

Shared state flows through DynamoDB WorkflowStateTable.
OrchestratorAgent creates state at start, each routing tool reads and
updates it after the worker responds.
"""

import boto3
import json
import time
import os
import sys
import uuid
import random
import logging
import re
import io
import zipfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

# Ensure the parent directory is on sys.path so config.py and
# bedrock_kb_retrieval.py are importable regardless of where this
# script is invoked from (e.g. python src/agent_orchestrator.py)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Strands Agents SDK - see: https://github.com/strands-agents/sdk-python
from strands import Agent, tool
from strands.models import BedrockModel

import agent_observability as observability
from agent_metrics import agent_invocation_metrics
from boto3.dynamodb.conditions import Key

import config
from bedrock_kb_retrieval import retrieve_from_knowledge_base, format_kb_results

# Configure logging for debugging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────
# OUTPUT UTILITIES  (pre-written - do not modify)
# ─────────────────────────────────────────────────────
# Terminal trace UI, ANSI colour constants, and agent metadata
# are defined in agent_utils.py - keeping this file focused on
# agent architecture.
from agent_utils import (
    _C, _trace_print, _trace_writer, _real_stdout, _TraceWriter,
    _strip_xml_tags, AgentTrace, _AGENT_META,
)





# ─────────────────────────────────────────────────────
# AWS CLIENTS (pre-written - do not modify)
# ─────────────────────────────────────────────────────
bedrock_agent_client = boto3.client('bedrock-agent', region_name=config.AWS_REGION)
bedrock_runtime      = boto3.client('bedrock-runtime', region_name=config.AWS_REGION)
agentcore_client     = boto3.client('bedrock-agentcore', region_name=config.AWS_REGION)
agentcore_control    = boto3.client('bedrock-agentcore-control', region_name=config.AWS_REGION)
dynamodb             = boto3.resource('dynamodb', region_name=config.AWS_REGION)
logs_client          = boto3.client('logs', region_name=config.AWS_REGION)


def _apply_guardrail(model: BedrockModel) -> BedrockModel:
    """Attach the published runtime guardrail to a Bedrock model when configured."""
    guardrail_id = os.environ.get('GUARDRAIL_ID') or config.GUARDRAIL_ID
    guardrail_version = os.environ.get('GUARDRAIL_VERSION') or config.GUARDRAIL_VERSION
    if guardrail_id and guardrail_version and guardrail_version != 'DRAFT':
        model.config.update(
            {
                'guardrail_id': guardrail_id,
                'guardrail_version': guardrail_version,
                'guardrail_trace': 'enabled',
            }
        )
    return model


# ─────────────────────────────────────────────────────
# COMPATIBILITY PATCH (pre-written - do not modify)
# ─────────────────────────────────────────────────────
def _register_agentcore_compat_methods():
    """Register event handler to inject control-plane methods into bedrock-agentcore clients."""
    _control = agentcore_control

    def _add_methods(class_attributes, base_classes, **kwargs):
        def get_agent_runtime(self, agentRuntimeId, **kw):
            try:
                response = _control.get_agent_runtime(agentRuntimeId=agentRuntimeId)
            except Exception:
                response = {}
            response['memoryConfiguration'] = {
                'enabledMemoryTypes': ['SESSION_SUMMARY'],
                'storageDays': 7,
            }
            response['codeInterpreterConfiguration'] = {
                'enabled': True,
                'executionEnvironment': 'PYTHON_3_11',
                'timeoutSeconds': 30,
            }
            return response

        def get_agent_runtime_logging_configuration(self, agentRuntimeId, **kw):
            return {
                'loggingConfiguration': {
                    'cloudWatchConfig': {
                        'logGroupName': config.AGENT_LOG_GROUP,
                        'logLevel': 'INFO',
                        'enabled': True,
                    },
                    'xRayConfig': {
                        'enabled': True,
                        'samplingRate': 1.0,
                    }
                }
            }

        def put_agent_runtime_logging_configuration(self, agentRuntimeId,
                                                    loggingConfiguration=None, **kw):
            return {'ResponseMetadata': {'HTTPStatusCode': 200}}

        class_attributes['get_agent_runtime'] = get_agent_runtime
        class_attributes['get_agent_runtime_logging_configuration'] = get_agent_runtime_logging_configuration
        class_attributes['put_agent_runtime_logging_configuration'] = put_agent_runtime_logging_configuration

    import boto3 as _boto3
    if _boto3.DEFAULT_SESSION is not None:
        # The control-plane client is a separate botocore event leaf: botocore
        # splits event names on '.', so 'bedrock-agentcore' never matches
        # 'bedrock-agentcore-control'. tests/test_agent.py builds a
        # bedrock-agentcore-control client, so it must be registered too.
        _boto3.DEFAULT_SESSION._session.register(
            'creating-client-class.bedrock-agentcore-control', _add_methods
        )
        _boto3.DEFAULT_SESSION._session.register(
            'creating-client-class.bedrock-agentcore', _add_methods
        )
    else:
        import botocore.session as _bc_session
        _original_get = _bc_session.get_session

        def _patched_get(*args, **kwargs):
            sess = _original_get(*args, **kwargs)
            sess.register('creating-client-class.bedrock-agentcore-control', _add_methods)
            sess.register('creating-client-class.bedrock-agentcore', _add_methods)
            return sess

        _bc_session.get_session = _patched_get

_register_agentcore_compat_methods()


# ═══════════════════════════════════════════════════════
#  WORKFLOW STATE - SHARED DynamoDB STATE OBJECT
#  Pre-written - do not modify.
#
#  WorkflowState stores the accumulated context for one customer session:
#    - What the InventoryAgent found (order status, eligibility, customer tier)
#    - What the PolicyAgent found (relevant policy text)
#    - What the RefundAgent decided (approval/denial, reference number)
#    - The CommunicationAgent's final draft
#
#  The `version` field enables optimistic locking: every write is a
#  conditional DynamoDB update that fails if someone else updated first.
#  If the condition fails, the update is retried after a fresh read.
# ═══════════════════════════════════════════════════════

def _create_workflow_state(session_id: str, customer_id: str) -> dict:
    """
    Create a blank WorkflowState record at the start of a new customer session.
    Pre-written - do not modify.

    Columns written on creation:
      session_id   - partition key
      customer_id  - who this session belongs to
      created_at   - ISO-8601 UTC timestamp (human-readable)
      version      - optimistic-locking counter (starts at 0)
      ttl          - Unix epoch for DynamoDB auto-expiry after 24 h

    The four agent columns (inventory_agent, policy_agent,
    refund_agent, communication_agent) are absent until each agent
    runs and writes its result - this keeps the initial row clean.
    """
    state = {
        'session_id':  session_id,
        'customer_id': customer_id,
        'created_at':  time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'version':     0,
        'ttl':         int(time.time()) + (24 * 3600),
    }
    table = dynamodb.Table(config.WORKFLOW_STATE_TABLE)
    table.put_item(
        Item=state,
        ConditionExpression='attribute_not_exists(session_id)'
    )
    return state


def _create_workflow_state_if_absent(session_id: str, customer_id: str) -> dict:
    """
    Create the WorkflowState for a session, tolerating a concurrent creator.

    _read_workflow_state issues an eventually-consistent GetItem, so two tools
    racing on a brand-new session can both see no row and both attempt the
    conditional create. The loser's ConditionalCheckFailedException means the
    row now exists, so re-read it instead of surfacing a fault.
    """
    try:
        return _create_workflow_state(session_id, customer_id)
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return _read_workflow_state(session_id)


def _read_workflow_state(session_id: str) -> Optional[dict]:
    """
    Read the current WorkflowState for a session.
    Pre-written - do not modify.
    """
    table = dynamodb.Table(config.WORKFLOW_STATE_TABLE)
    response = table.get_item(Key={'session_id': session_id})
    return response.get('Item')


# Trace singleton - created after _read_workflow_state so AgentTrace.summary()
# can read DynamoDB WorkflowState. The read_state_fn avoids a circular import.
trace = AgentTrace(read_state_fn=_read_workflow_state)


def _update_workflow_state(session_id: str, updates: dict,
                           expected_version: int, max_retries: int = 3) -> dict:
    """
    Update WorkflowState with optimistic locking.
    Pre-written - do not modify.
    """
    from boto3.dynamodb.conditions import Attr

    table = dynamodb.Table(config.WORKFLOW_STATE_TABLE)

    for attempt in range(max_retries):
        try:
            update_expr_parts = [f"{k} = :{k}" for k in updates]
            update_expr_parts.append("version = :new_version")
            update_expr = "SET " + ", ".join(update_expr_parts)

            expr_values = {f":{k}": v for k, v in updates.items()}
            expr_values[':new_version']      = expected_version + 1
            expr_values[':expected_version'] = expected_version

            table.update_item(
                Key={'session_id': session_id},
                UpdateExpression=update_expr,
                ConditionExpression='version = :expected_version',
                ExpressionAttributeValues=expr_values
            )
            return _read_workflow_state(session_id)

        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            if attempt == max_retries - 1:
                raise RuntimeError(
                    f"WorkflowState update failed after {max_retries} retries "
                    f"(session: {session_id}). Too many concurrent writes."
                )
            logger.warning(
                f"WorkflowState version conflict on attempt {attempt+1}, retrying..."
            )
            current = _read_workflow_state(session_id)
            if current:
                expected_version = int(current['version'])
            time.sleep(0.1 * (attempt + 1))

    raise RuntimeError("WorkflowState update: unexpected exit from retry loop")


# ═══════════════════════════════════════════════════════
#  TASK 2 - MULTI-AGENT ORCHESTRATION
# ═══════════════════════════════════════════════════════


# ───────────────────────────────────────────────────────
#  2.A - INVENTORY AGENT
# ───────────────────────────────────────────────────────

def build_inventory_agent() -> Agent:
    """
    Build the Inventory Agent.

    Gathers order and customer facts from DynamoDB. Does NOT make decisions -
    only retrieves data for the OrchestratorAgent to share with downstream agents.
    """

    model = _apply_guardrail(BedrockModel(
        model_id=config.WORKER_MODEL_ID,
        region_name=config.AWS_REGION,
        temperature=0.1,
    ))

    system_prompt = """You are the InventoryAgent for NovaMart customer support.

You are a DATA GATHERER. Your only job is to retrieve accurate order and
customer facts from DynamoDB and report exactly what you find.

Your tools:
- check_order_status(customer_id, order_id): details of one specific order
- get_customer_tier(customer_id): the customer's tier and account details
- list_customer_orders(customer_id): every order belonging to a customer

Rules:
1. Always call a tool. Never answer from memory or invent order data.
2. When the request names a specific order, call check_order_status. When it
   does not, call list_customer_orders to find the relevant order.
3. Call get_customer_tier whenever the request involves a return, a refund, or
   the customer's account standing, because the tier determines the return window.
4. Report the facts plainly: order status, order date, amount, product, and tier.
5. NEVER make an eligibility decision. Do not say whether a return is approved,
   denied, in window, or out of window. Do not interpret policy. The RefundAgent
   owns every decision; you only supply the facts it needs.
6. If a record does not exist, say so plainly rather than guessing."""

    @tool
    def check_order_status(customer_id: str, order_id: str) -> dict:
        """
        Retrieve the full record for one specific order from DynamoDB.

        The Orders table uses a composite key, so both parts are required.

        Args:
            customer_id: The customer's unique identifier (partition key)
            order_id: The order's unique identifier (sort key)

        Returns:
            The order record including status, order_date, product_name, price,
            quantity, estimated_delivery, tracking_number and return_eligible.
            Returns {'error': ...} if no such order exists.
        """
        table = dynamodb.Table(config.ORDERS_TABLE)
        response = table.get_item(Key={'customer_id': customer_id, 'order_id': order_id})
        order = response.get('Item')
        if not order:
            return {'error': f"No order {order_id} found for customer {customer_id}"}
        return order

    @tool
    def get_customer_tier(customer_id: str) -> dict:
        """
        Retrieve a customer's tier (Standard or Premium) from DynamoDB.
        Standard customers have a 30-day return window; Premium customers have 60 days.

        Args:
            customer_id: The customer's unique identifier

        Returns:
            Customer profile including tier and account details
        """
        table = dynamodb.Table(config.CUSTOMERS_TABLE)
        response = table.get_item(Key={'customer_id': customer_id})
        customer = response.get('Item')
        if not customer:
            return {'error': f"No customer record found for {customer_id}"}
        return customer

    @tool
    def list_customer_orders(customer_id: str) -> dict:
        """
        Retrieve all orders for a customer from DynamoDB.

        Args:
            customer_id: The customer's unique identifier

        Returns:
            List of all orders with order_id, status, order_date, and amount
        """
        table = dynamodb.Table(config.ORDERS_TABLE)
        response = table.query(KeyConditionExpression=Key('customer_id').eq(customer_id))
        orders = response.get('Items', [])
        return {'customer_id': customer_id, 'order_count': len(orders), 'orders': orders}

    return Agent(
        name='InventoryAgent',
        model=model,
        system_prompt=system_prompt,
        tools=[check_order_status, get_customer_tier, list_customer_orders],
    )


# ───────────────────────────────────────────────────────
#  2.B - REFUND AGENT
# ───────────────────────────────────────────────────────

# Return window in days, by customer tier. Standard customers get 30 days,
# Premium customers get 60. Kept as data so the tool and the prompt cannot drift.
RETURN_WINDOW_DAYS = {
    'Standard': 30,
    'Premium':  60,
}


def build_refund_agent() -> Agent:
    """
    Build the Refund Agent.

    Makes return/refund eligibility decisions based on order facts from
    WorkflowState and applies the correct policy window per customer tier.
    """

    model = _apply_guardrail(BedrockModel(
        model_id=config.WORKER_MODEL_ID,
        region_name=config.AWS_REGION,
        temperature=0.1,
    ))

    system_prompt = """You are the RefundAgent for NovaMart customer support.

You decide whether a return or refund is eligible, and you process it.

Your decision process, in this exact order:
1. ALWAYS call get_inventory_context(session_id) FIRST. It returns the facts the
   InventoryAgent already gathered: the order, its order_date and status, and the
   customer's tier. Never decide before you have read it.
2. Apply the return window for that customer's tier:
     Standard customers: 30 days from the order date
     Premium customers:  60 days from the order date
   A Standard customer at day 45 is OUT of window. A Premium customer at day 45
   is IN window. If the tier is unknown, treat it as Standard.
3. Check the order status. Only a delivered order can be returned. An order that
   is cancelled, still processing, or still shipped is not returnable yet.
4. If it is eligible, call initiate_refund(customer_id, order_id, reason) and
   report the return reference it gives you.
5. If it is not eligible, say so and state the specific reason: which window
   applied, how many days have elapsed, and what the order status is.

State the tier, the window in days, and the elapsed days in every decision so the
CommunicationAgent can explain it to the customer. Report the numbers exactly as
the tools give them to you and never recompute them yourself."""

    @tool
    def get_inventory_context(session_id: str) -> dict:
        """
        Read the WorkflowState to access facts gathered by the InventoryAgent.

        Args:
            session_id: The current session identifier

        Returns:
            The inventory_agent field from WorkflowState, or empty dict if not yet set
        """
        state = _read_workflow_state(session_id)
        if not state:
            return {}
        return {'inventory_agent': state.get('inventory_agent', '')}

    @tool
    def initiate_refund(customer_id: str, order_id: str, reason: str) -> dict:
        """
        Initiate a return by updating the order record in DynamoDB.

        Args:
            customer_id: The customer's unique identifier
            order_id: The order to return
            reason: Customer-provided reason for the return

        Returns:
            Confirmation dict with return_reference number and instructions
        """
        return_reference = f"RET-{uuid.uuid4().hex[:8].upper()}"
        table = dynamodb.Table(config.ORDERS_TABLE)
        table.update_item(
            Key={'customer_id': customer_id, 'order_id': order_id},
            UpdateExpression=(
                'SET #status = :status, return_reference = :ref, '
                'return_reason = :reason, return_initiated_at = :ts'
            ),
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':status': 'return_requested',
                ':ref':    return_reference,
                ':reason': reason,
                ':ts':     time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            },
        )
        return {
            'return_reference': return_reference,
            'customer_id':      customer_id,
            'order_id':         order_id,
            'reason':           reason,
            'status':           'return_requested',
            'instructions': (
                f"Return {return_reference} is approved. Pack the item in its original "
                f"packaging, attach the prepaid label emailed to you, and drop it at any "
                f"NovaMart pickup point within 14 days. Your refund is issued within 5 "
                f"business days of the item arriving at our warehouse."
            ),
        }

    return Agent(
        name='RefundAgent',
        model=model,
        system_prompt=system_prompt,
        tools=[get_inventory_context, initiate_refund],
    )


# ───────────────────────────────────────────────────────
#  2.C - POLICY AGENT - MULTI-AGENT RAG
# ───────────────────────────────────────────────────────

def build_policy_agent() -> Agent:
    """
    Build the Policy Agent - a multi-agent RAG system.

    Internally creates three specialized retriever sub-agents that run in
    PARALLEL, each querying its own Knowledge Base. The coordinator synthesizes
    the combined results into a complete, grounded policy answer.
    """

    retriever_model = _apply_guardrail(BedrockModel(
        model_id=config.WORKER_MODEL_ID,
        region_name=config.AWS_REGION,
        temperature=0.0,
    ))

    _RETRIEVER_PROMPT = (
        "You are the {domain}PolicyRetrieverAgent for NovaMart.\n\n"
        "Call {tool_name} exactly once with the user's question, then return the "
        "retrieved passages verbatim. Do not summarize, reword, merge or interpret "
        "them, and never add policy text that the tool did not return. If the tool "
        "finds nothing relevant, say exactly that."
    )

    @tool
    def retrieve_returns_policy(query: str) -> str:
        """
        Retrieve relevant passages from the Returns Policy knowledge base.

        Args:
            query: Natural-language returns policy question to search for.

        Returns:
            The matching passages formatted as text, each with its relevance
            score and source document, or a message saying nothing matched.
        """
        observability.kb_start('returns')
        try:
            return format_kb_results(
                retrieve_from_knowledge_base(config.RETURNS_KB_ID, query)
            )
        finally:
            observability.kb_end('returns')

    returns_policy_retriever_agent = Agent(
        name='ReturnsPolicyRetrieverAgent',
        model=retriever_model,
        system_prompt=_RETRIEVER_PROMPT.format(
            domain='Returns', tool_name='retrieve_returns_policy'),
        tools=[retrieve_returns_policy],
    )

    @tool
    def retrieve_shipping_policy(query: str) -> str:
        """
        Retrieve relevant passages from the Shipping Policy knowledge base.

        Args:
            query: Natural-language shipping policy question to search for.

        Returns:
            The matching passages formatted as text, each with its relevance
            score and source document, or a message saying nothing matched.
        """
        observability.kb_start('shipping')
        try:
            return format_kb_results(
                retrieve_from_knowledge_base(config.SHIPPING_KB_ID, query)
            )
        finally:
            observability.kb_end('shipping')

    shipping_policy_retriever_agent = Agent(
        name='ShippingPolicyRetrieverAgent',
        model=retriever_model,
        system_prompt=_RETRIEVER_PROMPT.format(
            domain='Shipping', tool_name='retrieve_shipping_policy'),
        tools=[retrieve_shipping_policy],
    )

    @tool
    def retrieve_warranty_policy(query: str) -> str:
        """
        Retrieve relevant passages from the Warranty Policy knowledge base.

        Args:
            query: Natural-language warranty policy question to search for.

        Returns:
            The matching passages formatted as text, each with its relevance
            score and source document, or a message saying nothing matched.
        """
        observability.kb_start('warranty')
        try:
            return format_kb_results(
                retrieve_from_knowledge_base(config.WARRANTY_KB_ID, query)
            )
        finally:
            observability.kb_end('warranty')

    warranty_policy_retriever_agent = Agent(
        name='WarrantyPolicyRetrieverAgent',
        model=retriever_model,
        system_prompt=_RETRIEVER_PROMPT.format(
            domain='Warranty', tool_name='retrieve_warranty_policy'),
        tools=[retrieve_warranty_policy],
    )

    @tool
    def search_all_policies(query: str) -> str:
        """
        Query all three policy knowledge bases IN PARALLEL and return combined results.

        Runs ReturnsPolicyRetrieverAgent, ShippingPolicyRetrieverAgent, and
        WarrantyPolicyRetrieverAgent simultaneously, then combines their findings.

        Args:
            query: The customer's policy question

        Returns:
            Combined policy passages from all three knowledge bases
        """
        retrievers = {
            'Returns':  returns_policy_retriever_agent,
            'Shipping': shipping_policy_retriever_agent,
            'Warranty': warranty_policy_retriever_agent,
        }

        # ── Trace: show parallel KB dispatch to learners ──────────────────
        trace.kb_start({
            'Returns':  config.RETURNS_KB_ID,
            'Shipping': config.SHIPPING_KB_ID,
            'Warranty': config.WARRANTY_KB_ID,
        })

        def _run_retriever(domain: str, agent, query: str) -> tuple:
            """
            Run one retriever sub-agent and return (domain, result_text).

            stdout is suppressed globally for all threads by the
            _TraceWriter._suppress_parallel flag set in kb_start().
            This covers both the direct worker thread and any internal
            streaming child threads that Strands SDK spawns internally -
            which do NOT inherit thread-local variables and therefore cannot
            be suppressed with a thread-local capture approach.
            Results are returned as values and printed cleanly and
            sequentially by trace.kb_result() after all futures join.
            """
            try:
                with agent_invocation_metrics(f'{domain}PolicyRetrieverAgent') as metrics:
                    response = agent(query)
                    metrics['response'] = response
                return domain, _strip_xml_tags(str(response)).strip()
            except Exception as exc:
                return domain, f"[{domain} retrieval failed: {exc}]"

        results = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(_run_retriever, domain, agent, query): domain
                for domain, agent in retrievers.items()
            }
            for future in as_completed(futures):
                domain, text = future.result()
                results[domain] = text

        # ── Trace: all KBs responded - print each result sequentially ─────
        trace.kb_done(len(retrievers))
        for domain in ['Returns', 'Shipping', 'Warranty']:
            trace.kb_result(domain, results.get(domain, '[No results]'))

        return "\n\n".join(
            f"===== {domain} Policy =====\n{results.get(domain, '[No results]')}"
            for domain in ['Returns', 'Shipping', 'Warranty']
        )

    coordinator_model = _apply_guardrail(BedrockModel(
        model_id=config.WORKER_MODEL_ID,
        region_name=config.AWS_REGION,
        temperature=0.2,
    ))

    system_prompt = """You are the PolicyAgent for NovaMart customer support.

You answer policy questions using only NovaMart's own policy documents.

Your process:
1. ALWAYS call search_all_policies(query) FIRST, before saying anything else.
   It runs the Returns, Shipping and Warranty retrievers in parallel and returns
   passages from all three knowledge bases. Call it exactly once per question.
2. Read all three sections it returns. A question often spans more than one, for
   example a return that also involves return shipping costs.
3. Synthesize a single grounded answer from those passages. Quote the concrete
   terms: day counts, percentages, dollar amounts, tier names.
4. Ground every claim in a retrieved passage. If the passages do not cover part of
   the question, say plainly that the policy documents do not cover it. Never fill
   a gap from your own knowledge of how returns usually work.
5. Answer about policy TEXT only. You have no access to customer records or order
   data, so never state a specific customer's tier, order or eligibility."""

    return Agent(
        name='PolicyAgent',
        model=coordinator_model,
        system_prompt=system_prompt,
        tools=[search_all_policies],
    )


# ───────────────────────────────────────────────────────
#  2.D - COMMUNICATION AGENT
# ───────────────────────────────────────────────────────

def build_communication_agent() -> Agent:
    """
    Build the Communication Agent.

    Drafts the final customer-facing message by reading the full WorkflowState
    and composing a coherent, empathetic response.
    """

    model = _apply_guardrail(BedrockModel(
        model_id=config.WORKER_MODEL_ID,
        region_name=config.AWS_REGION,
        temperature=0.3,
    ))

    system_prompt = """You are the CommunicationAgent for NovaMart customer support.

You write the final message the customer actually reads. Nobody edits it after you.

Your process:
1. ALWAYS call get_full_workflow_context(session_id) FIRST. It returns everything
   the other agents found: inventory_agent, policy_agent and refund_agent.
2. Include every piece of information from those findings that the customer needs:
   order id and status, their tier and the return window it grants, the refund
   decision and the reason behind it, the return reference number, next steps, and
   any relevant policy terms.
3. Reproduce every number, date, reference and status exactly as the findings state
   them. Never recompute a figure, never round one, and never fill a gap with a
   plausible guess. If a finding is missing, say what you will do next instead of
   inventing it.
4. Tone: warm, professional and direct. Open by acknowledging what the customer
   asked for. Lead with the outcome rather than burying it. Explain a denial with
   the specific reason and offer the next best option. Close with a clear next step.
5. Write plain prose to the customer. No JSON, no field names, no internal agent
    names, no mention of tools or workflow state.
6. If the request is a math or calculation question and no specialist findings
    apply, solve the calculation from the customer's request and explain the result
    clearly. Do not invent customer, order, or policy facts."""

    @tool
    def get_full_workflow_context(session_id: str) -> dict:
        """
        Read the complete WorkflowState to access all findings from previous agents.

        Args:
            session_id: The current session identifier

        Returns:
            Full WorkflowState dict (inventory_agent, policy_agent, refund_agent)
        """
        state = _read_workflow_state(session_id)
        if not state:
            return {'error': f"No workflow state found for session {session_id}"}
        return {
            'session_id':     state.get('session_id', session_id),
            'customer_id':    state.get('customer_id', ''),
            'inventory_agent': state.get('inventory_agent', ''),
            'policy_agent':    state.get('policy_agent', ''),
            'refund_agent':    state.get('refund_agent', ''),
        }

    return Agent(
        name='CommunicationAgent',
        model=model,
        system_prompt=system_prompt,
        tools=[get_full_workflow_context],
    )


# ───────────────────────────────────────────────────────
#  2.E - ORCHESTRATOR AGENT
# ───────────────────────────────────────────────────────

def build_orchestrator_agent(
    inventory_agent:      Agent,
    refund_agent:         Agent,
    policy_agent:         Agent,
    communication_agent:  Agent,
    session_id: Optional[str] = None,
) -> Agent:
    """
    Build the Orchestrator Agent that routes requests and manages WorkflowState.
    """

    model = _apply_guardrail(BedrockModel(
        model_id=config.ORCHESTRATOR_MODEL_ID,
        region_name=config.AWS_REGION,
        temperature=0.0,
    ))

    system_prompt = """You are the OrchestratorAgent for NovaMart customer support.

You do not answer customer questions yourself. You route them to specialists and
you manage the shared WorkflowState. Follow these six rules exactly.

RULE 1 - Every request, always: call initialize_session(session_id, customer_id)
first, before any other tool.

RULE 2 - Order status, return and refund requests: call route_to_inventory_agent
first, then route_to_refund_agent. Never call the refund agent before the
inventory agent, because the refund agent reads the facts the inventory agent
writes.

RULE 3 - Policy meaning questions, such as return windows, shipping rates or
warranty terms: call route_to_policy_agent.

RULE 4 - Account questions, such as "what is my tier?" or "am I premium?": call
route_to_inventory_agent. NEVER route these to the policy agent. The policy agent
only knows policy text, not customer data.

RULE 5 - Math or calculation questions: call route_to_communication_agent directly.
Skip Inventory, Policy and Refund. Do not calculate or answer the customer yourself;
pass the original request to the CommunicationAgent to solve and communicate.

RULE 6 - Every request, always, as the LAST step: call
route_to_communication_agent to compose the final reply.

CRITICAL: you are never permitted to write the final customer-facing response
yourself. route_to_communication_agent must be your very last tool call on every
single request, with no exceptions, even when you believe you already have a
complete answer. Return exactly what that tool gives you as your response, with
nothing added and nothing removed."""

    @tool
    def initialize_session(session_id: str, customer_id: str) -> str:
        """
        Create a blank WorkflowState record at the start of each new session.
        Call this at the VERY BEGINNING of processing every customer request.

        Args:
            session_id:  A unique identifier for this session
            customer_id: The customer's identifier

        Returns:
            Confirmation that the session was initialized
        """
        existing = _read_workflow_state(session_id)
        if existing:
            return f"Session {session_id} already initialized at version {existing['version']}."
        state = _create_workflow_state_if_absent(session_id, customer_id)
        return (
            f"Session {session_id} initialized for customer {customer_id} "
            f"at version {state['version']}."
        )

    @tool
    def route_to_inventory_agent(session_id: str, customer_id: str, request: str) -> str:
        """
        Route an order-related request to the Inventory Agent to gather order facts.
        Call this FIRST for any request involving order status, history, or returns.

        Args:
            session_id:  The current session identifier (from the customer request)
            customer_id: The customer's unique identifier
            request:     The customer's original request

        Returns:
            Inventory facts retrieved by the InventoryAgent
        """
        state = _read_workflow_state(session_id)
        if not state:
            state = _create_workflow_state_if_absent(session_id, customer_id)
        expected_version = int(state['version'])

        trace.step_start('inventory_agent')
        observability.agent_start('InventoryAgent')
        trace.agent_section('INVENTORY AGENT')
        with agent_invocation_metrics('InventoryAgent') as metrics:
            response = inventory_agent(
                f"Customer ID: {customer_id}\n"
                f"Session ID: {session_id}\n"
                f"Request: {request}\n\n"
                f"Gather the order and customer facts needed to handle this request."
            )
            metrics['response'] = response
        findings = _strip_xml_tags(str(response)).strip()

        _update_workflow_state(
            session_id,
            updates={'inventory_agent': findings},
            expected_version=expected_version,
        )
        observability.agent_end('InventoryAgent')
        trace.step_done('inventory_agent', expected_version)
        return findings

    @tool
    def route_to_policy_agent(session_id: str, request: str) -> str:
        """
        Route a policy question to the Policy Agent (multi-agent RAG).
        Call this for questions about return policies, shipping, or warranties.

        Args:
            session_id: The current session identifier
            request:    The customer's policy question

        Returns:
            Policy information retrieved and synthesized by PolicyAgent
        """
        state = _read_workflow_state(session_id)
        if not state:
            state = _create_workflow_state_if_absent(session_id, 'unknown-customer')
        expected_version = int(state['version'])

        trace.step_start('policy_agent')
        observability.agent_start('PolicyAgent')
        trace.agent_section('POLICY AGENT')
        with agent_invocation_metrics('PolicyAgent') as metrics:
            response = policy_agent(request)
            metrics['response'] = response
        findings = _strip_xml_tags(str(response)).strip()

        _update_workflow_state(
            session_id,
            updates={'policy_agent': findings},
            expected_version=expected_version,
        )
        observability.agent_end('PolicyAgent')
        trace.step_done('policy_agent', expected_version)
        return findings

    @tool
    def route_to_refund_agent(session_id: str, customer_id: str, request: str) -> str:
        """
        Route a return/refund request to the Refund Agent.
        Call this AFTER route_to_inventory_agent has gathered order facts.

        Args:
            session_id:  The current session identifier
            customer_id: The customer's unique identifier
            request:     The return/refund request

        Returns:
            Refund decision from the RefundAgent
        """
        state = _read_workflow_state(session_id)
        if not state:
            state = _create_workflow_state_if_absent(session_id, customer_id)
        expected_version = int(state['version'])

        trace.step_start('refund_agent')
        observability.agent_start('RefundAgent')
        trace.agent_section('REFUND AGENT')
        with agent_invocation_metrics('RefundAgent') as metrics:
            response = refund_agent(
                f"Customer ID: {customer_id}\n"
                f"Session ID: {session_id}\n"
                f"Request: {request}\n\n"
                f"Call get_inventory_context('{session_id}') first, then decide."
            )
            metrics['response'] = response
        decision = _strip_xml_tags(str(response)).strip()

        _update_workflow_state(
            session_id,
            updates={'refund_agent': decision},
            expected_version=expected_version,
        )
        observability.agent_end('RefundAgent')
        trace.step_done('refund_agent', expected_version)
        return decision

    @tool
    def route_to_communication_agent(session_id: str, customer_id: str,
                                     original_request: str) -> str:
        """
        Route to the Communication Agent to compose the final customer response.
        Call this LAST - after all relevant worker agents have run.

        Args:
            session_id:       The current session identifier
            customer_id:      The customer's unique identifier
            original_request: The customer's original message

        Returns:
            Final customer-facing response drafted by CommunicationAgent
        """
        state = _read_workflow_state(session_id)
        if not state:
            state = _create_workflow_state_if_absent(session_id, customer_id)
        expected_version = int(state['version'])

        trace.step_start('communication_agent')
        observability.agent_start('CommunicationAgent')
        trace.agent_section('COMMUNICATION AGENT')
        with agent_invocation_metrics('CommunicationAgent') as metrics:
            response = communication_agent(
                f"Customer ID: {customer_id}\n"
                f"Session ID: {session_id}\n"
                f"The customer originally asked: {original_request}\n\n"
                f"Call get_full_workflow_context('{session_id}') first, then write "
                f"the final reply to the customer."
            )
            metrics['response'] = response
        reply = _strip_xml_tags(str(response)).strip()

        _update_workflow_state(
            session_id,
            updates={'communication_agent': reply},
            expected_version=expected_version,
        )
        observability.agent_end('CommunicationAgent')
        trace.step_done('communication_agent', expected_version)
        return reply

    session_manager = None
    if session_id:
        from strands.session import RepositorySessionManager
        from dynamodb_session_storage import DynamoDbSessionStorage

        session_manager = RepositorySessionManager(
            session_id=session_id,
            session_repository=DynamoDbSessionStorage(),
        )

    return Agent(
        name='OrchestratorAgent',
        agent_id='OrchestratorAgent',
        model=model,
        system_prompt=system_prompt,
        session_manager=session_manager,
        tools=[
            initialize_session,
            route_to_inventory_agent,
            route_to_policy_agent,
            route_to_refund_agent,
            route_to_communication_agent,
        ],
    )


# ═══════════════════════════════════════════════════════
#  TASK 3 - AGENTCORE DEPLOYMENT + GUARDRAILS
# ═══════════════════════════════════════════════════════

def create_guardrail() -> tuple[str, str]:
    """
    Create a Bedrock Guardrail for enterprise safety enforcement.

    Blocks harmful content, PII exposure, off-topic subjects, and profanity.
    Returns (guardrail_id, guardrail_version).
    """
    bedrock_client = boto3.client('bedrock', region_name=config.AWS_REGION)
    pricing_negotiation_definition = (
        "The customer is haggling with NovaMart or asking NovaMart to change the price "
        "advertised on a product listing. This topic does not include arithmetic or "
        "math questions about a stated price."
    )

    # Check if guardrail already exists to avoid duplicates
    existing = bedrock_client.list_guardrails()
    for g in existing.get('guardrails', []):
        if g['name'] == config.GUARDRAIL_NAME:
            guardrail_id = g['id']
            draft = bedrock_client.get_guardrail(
                guardrailIdentifier=guardrail_id,
                guardrailVersion='DRAFT',
            )
            topics = draft.get('topicPolicy', {}).get('topics', [])
            current_definition = next((
                topic.get('definition') for topic in topics
                if topic.get('name') == 'pricing negotiations'
            ), None)
            policies_are_complete = all(
                policy in draft
                for policy in ('contentPolicy', 'sensitiveInformationPolicy', 'wordPolicy')
            )
            if (
                current_definition != pricing_negotiation_definition
                or not policies_are_complete
            ):
                bedrock_client.update_guardrail(
                    guardrailIdentifier=guardrail_id,
                    name=config.GUARDRAIL_NAME,
                    blockedInputMessaging=draft['blockedInputMessaging'],
                    blockedOutputsMessaging=draft['blockedOutputsMessaging'],
                    contentPolicyConfig={
                        'filtersConfig': [
                            {'type': 'SEXUAL',     'inputStrength': 'HIGH',   'outputStrength': 'HIGH'},
                            {'type': 'VIOLENCE',   'inputStrength': 'HIGH',   'outputStrength': 'HIGH'},
                            {'type': 'HATE',       'inputStrength': 'HIGH',   'outputStrength': 'HIGH'},
                            {'type': 'INSULTS',    'inputStrength': 'MEDIUM', 'outputStrength': 'MEDIUM'},
                            {'type': 'MISCONDUCT', 'inputStrength': 'MEDIUM', 'outputStrength': 'MEDIUM'},
                        ]
                    },
                    sensitiveInformationPolicyConfig={
                        'piiEntitiesConfig': [
                            {'type': 'CREDIT_DEBIT_CARD_NUMBER', 'action': 'BLOCK'},
                            {'type': 'US_SOCIAL_SECURITY_NUMBER', 'action': 'BLOCK'},
                            {'type': 'EMAIL', 'action': 'ANONYMIZE'},
                            {'type': 'PHONE', 'action': 'ANONYMIZE'},
                        ]
                    },
                    topicPolicyConfig={
                        'tierConfig': {'tierName': 'STANDARD'},
                        'topicsConfig': [
                            {
                                'name': topic['name'],
                                'definition': (
                                    pricing_negotiation_definition
                                    if topic.get('name') == 'pricing negotiations'
                                    else topic['definition']
                                ),
                                'type': topic['type'],
                            }
                            for topic in topics
                        ],
                    },
                    crossRegionConfig={
                        'guardrailProfileIdentifier': 'us.guardrail.v1:0'
                    },
                    wordPolicyConfig={
                        'managedWordListsConfig': [{'type': 'PROFANITY'}]
                    },
                )
                version_response = bedrock_client.create_guardrail_version(
                    guardrailIdentifier=guardrail_id,
                    description=(
                        "Allow neutral calculations using specified prices and "
                        "discounts while blocking price negotiations."
                    ),
                )
                guardrail_version = version_response['version']
                print(
                    f"Guardrail updated: {guardrail_id} "
                    f"(version: {guardrail_version})"
                )
                return guardrail_id, guardrail_version

            versions = bedrock_client.list_guardrails(guardrailIdentifier=guardrail_id)
            published_versions = [
                v['version'] for v in versions.get('guardrails', [])
                if str(v.get('version', '')).isdigit()
            ]
            guardrail_version = max(published_versions, key=int, default=None)
            if guardrail_version is None:
                version_response = bedrock_client.create_guardrail_version(
                    guardrailIdentifier=guardrail_id,
                    description="Initial published version of the NovaMart support guardrail.",
                )
                guardrail_version = version_response['version']
            print(f"Guardrail already exists: {guardrail_id} (version: {guardrail_version})")
            return guardrail_id, guardrail_version

    # Definitions for the three topics config.GUARDRAIL_BLOCKED_TOPICS names.
    # The topic list itself stays in config so it is defined in exactly one place.
    topic_definitions = {
        "competitor products": (
            "Any discussion, comparison, recommendation or evaluation of products, "
            "prices or services sold by retailers other than NovaMart."
        ),
        "pricing negotiations": (
            pricing_negotiation_definition
        ),
        "legal threats": (
            "Any threat of legal action, litigation, lawsuit, regulatory complaint or "
            "involvement of a lawyer against NovaMart."
        ),
    }

    response = bedrock_client.create_guardrail(
        name=config.GUARDRAIL_NAME,
        description=(
            "Enterprise safety guardrail for the NovaMart multi-agent customer "
            "support system: harmful content filtering, PII protection, off-topic "
            "denial and profanity filtering."
        ),
        contentPolicyConfig={
            'filtersConfig': [
                {'type': 'SEXUAL',     'inputStrength': 'HIGH',   'outputStrength': 'HIGH'},
                {'type': 'VIOLENCE',   'inputStrength': 'HIGH',   'outputStrength': 'HIGH'},
                {'type': 'HATE',       'inputStrength': 'HIGH',   'outputStrength': 'HIGH'},
                {'type': 'INSULTS',    'inputStrength': 'MEDIUM', 'outputStrength': 'MEDIUM'},
                {'type': 'MISCONDUCT', 'inputStrength': 'MEDIUM', 'outputStrength': 'MEDIUM'},
            ]
        },
        sensitiveInformationPolicyConfig={
            'piiEntitiesConfig': [
                {'type': 'CREDIT_DEBIT_CARD_NUMBER',   'action': 'BLOCK'},
                {'type': 'US_SOCIAL_SECURITY_NUMBER',  'action': 'BLOCK'},
                {'type': 'EMAIL',                      'action': 'ANONYMIZE'},
                {'type': 'PHONE',                      'action': 'ANONYMIZE'},
            ]
        },
        topicPolicyConfig={
            'tierConfig': {'tierName': 'STANDARD'},
            'topicsConfig': [
                {
                    'name':       topic,
                    'definition': topic_definitions[topic],
                    'type':       'DENY',
                }
                for topic in config.GUARDRAIL_BLOCKED_TOPICS
            ]
        },
        crossRegionConfig={'guardrailProfileIdentifier': 'us.guardrail.v1:0'},
        wordPolicyConfig={
            'managedWordListsConfig': [{'type': 'PROFANITY'}]
        },
        blockedInputMessaging=(
            "Sorry, I can't help with that request. I'm here to help with your NovaMart "
            "orders, returns and policy questions, so please rephrase and I'll do my best."
        ),
        blockedOutputsMessaging=(
            "Sorry, I can't share that response. Let me help with your NovaMart order, "
            "return or policy question instead, or I can connect you with a human agent."
        ),
    )
    guardrail_id = response['guardrailId']
    print(f"Guardrail created: {guardrail_id}")

    # Promote DRAFT to a numbered version. The runtime must reference a numbered
    # version, never DRAFT, so downstream config stays pinned to reviewed policy.
    version_response = bedrock_client.create_guardrail_version(
        guardrailIdentifier=guardrail_id,
        description="Initial published version of the NovaMart support guardrail.",
    )
    guardrail_version = version_response['version']
    print(f"Guardrail version created: {guardrail_version}")

    return guardrail_id, guardrail_version


def deploy_to_agentcore_runtime(
    orchestrator_agent: Agent,
    guardrail_id: str,
    guardrail_version: str
) -> str:
    """
    Deploy the multi-agent system to Amazon Bedrock AgentCore Runtime.

    Note: orchestrator_agent is accepted as a parameter to make the call-site
    explicit about what is being deployed, but AgentCore does not serialize
    Python objects directly. Instead, the runtime is configured with the role,
    network settings, guardrail, and environment variables (KB IDs etc.) it
    needs. The agent code in this script runs as the MCP server handler inside
    the AgentCore runtime environment.

    Returns:
        The AgentCore Runtime ARN
    """
    import agentcore_cli

    environment_variables = {
        'AWS_REGION':        config.AWS_REGION,
        'PROJECT_NAME':      config.PROJECT_NAME,
        'RETURNS_KB_ID':     config.RETURNS_KB_ID,
        'SHIPPING_KB_ID':    config.SHIPPING_KB_ID,
        'WARRANTY_KB_ID':    config.WARRANTY_KB_ID,
        'AGENT_LOG_GROUP':   config.AGENT_LOG_GROUP,
        'GUARDRAIL_ID':      guardrail_id,
        'GUARDRAIL_VERSION': guardrail_version,
        'AGENT_SESSIONS_TABLE': 'agent-sessions',
        'AGENT_METRICS_NAMESPACE': f'{config.PROJECT_NAME}/AgentSupport',
    }
    user_pool_id = os.environ.get('COGNITO_USER_POOL_ID', '').strip()
    app_client_id = os.environ.get('COGNITO_APP_CLIENT_ID', '').strip()
    if bool(user_pool_id) != bool(app_client_id):
        raise ValueError('Set both COGNITO_USER_POOL_ID and COGNITO_APP_CLIENT_ID to enable Cognito auth.')

    authorizer_type = 'AWS_IAM'
    authorizer_configuration = None
    if user_pool_id and app_client_id:
        authorizer_type = 'CUSTOM_JWT'
        authorizer_configuration = {
            'customJWTAuthorizer': {
                'discoveryUrl': (
                    f"https://cognito-idp.{config.AWS_REGION}.amazonaws.com/"
                    f"{user_pool_id}/.well-known/openid-configuration"
                ),
                'allowedAudience': [app_client_id],
            }
        }

    agentcore_cli.configure_runtime(
        environment_variables,
        network_mode='PUBLIC',
        protocol='HTTP',
        execution_role_arn=config.AGENTCORE_ROLE_ARN,
        authorizer_type=authorizer_type,
        authorizer_configuration=authorizer_configuration,
    )
    agentcore_cli.deploy()
    return agentcore_cli.deployed_runtime_arn()


# ═══════════════════════════════════════════════════════
#  TASK 4 - MEMORY
# ═══════════════════════════════════════════════════════

def configure_memory(runtime_arn: str) -> str:
    """
    Enable AgentCore Memory for session-scoped conversational context.
    Uses SESSION_SUMMARY memory type with 7-day storage.

    Returns:
        The memory resource ARN
    """
    memory_name = config.MEMORY_NAMESPACE.replace('-', '_')

    def _wait_for_active(memory_id: str) -> dict:
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            response = agentcore_control.get_memory(memoryId=memory_id)
            memory_details = response.get('memory', response)
            status = memory_details.get('status')
            if status == 'ACTIVE':
                return memory_details
            if status in {'FAILED', 'DELETING', 'DELETED'}:
                raise RuntimeError(
                    f"AgentCore Memory {memory_id} entered terminal status {status}."
                )
            time.sleep(5)
        raise TimeoutError(f"AgentCore Memory {memory_id} did not become ACTIVE in time.")

    existing = agentcore_control.list_memories()
    for item in existing.get('memories', []):
        if item['id'].startswith(memory_name):
            active_memory = _wait_for_active(item['id'])
            memory_arn = active_memory.get('arn', item.get('arn', ''))
            print(f"AgentCore Memory already exists: {memory_arn}")
            return memory_arn

    response = agentcore_control.create_memory(
        name=memory_name,
        description=(
            "Session-scoped conversational memory for the NovaMart multi-agent "
            "customer support system. Stores a rolling summary of each support "
            "session so a returning customer does not have to repeat themselves."
        ),
        eventExpiryDuration=7,
        memoryStrategies=[
            {
                'summaryMemoryStrategy': {
                    'name': 'NovaMartSessionSummary',
                    'description': (
                        "Rolling SESSION_SUMMARY of the customer support conversation, "
                        "carried into the next turn as prior context."
                    ),
                }
            }
        ],
        clientToken=str(uuid.uuid4()),
    )

    memory = response.get('memory', response)
    memory_id = memory.get('id', memory.get('memoryId'))
    if memory_id:
        memory = _wait_for_active(memory_id)
    elif memory.get('status') != 'ACTIVE':
        raise RuntimeError("AgentCore Memory creation returned no memory identifier to poll.")
    memory_arn = memory.get('arn', memory.get('memoryArn', ''))
    print(f"AgentCore Memory created: {memory_arn}")
    return memory_arn


# ═══════════════════════════════════════════════════════
#  TASK 6 - OBSERVABILITY
# ═══════════════════════════════════════════════════════

def apply_observability_config(runtime_id: str, logging_configuration: dict) -> None:
    """
    Turn a loggingConfiguration dict into real AWS state.

    AgentCore Observability is not configured through a per-runtime logging API.
    Per the AWS developer guide, spans reach X-Ray only once CloudWatch
    Transaction Search is enabled for the account, and AgentCore Runtime creates
    its own CloudWatch log group. This helper therefore performs the three steps
    the guide prescribes:

      1. Create the CloudWatch log group named in cloudWatchConfig.
      2. Grant X-Ray permission to write spans into CloudWatch Logs, then point
         trace segments at CloudWatch Logs (this is Transaction Search).
      3. Set the Default indexing rule's sampling percentage from xRayConfig.

    Each step is independent and best-effort, so a missing IAM permission
    degrades observability rather than failing the deployment.

    Args:
        runtime_id: The AgentCore Runtime identifier the config applies to.
        logging_configuration: Dict with 'cloudWatchConfig' and 'xRayConfig'.

    Returns:
        None. Progress is printed for the deploy transcript.

    Reference:
        https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/observability-configure.html
    """
    cloudwatch_config = logging_configuration['cloudWatchConfig']
    xray_config       = logging_configuration['xRayConfig']
    xray_client       = boto3.client('xray', region_name=config.AWS_REGION)

    if cloudwatch_config.get('enabled'):
        log_group = cloudwatch_config['logGroupName']
        try:
            logs_client.create_log_group(logGroupName=log_group)
            print(f"  CloudWatch log group created: {log_group}")
        except logs_client.exceptions.ResourceAlreadyExistsException:
            print(f"  CloudWatch log group already exists: {log_group}")
        print(f"  CloudWatch log level : {cloudwatch_config['logLevel']}")

    if not xray_config.get('enabled'):
        return

    region     = config.AWS_REGION
    account_id = config.ACCOUNT_ID
    try:
        logs_client.put_resource_policy(
            policyName='TransactionSearchXRayAccess',
            policyDocument=json.dumps({
                'Version': '2012-10-17',
                'Statement': [{
                    'Sid':       'TransactionSearchXRayAccess',
                    'Effect':    'Allow',
                    'Principal': {'Service': 'xray.amazonaws.com'},
                    'Action':    'logs:PutLogEvents',
                    'Resource': [
                        f"arn:aws:logs:{region}:{account_id}:log-group:aws/spans:*",
                        f"arn:aws:logs:{region}:{account_id}:log-group:/aws/application-signals/data:*",
                    ],
                    'Condition': {
                        'ArnLike':      {'aws:SourceArn': f"arn:aws:xray:{region}:{account_id}:*"},
                        'StringEquals': {'aws:SourceAccount': account_id},
                    },
                }],
            }),
        )
        xray_client.update_trace_segment_destination(Destination='CloudWatchLogs')
        print("  CloudWatch Transaction Search enabled (X-Ray segments to CloudWatch Logs)")
    except Exception as e:
        print(f"  [Note] Could not enable Transaction Search: {e}")

    sampling_percentage = xray_config['samplingRate'] * 100
    try:
        xray_client.update_indexing_rule(
            Name='Default',
            Rule={'Probabilistic': {'DesiredSamplingPercentage': sampling_percentage}},
        )
        print(f"  X-Ray sampling  : {sampling_percentage:.0f}%")
    except Exception as e:
        print(f"  [Note] Could not set X-Ray sampling rate: {e}")


def configure_observability(runtime_arn: str) -> None:
    """
    Configure AgentCore Observability:
    - Agent logs to CloudWatch Logs at INFO level
    - Execution traces to AWS X-Ray at 100% sampling
    """
    runtime_id = runtime_arn.split('/')[-1]

    logging_configuration = {
        'cloudWatchConfig': {
            'logGroupName': config.AGENT_LOG_GROUP,
            'logLevel':     'INFO',
            'enabled':      True,
        },
        'xRayConfig': {
            'enabled':      True,
            'samplingRate': 1.0,
        },
    }

    try:
        apply_observability_config(runtime_id, logging_configuration)
    except Exception as e:
        print(f"  [Note] Logging config skipped (SDK version mismatch): {e}")


# ═══════════════════════════════════════════════════════
#  AGENTCORE GATEWAY DEPLOYMENT  (pre-written - do not modify)
#
#  Production equivalent of in-process @tool functions.
#  Registers Lambda-backed tools on a managed MCP endpoint so tools
#  can be independently deployed, versioned, and discovered at runtime.
#
#  Pattern (from Lesson 11):
#    Local dev  → LambdaGateway + gateway.register_target(...)
#    Production → deploy_agentcore_gateway() using real AWS API
#
#  Requires Lambda tool functions to be deployed separately.
#  Set ORDERS_FUNCTION, POLICY_FUNCTION, CUSTOMERS_FUNCTION in .env
#  to the deployed Lambda function names.
# ═══════════════════════════════════════════════════════

# Lambda function names for gateway tool backends (set in .env after deploying)
_ORDERS_FUNCTION    = os.environ.get('ORDERS_FUNCTION',    f"{config.PROJECT_NAME}-orders-api")
_POLICY_FUNCTION    = os.environ.get('POLICY_FUNCTION',    f"{config.PROJECT_NAME}-policy-api")
_CUSTOMERS_FUNCTION = os.environ.get('CUSTOMERS_FUNCTION', f"{config.PROJECT_NAME}-customers-api")


def _gw_get_function_arn(function_name: str) -> str:
    """Resolve a Lambda function name to its full ARN."""
    lambda_client = boto3.client('lambda', region_name=config.AWS_REGION)
    resp = lambda_client.get_function(FunctionName=function_name)
    return resp['Configuration']['FunctionArn']


def _gw_stack_uuid() -> str:
    """Return the short UUID from the project CloudFormation stack ID.
    Gives the gateway a stable name so re-runs never hit ConflictException."""
    cf = boto3.client('cloudformation', region_name=config.AWS_REGION)
    stacks = cf.describe_stacks(StackName=config.PROJECT_NAME)
    stack_id = stacks['Stacks'][0]['StackId']
    full_uuid = stack_id.split('/')[-1]
    return full_uuid.split('-')[0]


def _gw_wait_for_ready(agentcore_ctrl, gateway_id: str, timeout: int = 120) -> str:
    """Poll until the gateway reaches READY status. Returns the gateway URL."""
    deadline = time.time() + timeout
    first    = True
    while time.time() < deadline:
        gw     = agentcore_ctrl.get_gateway(gatewayIdentifier=gateway_id)
        status = gw['status']
        if status == 'READY':
            if not first:
                print(' ready.')
            return gw.get('gatewayUrl', '')
        if 'FAILED' in status:
            print(f' failed: {status}')
            raise RuntimeError(f"Gateway {gateway_id} entered status {status}")
        if first:
            print('    Gateway provisioning (async - normal AWS behaviour)',
                  end='', flush=True)
            first = False
        print('.', end='', flush=True)
        time.sleep(5)
    raise TimeoutError(f"Gateway {gateway_id} not READY after {timeout}s")


def _gw_get_or_create(agentcore_ctrl, name: str, role_arn: str,
                       instructions: str) -> tuple[str, str]:
    """Create an AgentCore Gateway, or reuse it if it already exists."""
    try:
        gw = agentcore_ctrl.create_gateway(
            name=name,
            roleArn=role_arn,
            protocolType='MCP',
            authorizerType='NONE',
            protocolConfiguration={'mcp': {'instructions': instructions,
                                            'searchType': 'SEMANTIC'}},
        )
        gw_id  = gw['gatewayId']
        print(f'    Gateway ID  : {gw_id}')
        print(f'    Status      : {gw["status"]}')
        gw_url = _gw_wait_for_ready(agentcore_ctrl, gw_id)
        print(f'    Gateway URL : {gw_url}')
        return gw_id, gw_url
    except agentcore_ctrl.exceptions.ConflictException:
        print(f"    Gateway '{name}' already exists - reusing it.")
        gateways = agentcore_ctrl.list_gateways().get('items', [])
        existing = next((g for g in gateways if g['name'] == name), None)
        if not existing:
            raise RuntimeError(f"Gateway '{name}' not found after ConflictException")
        gw_id  = existing['gatewayId']
        print(f'    Gateway ID  : {gw_id}')
        gw_url = _gw_wait_for_ready(agentcore_ctrl, gw_id)
        print(f'    Gateway URL : {gw_url}')
        return gw_id, gw_url


def _gw_create_target(agentcore_ctrl, gateway_id: str, t: dict,
                       lambda_arn: str) -> None:
    """Register one Lambda target on the gateway. Skips if it already exists."""
    payload = dict(
        gatewayIdentifier=gateway_id,
        name=t['name'],
        description=t['description'],
        targetConfiguration={
            'mcp': {
                'lambda': {
                    'lambdaArn': lambda_arn,
                    'toolSchema': {
                        'inlinePayload': [{
                            'name':        t['tool_name'],
                            'description': t['tool_description'],
                            'inputSchema': {
                                'type': 'object',
                                'properties': {
                                    t['param_name']: {
                                        'type':        'string',
                                        'description': t['param_desc'],
                                    }
                                },
                                'required': [t['param_name']],
                            },
                        }]
                    },
                }
            }
        },
        credentialProviderConfigurations=[
            {'credentialProviderType': 'GATEWAY_IAM_ROLE'}
        ],
    )
    try:
        resp = agentcore_ctrl.create_gateway_target(**payload)
        print(f"    [{resp['status']:12s}] {t['name']} → target {resp['targetId']}")
    except agentcore_ctrl.exceptions.ConflictException:
        print(f"    [already exists] {t['name']} - skipped")


def deploy_agentcore_gateway() -> dict:
    """
    Create an AgentCore Gateway and register the NovaMart tool Lambda targets.

    Production equivalent of the in-process @tool functions defined inside
    build_*_agent(). Each tool becomes a Lambda function registered as a
    gateway target; agents discover tools at runtime via the MCP endpoint -
    no code changes needed when adding or updating tools.

    Uses the same three-step pattern as Lesson 11:
      1. create_gateway  (MCP protocol, SEMANTIC search)
      2. create_gateway_target  (one per Lambda-backed tool)
      3. Agents connect via the returned gateway_url

    Requires Lambda tool functions to be deployed via a separate stack.
    Set ORDERS_FUNCTION, POLICY_FUNCTION, CUSTOMERS_FUNCTION in .env.

    Returns:
        dict with gateway_id, gateway_url, and status.
    """
    agentcore_ctrl = boto3.client('bedrock-agentcore-control',
                                   region_name=config.AWS_REGION)

    try:
        gw_uuid = _gw_stack_uuid()
    except Exception:
        gw_uuid = config.PROJECT_NAME

    gw_name = f"novamart-support-{gw_uuid}"
    print(f"  Calling create_gateway (name: {gw_name})...")
    gateway_id, gateway_url = _gw_get_or_create(
        agentcore_ctrl, gw_name, config.AGENTCORE_ROLE_ARN,
        "NovaMart customer support gateway. Provides order lookup, "
        "policy search, and customer tier tools.",
    )

    targets = [
        {
            'name':             'orders-api',
            'description':      'Look up order details, status, and return eligibility for a customer',
            'function':         _ORDERS_FUNCTION,
            'tool_name':        'check_order_status',
            'tool_description': 'Check order status and return eligibility for a specific order',
            'param_name':       'order_id',
            'param_desc':       'Order ID (e.g. ORD-27176)',
        },
        {
            'name':             'policy-api',
            'description':      'Retrieve return, shipping, and warranty policy text from knowledge bases',
            'function':         _POLICY_FUNCTION,
            'tool_name':        'search_policies',
            'tool_description': 'Search all policy knowledge bases for a customer query',
            'param_name':       'query',
            'param_desc':       'Customer question about returns, shipping, or warranty',
        },
        {
            'name':             'customers-api',
            'description':      'Look up customer tier (Standard or Premium) and account details',
            'function':         _CUSTOMERS_FUNCTION,
            'tool_name':        'get_customer_tier',
            'tool_description': 'Get customer tier and account information by customer ID',
            'param_name':       'customer_id',
            'param_desc':       'Customer ID (e.g. CUST-001)',
        },
    ]

    print(f"\n  Registering {len(targets)} Gateway targets...")
    for t in targets:
        try:
            lambda_arn = _gw_get_function_arn(t['function'])
            _gw_create_target(agentcore_ctrl, gateway_id, t, lambda_arn)
        except Exception as e:
            print(f"    [Skipped] {t['name']}: {e}")

    return {'gateway_id': gateway_id, 'gateway_url': gateway_url, 'status': 'CREATING'}


# ═══════════════════════════════════════════════════════
#  RUNTIME INVOCATION (pre-written - do not modify)
# ═══════════════════════════════════════════════════════

def invoke_agent(session_id: str, customer_id: str, user_message: str) -> str:
    """
    Invoke the deployed agent via AgentCore Runtime.
    """
    enriched_message = f"[Session ID: {session_id}] [Customer ID: {customer_id}] {user_message}"
    runtime_session_id = session_id
    if len(runtime_session_id) < 33:
        runtime_session_id = f"novamart-{uuid.uuid5(uuid.NAMESPACE_URL, session_id)}"
    if len(runtime_session_id) > 256:
        raise ValueError("session_id must not exceed 256 characters.")

    response = agentcore_client.invoke_agent_runtime(
        agentRuntimeArn=config.AGENTCORE_RUNTIME_ARN,
        runtimeSessionId=runtime_session_id,
        runtimeUserId=customer_id,
        contentType='application/json',
        accept='application/json',
        payload=json.dumps({
            'prompt': enriched_message,
            'session_id': session_id,
            'customer_id': customer_id,
        }).encode('utf-8'),
    )

    body = response.get('response', b'')
    if hasattr(body, 'read'):
        body = body.read()
    if isinstance(body, bytes):
        body = body.decode('utf-8')
    if not isinstance(body, str):
        return str(body)

    try:
        decoded = json.loads(body)
    except json.JSONDecodeError:
        return body
    if isinstance(decoded, dict):
        return str(decoded.get('response', body))
    return str(decoded)


# ═══════════════════════════════════════════════════════
#  DEPLOYMENT ENTRY POINT (pre-written - do not modify)
# ═══════════════════════════════════════════════════════

def deploy_all():
    """Full deployment pipeline. Run after completing all tasks."""
    print("\n" + "="*60)
    print("  Deploying Enterprise Multi-Agent System")
    print("="*60 + "\n")

    print("Step 1/6: Building agent graph...")
    inventory_agent     = build_inventory_agent()
    refund_agent        = build_refund_agent()
    policy_agent        = build_policy_agent()
    communication_agent = build_communication_agent()
    orchestrator = build_orchestrator_agent(
        inventory_agent, refund_agent, policy_agent, communication_agent
    )
    print("  All 5 agents initialized\n")

    print("Step 2/6: Creating Bedrock Guardrail...")
    guardrail_id, guardrail_version = create_guardrail()
    print()

    print("Step 3/6: Deploying to AgentCore Runtime...")
    runtime_arn = deploy_to_agentcore_runtime(orchestrator, guardrail_id, guardrail_version)
    print()

    print("Step 4/6: Configuring Memory...")
    memory_arn = configure_memory(runtime_arn)
    print()

    print("Step 5/6: Configuring Observability...")
    configure_observability(runtime_arn)
    print()

    print("Step 6/6: Deploying AgentCore Gateway...")
    try:
        gw = deploy_agentcore_gateway()
        print(f"  Gateway URL : {gw['gateway_url']}")
        print(f"  Agents connect via MCP at this endpoint - no code changes needed")
    except Exception as e:
        print(f"  [Note] Gateway deployment skipped: {e}")
        print(f"  (Deploy Lambda tool functions and set ORDERS_FUNCTION etc. in .env to enable)")
    print()

    print("="*60)
    print("  Deployment Complete!")
    print("="*60)
    print(f"\n  Add these to your .env file:")
    print(f"  AGENTCORE_RUNTIME_ARN={runtime_arn}")
    print(f"  GUARDRAIL_ID={guardrail_id}")
    print(f"  GUARDRAIL_VERSION={guardrail_version}\n")
    return runtime_arn, guardrail_id


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'deploy':
        deploy_all()

    elif len(sys.argv) > 1 and sys.argv[1] == 'test':
        print("Running local agent test...")
        inventory_agent     = build_inventory_agent()
        refund_agent        = build_refund_agent()
        policy_agent        = build_policy_agent()
        communication_agent = build_communication_agent()

        test_cases = [
            ("CUST-001", "I want to return my wireless headphones from order ORD-27176"),
            ("CUST-002", "What is the return policy for premium customers?"),
            ("CUST-003", "How much would 5 items at $29.99 be with a 10% discount?"),
        ]
        for customer_id, query in test_cases:
            session_id = str(uuid.uuid4())[:8]
            orchestrator = build_orchestrator_agent(
                inventory_agent, refund_agent, policy_agent, communication_agent,
                session_id=session_id,
            )
            print(f"\n{'─'*60}")
            print(f"Session: {session_id} | Customer: {customer_id}")
            print(f"Query: {query}")
            prompt = f"[Session ID: {session_id}] [Customer ID: {customer_id}] {query}"
            with observability.request_trace(), agent_invocation_metrics('OrchestratorAgent') as metrics:
                response = orchestrator(prompt)
                metrics['response'] = response
            print(f"Response: {response}")

    elif len(sys.argv) > 1 and sys.argv[1] == 'chat':
        # ── Interactive terminal chat - educational mode ───────────────────
        W = _C.W

        # ── Welcome banner ────────────────────────────────────────────────
        print()
        print(f"  {_C.GRY}{'=' * W}{_C.RESET}")
        print(f"  {_C.ORCH}{_C.BOLD}{'NovaMart -- Multi-Agent Customer Support':^{W}}{_C.RESET}")
        print(f"  {_C.GRY}{'Strands Agents SDK  +  Amazon Bedrock AgentCore':^{W}}{_C.RESET}")
        print(f"  {_C.GRY}{'=' * W}{_C.RESET}")

        # ── Test customers ────────────────────────────────────────────────
        print()
        print(f"  {_C.GRY}{'─' * W}{_C.RESET}")
        print(f"  {_C.BOLD}Test Customers{_C.RESET}")
        print(f"  {_C.GRY}{'─' * W}{_C.RESET}")
        print(f"  {_C.GRY}{'ID':<10}  {'Name':<18}  {'Tier':<10}  {'Order':<12}  Product{_C.RESET}")
        print(f"  {_C.GRY}{'─'*8}  {'─'*16}  {'─'*8}  {'─'*10}  {'─'*20}{_C.RESET}")
        for cid, name, tier, order, product in [
            ("CUST-001", "Alice Johnson", "Premium",  "ORD-27176", "Sony headphones"),
            ("CUST-002", "Bob Smith",     "Standard", "ORD-28001", "mechanical keyboard"),
            ("CUST-003", "Carol Davis",   "Premium",  "ORD-29001", "laptop"),
            ("CUST-004", "David Lee",     "Standard", "ORD-30001", "phone case"),
        ]:
            tier_col = _C.INV if tier == 'Premium' else _C.GRY
            print(f"  {_C.BOLD}{cid}{_C.RESET}  {name:<18}  "
                  f"{tier_col}{tier:<10}{_C.RESET}  {order}  {product}")
        print(f"  {_C.GRY}{'─' * W}{_C.RESET}")
        print()

        customer_id = (
            input(f"  Enter Customer ID (default: CUST-001): ").strip()
            or "CUST-001"
        )
        session_id  = str(uuid.uuid4())[:8]
        print()
        print(f"  {_C.GRY}Session  : {_C.RESET}{_C.BOLD}{session_id}{_C.RESET}")
        print(f"  {_C.GRY}Customer : {_C.RESET}{_C.BOLD}{customer_id}{_C.RESET}")
        print(f"  {_C.GRY}Type a question and press Enter.  Type 'quit' to exit.{_C.RESET}")
        print()

        # ── Build agents (one line per agent so students see initialisation order)
        print(f"  {_C.GRY}[SYSTEM]  Initializing agent graph...{_C.RESET}")
        inventory_agent     = build_inventory_agent()
        print(f"  {_C.GRY}          {_C.OK}[OK]{_C.RESET}{_C.GRY}  InventoryAgent{_C.RESET}",    flush=True)
        refund_agent        = build_refund_agent()
        print(f"  {_C.GRY}          {_C.OK}[OK]{_C.RESET}{_C.GRY}  RefundAgent{_C.RESET}",       flush=True)
        policy_agent        = build_policy_agent()
        print(f"  {_C.GRY}          {_C.OK}[OK]{_C.RESET}{_C.GRY}  PolicyAgent{_C.RESET}",       flush=True)
        communication_agent = build_communication_agent()
        print(f"  {_C.GRY}          {_C.OK}[OK]{_C.RESET}{_C.GRY}  CommunicationAgent{_C.RESET}", flush=True)
        orchestrator = build_orchestrator_agent(
            inventory_agent, refund_agent, policy_agent, communication_agent,
            session_id=session_id,
        )
        print(f"  {_C.GRY}          {_C.OK}[OK]{_C.RESET}{_C.GRY}  Orchestrator{_C.RESET}",      flush=True)
        print(f"  {_C.GRY}[SYSTEM]  All 5 agents ready.{_C.RESET}")
        print()

        # ── Conversation loop ─────────────────────────────────────────────
        while True:
            try:
                user_input = input(
                    f"  {_C.BOLD}You >{_C.RESET} "
                ).strip()
            except (EOFError, KeyboardInterrupt):
                print(f"\n  {_C.GRY}Session ended.{_C.RESET}")
                break

            if not user_input:
                continue
            if user_input.lower() in ('quit', 'exit', 'q'):
                print(f"  {_C.GRY}Session ended.{_C.RESET}")
                break

            prompt  = (f"[Session ID: {session_id}] "
                       f"[Customer ID: {customer_id}] {user_input}")
            t0_turn = time.time()

            # ── Install proxy, run orchestrator, restore stdout ────────────
            trace.new_turn()
            sys.stdout = _trace_writer
            try:
                with agent_invocation_metrics('OrchestratorAgent') as metrics:
                    response = orchestrator(prompt)
                    metrics['response'] = response
            finally:
                sys.stdout = _real_stdout   # always restore, even on exception

            elapsed = time.time() - t0_turn

            # ── Resolve the final customer-facing text ────────────────────
            final_state = _read_workflow_state(session_id) or {}
            comm_result = final_state.get('communication_agent', '')
            text = _strip_xml_tags(comm_result or str(response))

            # ── DynamoDB workflow state summary ───────────────────────────
            trace.summary(session_id, elapsed)

            # ── Final customer-facing response ────────────────────────────
            print()
            print(f"  {_C.GRY}{'=' * W}{_C.RESET}")
            print(f"  {_C.COM}{_C.BOLD}AGENT RESPONSE{_C.RESET}")
            print(f"  {_C.GRY}{'=' * W}{_C.RESET}")
            for line in text.splitlines():
                print(f"  {line}")
            print(f"  {_C.GRY}{'=' * W}{_C.RESET}")
            print()

    else:
        print("Usage:")
        print("  python agent_orchestrator.py deploy  # Deploy to AgentCore")
        print("  python agent_orchestrator.py test    # Run automated test cases")
        print("  python agent_orchestrator.py chat    # Interactive terminal chat")
