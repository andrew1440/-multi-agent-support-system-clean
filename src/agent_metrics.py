"""CloudWatch metrics for NovaMart agent invocations."""

from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Iterator

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)
_NAMESPACE = os.environ.get('AGENT_METRICS_NAMESPACE', 'NovaMart/AgentSupport')
_cloudwatch = boto3.client('cloudwatch', region_name=os.environ.get('AWS_REGION', 'us-east-1'))


def _guardrail_intervened(response: Any) -> bool:
    stop_reason = getattr(response, 'stop_reason', None)
    if isinstance(stop_reason, str) and 'guardrail' in stop_reason.lower():
        return True

    message = getattr(response, 'message', response)
    try:
        text = json.dumps(message, default=str).lower()
    except (TypeError, ValueError):
        text = str(message).lower()
    return any(
        marker in text
        for marker in (
            'guardrail_intervened',
            'guardrail intervened',
            "sorry, i can't help with that request",
            "sorry, i can't share that response",
        )
    )


def _publish(agent_name: str, elapsed_ms: float, failed: bool, response: Any) -> None:
    dimensions = [{'Name': 'AgentName', 'Value': agent_name}]
    metrics = [
        {
            'MetricName': 'AgentInvocationCount',
            'Dimensions': dimensions,
            'Value': 1,
            'Unit': 'Count',
        },
        {
            'MetricName': 'AgentLatencyMilliseconds',
            'Dimensions': dimensions,
            'Value': max(elapsed_ms, 0.0),
            'Unit': 'Milliseconds',
        },
    ]
    if failed:
        metrics.append({
            'MetricName': 'AgentInvocationFailureCount',
            'Dimensions': dimensions,
            'Value': 1,
            'Unit': 'Count',
        })
    if _guardrail_intervened(response):
        metrics.append({
            'MetricName': 'GuardrailInterventionCount',
            'Dimensions': dimensions,
            'Value': 1,
            'Unit': 'Count',
        })

    try:
        _cloudwatch.put_metric_data(Namespace=_NAMESPACE, MetricData=metrics)
    except (BotoCoreError, ClientError) as error:
        logger.warning('Unable to publish CloudWatch metrics for %s: %s', agent_name, error)


@contextmanager
def agent_invocation_metrics(agent_name: str) -> Iterator[dict[str, Any]]:
    """Record invocation count, latency, failures, and guardrail interventions."""
    started = time.perf_counter()
    state: dict[str, Any] = {}
    failed = False
    try:
        yield state
    except Exception:
        failed = True
        raise
    finally:
        _publish(
            agent_name,
            (time.perf_counter() - started) * 1000,
            failed,
            state.get('response'),
        )
