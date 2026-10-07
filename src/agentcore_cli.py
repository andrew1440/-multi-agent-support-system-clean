"""Stage and deploy this project with the AWS AgentCore npm CLI."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent
BUILD_DIR = ROOT_DIR / "build" / "runtime"
PROJECT_NAME = "UdacityAgentCore"
AGENT_NAME = "NovaMartSupport"
PROJECT_DIR = BUILD_DIR / PROJECT_NAME
APP_DIR = PROJECT_DIR / "app" / AGENT_NAME
CONFIG_PATH = PROJECT_DIR / "agentcore" / "agentcore.json"
TARGETS_PATH = PROJECT_DIR / "agentcore" / "aws-targets.json"
STATE_PATH = PROJECT_DIR / "agentcore" / ".cli" / "deployed-state.json"

_runtime_environment: dict[str, str] = {}


def _cli_command(arguments: list[str]) -> list[str]:
    executable = shutil.which("agentcore")
    if not executable:
        raise FileNotFoundError(
            "The AgentCore CLI was not found on PATH. Install @aws/agentcore@0.30.0."
        )

    if os.name == "nt" and Path(executable).suffix.lower() in {".cmd", ".bat"}:
        command = subprocess.list2cmdline([executable, *arguments])
        return [os.environ.get("COMSPEC", "cmd.exe"), "/d", "/s", "/c", command]
    return [executable, *arguments]


def _run_cli(
    arguments: list[str],
    *,
    cwd: Path,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    process_environment = os.environ.copy()
    if environment:
        process_environment.update(environment)
    result = subprocess.run(
        _cli_command(arguments),
        cwd=cwd,
        env=process_environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode:
        details = (result.stderr or result.stdout).strip()
        raise RuntimeError(
            f"AgentCore CLI {' '.join(arguments)} failed with exit code "
            f"{result.returncode}:\n{details[-6000:]}"
        )
    return result


def _ensure_project() -> None:
    if CONFIG_PATH.is_file():
        return

    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    _run_cli(
        [
            "create",
            "--name",
            AGENT_NAME,
            "--project-name",
            PROJECT_NAME,
            "--framework",
            "Strands",
            "--model-provider",
            "Bedrock",
            "--memory",
            "none",
            "--protocol",
            "HTTP",
            "--network-mode",
            "PUBLIC",
            "--build",
            "CodeZip",
            "--language",
            "Python",
            "--skip-git",
            "--skip-install",
            "--skip-python-setup",
            "--output-dir",
            str(BUILD_DIR),
            "--json",
        ],
        cwd=ROOT_DIR,
    )


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def _render_runtime_config(environment_variables: dict[str, str]) -> str:
    import config

    names = (
        "AWS_REGION",
        "PROJECT_NAME",
        "ACCOUNT_ID",
        "ORCHESTRATOR_MODEL_ID",
        "WORKER_MODEL_ID",
        "ORDERS_TABLE",
        "CUSTOMERS_TABLE",
        "WORKFLOW_STATE_TABLE",
        "POLICY_BUCKET",
        "VECTOR_STORE_BUCKET",
        "AGENTCORE_ROLE_ARN",
        "AGENT_LOG_GROUP",
        "RETURNS_KB_ID",
        "SHIPPING_KB_ID",
        "WARRANTY_KB_ID",
        "AGENTCORE_RUNTIME_ARN",
        "GUARDRAIL_ID",
        "GUARDRAIL_VERSION",
        "MEMORY_NAMESPACE",
        "GUARDRAIL_NAME",
        "GUARDRAIL_BLOCKED_TOPICS",
    )
    values = {name: getattr(config, name) for name in names}
    values.update(environment_variables)

    lines = ["import os", ""]
    for name, value in values.items():
        if name in environment_variables:
            lines.append(f"{name} = os.environ.get({name!r}, {value!r})")
        else:
            lines.append(f"{name} = {value!r}")
    lines.extend(
        [
            "",
            "def print_config() -> None:",
            "    for name in (",
            "        'AWS_REGION', 'PROJECT_NAME', 'ORDERS_TABLE',",
            "        'CUSTOMERS_TABLE', 'WORKFLOW_STATE_TABLE', 'POLICY_BUCKET',",
            "        'RETURNS_KB_ID', 'SHIPPING_KB_ID', 'WARRANTY_KB_ID',",
            "    ):",
            "        print(f'{name}: {globals()[name]}')",
            "",
        ]
    )
    return "\n".join(lines)


def _runtime_entrypoint() -> str:
    return '''import asyncio
import uuid
from typing import Any

from bedrock_agentcore.runtime import BedrockAgentCoreApp

import agent_observability
from agent_metrics import agent_invocation_metrics
from agent_orchestrator import (
    build_communication_agent,
    build_inventory_agent,
    build_orchestrator_agent,
    build_policy_agent,
    build_refund_agent,
)

app = BedrockAgentCoreApp()


@app.entrypoint
async def invoke(payload: Any, context: Any) -> dict[str, str]:
    session_id = getattr(context, "session_id", None) or str(uuid.uuid4())
    customer_id = "unknown-customer"

    if isinstance(payload, str):
        request = payload
    elif isinstance(payload, dict):
        session_id = payload.get("session_id") or session_id
        customer_id = payload.get("customer_id") or customer_id
        request = (
            payload.get("prompt")
            or payload.get("inputText")
            or payload.get("message")
            or payload.get("user_message")
            or ""
        )
        if not isinstance(request, str):
            raise ValueError("The request prompt must be a string.")
        if "[Session ID:" not in request:
            request = (
                f"[Session ID: {session_id}] [Customer ID: {customer_id}] "
                f"{request}"
            )
    else:
        raise ValueError("The invocation payload must be a string or JSON object.")

    if not request.strip():
        raise ValueError("The invocation payload did not contain a customer request.")

    def run_agent() -> str:
        inventory = build_inventory_agent()
        refund = build_refund_agent()
        policy = build_policy_agent()
        communication = build_communication_agent()
        orchestrator = build_orchestrator_agent(
            inventory, refund, policy, communication, session_id=session_id
        )
        with agent_invocation_metrics("OrchestratorAgent") as metrics:
            response = orchestrator(request)
            metrics["response"] = response
        return str(response)

    with agent_observability.request_trace():
        return {"response": await asyncio.to_thread(run_agent)}


if __name__ == "__main__":
    app.run()
'''


def _stage_application(environment_variables: dict[str, str]) -> None:
    import config

    APP_DIR.mkdir(parents=True, exist_ok=True)
    source_files = (
        (ROOT_DIR / "src" / "agent_orchestrator.py", "agent_orchestrator.py"),
        (ROOT_DIR / "src" / "agent_utils.py", "agent_utils.py"),
        (ROOT_DIR / "src" / "agent_observability.py", "agent_observability.py"),
        (ROOT_DIR / "src" / "agent_metrics.py", "agent_metrics.py"),
        (ROOT_DIR / "src" / "bedrock_kb_retrieval.py", "bedrock_kb_retrieval.py"),
        (ROOT_DIR / "src" / "dynamodb_session_storage.py", "dynamodb_session_storage.py"),
    )
    for source, filename in source_files:
        shutil.copy2(source, APP_DIR / filename)

    (APP_DIR / "config.py").write_text(
        _render_runtime_config(environment_variables), encoding="utf-8"
    )
    (APP_DIR / "main.py").write_text(_runtime_entrypoint(), encoding="utf-8")
    (APP_DIR / "pyproject.toml").write_text(
        '''[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "novamart-support-runtime"
version = "0.1.0"
description = "NovaMart multi-agent customer support runtime"
requires-python = ">=3.12"
dependencies = [
    "aws-opentelemetry-distro ~= 0.18.0",
    "bedrock-agentcore >= 1.9.1",
    "boto3 >= 1.34.0",
    "botocore[crt] >= 1.35.0",
    "mcp ~= 1.24.0",
    "python-dotenv >= 1.0.0",
    "strands-agents >= 1.15.0",
]

[tool.hatch.build.targets.wheel]
packages = ["."]
''',
        encoding="utf-8",
    )

    project = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    runtime = next(
        (item for item in project.get("runtimes", []) if item.get("name") == AGENT_NAME),
        None,
    )
    if runtime is None:
        runtime = {"name": AGENT_NAME}
        project.setdefault("runtimes", []).append(runtime)
    runtime.update(
        {
            "build": "CodeZip",
            "entrypoint": "main.py",
            "codeLocation": f"app/{AGENT_NAME}",
            "runtimeVersion": "PYTHON_3_12",
            "networkMode": "PUBLIC",
            "protocol": "HTTP",
            "executionRoleArn": config.AGENTCORE_ROLE_ARN,
            "authorizerType": authorizer_type,
            "envVars": [
                {"name": name, "value": value}
                for name, value in sorted(environment_variables.items())
            ],
        }
    )
    if authorizer_configuration:
        runtime["authorizerConfiguration"] = authorizer_configuration
    else:
        runtime.pop("authorizerConfiguration", None)
    _write_json(CONFIG_PATH, project)

    targets = json.loads(TARGETS_PATH.read_text(encoding="utf-8")) if TARGETS_PATH.exists() else []
    account_id = config.ACCOUNT_ID
    region = environment_variables.get("AWS_REGION", config.AWS_REGION)
    target = next((item for item in targets if item.get("name") == "default"), None)
    if target is None:
        target = {"name": "default"}
        targets.append(target)
    target.update({"account": account_id, "region": region})
    _write_json(TARGETS_PATH, targets)


def configure_runtime(
    environment_variables: dict[str, str],
    *,
    network_mode: str,
    protocol: str,
    execution_role_arn: str,
    authorizer_type: str = "AWS_IAM",
    authorizer_configuration: dict[str, Any] | None = None,
) -> None:
    """Stage application code and configure a CLI CodeZip runtime project."""
    if network_mode != "PUBLIC":
        raise ValueError("This project currently supports only PUBLIC networking.")
    if protocol != "HTTP":
        raise ValueError("This project runtime must use the HTTP protocol.")

    required_variables = {
        "AWS_REGION",
        "PROJECT_NAME",
        "RETURNS_KB_ID",
        "SHIPPING_KB_ID",
        "WARRANTY_KB_ID",
        "AGENT_LOG_GROUP",
        "GUARDRAIL_ID",
        "GUARDRAIL_VERSION",
    }
    missing = sorted(required_variables - environment_variables.keys())
    if missing:
        raise ValueError(f"Missing runtime environment variables: {', '.join(missing)}")
    if not execution_role_arn:
        raise ValueError("execution_role_arn is required.")

    _ensure_project()
    normalized_environment = {
        name: str(value) for name, value in environment_variables.items()
    }
    _runtime_environment.clear()
    _runtime_environment.update(normalized_environment)
    _stage_application(normalized_environment)


def deploy() -> None:
    """Validate and deploy the staged project through the AgentCore CLI/CDK."""
    if not CONFIG_PATH.is_file():
        raise RuntimeError("Runtime has not been configured; call configure_runtime first.")
    _run_cli(
        ["validate", "--directory", str(PROJECT_DIR), "--json"],
        cwd=ROOT_DIR,
        environment=_runtime_environment,
    )
    _run_cli(
        ["deploy", "--target", "default", "--yes", "--json"],
        cwd=PROJECT_DIR,
        environment=_runtime_environment,
    )


def _find_runtime_arn(value: Any) -> str | None:
    if isinstance(value, dict):
        for item in value.values():
            result = _find_runtime_arn(item)
            if result:
                return result
    elif isinstance(value, list):
        for item in value:
            result = _find_runtime_arn(item)
            if result:
                return result
    elif isinstance(value, str) and value.startswith("arn:") and ":runtime/" in value:
        return value
    return None


def deployed_runtime_arn() -> str:
    """Read the deployed runtime ARN from CLI state or status output."""
    if STATE_PATH.is_file():
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        arn = _find_runtime_arn(state)
        if arn:
            return arn

    result = _run_cli(
        ["status", "--target", "default", "--runtime", AGENT_NAME, "--json"],
        cwd=PROJECT_DIR,
        environment=_runtime_environment,
    )
    try:
        arn = _find_runtime_arn(json.loads(result.stdout))
    except json.JSONDecodeError:
        arn = None
    if not arn:
        raise RuntimeError(
            "AgentCore CLI completed but did not report a runtime ARN in its "
            "deployed state or status output."
        )
    return arn