"""
X-Ray trace emission for the NovaMart multi-agent system.

Records each request as one X-Ray trace rooted at NovaMart-Orchestrator, with
every routed worker agent and every Knowledge Base retrieval as a downstream
node on the service map.
"""

import os
import sys
import time
import secrets
import threading
from contextlib import contextmanager

import boto3
from botocore.exceptions import BotoCoreError, ClientError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

ROOT_SERVICE_NAME = 'NovaMart-Orchestrator'

# X-Ray builds a service-map node for any subsegment carrying this namespace,
# naming the node after the subsegment. Without it the worker agents collapse
# into the orchestrator's own node.
# https://docs.aws.amazon.com/xray/latest/devguide/xray-api-segmentdocuments.html
_REMOTE = 'remote'

_xray = boto3.client('xray', region_name=config.AWS_REGION)

_lock = threading.Lock()
_current = None


class _Request:
    def __init__(self) -> None:
        self.start = time.time()
        self.trace_id = '1-{:08x}-{}'.format(int(self.start), secrets.token_hex(12))
        self.root_id = secrets.token_hex(8)
        self.spans = []


def _new_span(name: str, parent: str) -> dict:
    return {
        'name': name,
        'parent': parent,
        'id': secrets.token_hex(8),
        'start_time': time.time(),
        'end_time': None,
    }


def _open(name: str, parent: str) -> None:
    request = _current
    if request is None:
        return
    with _lock:
        request.spans.append(_new_span(name, parent))


def _close(name: str) -> None:
    request = _current
    if request is None:
        return
    with _lock:
        for span in reversed(request.spans):
            if span['name'] == name and span['end_time'] is None:
                span['end_time'] = time.time()
                return


def agent_start(agent_name: str) -> None:
    """Open a span for a worker agent the orchestrator routed to."""
    _open(agent_name, ROOT_SERVICE_NAME)


def agent_end(agent_name: str) -> None:
    _close(agent_name)


def kb_start(domain: str) -> None:
    """Open a span for a Knowledge Base retrieval performed by the PolicyAgent."""
    _open('KnowledgeBase:{}'.format(domain), 'PolicyAgent')


def kb_end(domain: str) -> None:
    _close('KnowledgeBase:{}'.format(domain))


def start_request() -> str:
    global _current
    _current = _Request()
    return _current.trace_id


def _document(request: '_Request') -> dict:
    end = time.time()
    by_parent = {}
    for span in request.spans:
        by_parent.setdefault(span['parent'], []).append(span)

    def build(span):
        out = {
            'id': span['id'],
            'name': span['name'],
            'start_time': span['start_time'],
            'end_time': span['end_time'] or end,
            'namespace': _REMOTE,
        }
        children = [build(c) for c in by_parent.get(span['name'], [])]
        if children:
            out['subsegments'] = children
        return out

    return {
        'name': ROOT_SERVICE_NAME,
        'id': request.root_id,
        'trace_id': request.trace_id,
        'start_time': request.start,
        'end_time': end,
        'subsegments': [build(s) for s in by_parent.get(ROOT_SERVICE_NAME, [])],
    }


def end_request() -> None:
    global _current
    request, _current = _current, None
    if request is None or not request.spans:
        return
    try:
        import json
        _xray.put_trace_segments(
            TraceSegmentDocuments=[json.dumps(_document(request))]
        )
    except (BotoCoreError, ClientError) as exc:
        print('  [x-ray] segment upload failed: {}'.format(exc))


@contextmanager
def request_trace():
    """Wrap one customer request so it lands in X-Ray as a single trace."""
    trace_id = start_request()
    print('  X-Ray trace id: {}'.format(trace_id))
    try:
        yield trace_id
    finally:
        end_request()
