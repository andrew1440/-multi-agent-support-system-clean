"""DynamoDB-backed Strands session repository."""

from __future__ import annotations

import os
import time
from decimal import Decimal
from enum import Enum
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from strands.session import SessionRepository
from strands.types.session import Session, SessionAgent, SessionMessage, SessionType


class DynamoDbSessionStorage(SessionRepository):
    """Persist Strands session, agent, and message records in one DynamoDB table."""

    def __init__(
        self,
        table_name: str | None = None,
        *,
        region_name: str | None = None,
        ttl_days: int = 90,
    ) -> None:
        self.table_name = table_name or os.environ.get('AGENT_SESSIONS_TABLE', 'agent-sessions')
        self.region_name = region_name or os.environ.get('AWS_REGION', 'us-east-1')
        self.ttl_days = ttl_days
        self.table = boto3.resource('dynamodb', region_name=self.region_name).Table(self.table_name)

    @staticmethod
    def _dynamo_safe(value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, dict):
            return {key: DynamoDbSessionStorage._dynamo_safe(item) for key, item in value.items()}
        if isinstance(value, list):
            return [DynamoDbSessionStorage._dynamo_safe(item) for item in value]
        if isinstance(value, tuple):
            return [DynamoDbSessionStorage._dynamo_safe(item) for item in value]
        return value

    @staticmethod
    def _python_safe(value: Any) -> Any:
        if isinstance(value, Decimal):
            return int(value) if value == value.to_integral_value() else float(value)
        if isinstance(value, dict):
            return {key: DynamoDbSessionStorage._python_safe(item) for key, item in value.items()}
        if isinstance(value, list):
            return [DynamoDbSessionStorage._python_safe(item) for item in value]
        return value

    def _item(self, session_id: str, record_key: str, record_type: str, payload: dict) -> dict:
        return {
            'session_id': session_id,
            'record_key': record_key,
            'record_type': record_type,
            'payload': self._dynamo_safe(payload),
            'ttl': int(time.time()) + self.ttl_days * 24 * 60 * 60,
        }

    def _write(self, session_id: str, record_key: str, record_type: str, payload: dict) -> None:
        self.table.put_item(Item=self._item(session_id, record_key, record_type, payload))

    def _read(self, session_id: str, record_key: str) -> dict | None:
        result = self.table.get_item(Key={'session_id': session_id, 'record_key': record_key})
        item = result.get('Item')
        return self._python_safe(item['payload']) if item else None

    def create_session(self, session: Session, **kwargs: Any) -> Session:
        try:
            self.table.put_item(
                Item=self._item(session.session_id, 'SESSION', 'SESSION', session.to_dict()),
                ConditionExpression='attribute_not_exists(session_id)',
            )
        except ClientError as error:
            if error.response.get('Error', {}).get('Code') != 'ConditionalCheckFailedException':
                raise
        return session

    def read_session(self, session_id: str, **kwargs: Any) -> Session | None:
        payload = self._read(session_id, 'SESSION')
        if payload is None:
            return None
        payload['session_type'] = SessionType(payload['session_type'])
        return Session.from_dict(payload)

    def create_agent(self, session_id: str, session_agent: SessionAgent, **kwargs: Any) -> None:
        self._write(session_id, f"AGENT#{session_agent.agent_id}", 'AGENT', session_agent.to_dict())

    def read_agent(self, session_id: str, agent_id: str, **kwargs: Any) -> SessionAgent | None:
        payload = self._read(session_id, f'AGENT#{agent_id}')
        return SessionAgent.from_dict(payload) if payload else None

    def update_agent(self, session_id: str, session_agent: SessionAgent, **kwargs: Any) -> None:
        self.create_agent(session_id, session_agent)

    def create_message(
        self,
        session_id: str,
        agent_id: str,
        session_message: SessionMessage,
        **kwargs: Any,
    ) -> None:
        record_key = f"MESSAGE#{agent_id}#{session_message.message_id:020d}"
        self._write(session_id, record_key, 'MESSAGE', session_message.to_dict())

    def read_message(
        self,
        session_id: str,
        agent_id: str,
        message_id: int,
        **kwargs: Any,
    ) -> SessionMessage | None:
        record_key = f'MESSAGE#{agent_id}#{message_id:020d}'
        payload = self._read(session_id, record_key)
        return SessionMessage.from_dict(payload) if payload else None

    def update_message(
        self,
        session_id: str,
        agent_id: str,
        session_message: SessionMessage,
        **kwargs: Any,
    ) -> None:
        self.create_message(session_id, agent_id, session_message)

    def list_messages(
        self,
        session_id: str,
        agent_id: str,
        limit: int | None = None,
        offset: int = 0,
        **kwargs: Any,
    ) -> list[SessionMessage]:
        prefix = f'MESSAGE#{agent_id}#'
        query = {
            'KeyConditionExpression': Key('session_id').eq(session_id) & Key('record_key').begins_with(prefix),
            'ScanIndexForward': True,
        }
        items: list[dict] = []
        while True:
            page = self.table.query(**query)
            items.extend(page.get('Items', []))
            if 'LastEvaluatedKey' not in page:
                break
            query['ExclusiveStartKey'] = page['LastEvaluatedKey']

        selected = items[offset:offset + limit if limit is not None else None]
        return [
            SessionMessage.from_dict(self._python_safe(item['payload']))
            for item in selected
        ]