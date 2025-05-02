import enum

from pydantic import ConfigDict, BaseModel
from sqlmodel import Field, SQLModel, Session, select
from pydantic.alias_generators import to_camel
from typing import Dict, Optional
from enum import Enum


class MessageStatus(str, Enum):
    QUEUED = "QUEUED"
    DEQUEUED = "DEQUEUED"
    FAILED = "FAILED"


# Several class using BaseModel
class QueueProperties(BaseModel):
    name: str
    rate_limit: float
    max_retries: int
    visibility_timeout: int


class ActualMessage(BaseModel):
    """
    Will be used in the external modules to parse the message.
    """

    message_id: int
    tenant_id: int
    queue_id: int
    deliver_at: int
    delivered_at: int
    tries: int
    max_tries: int
    message: bytes
    KV: Dict[str, str]


# ORM with SQLModel BaseSchema
model_config = ConfigDict(
    alias_generator=to_camel,
    populate_by_name=True,
    from_attributes=True,
)


class BaseSchema(SQLModel):
    model_config = model_config


class Queue(BaseSchema, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    tenant_id: int = Field(index=True)
    name: str
    rate_limit: float
    max_retry: int
    visibility_timeout: int


class KV(BaseSchema, table=True):
    id: int = Field(
        default=None, primary_key=True
    )  # Just put this here to let the ORM auto generate sth
    tenant_id: int = Field(index=True, nullable=False, foreign_key="message.tenant_id")
    queue_id: int = Field(index=True, nullable=False, foreign_key="message.queue_id")
    message_id: int = Field(
        index=True, nullable=False, foreign_key="message.message_id"
    )
    K: str
    V: str


class Message(BaseSchema, table=True):
    """
    Expected to be used when retrieved messages directly from the SQLite3 database
    """

    message_id: int = Field(primary_key=True, nullable=False, index=True)
    tenant_id: int = Field(nullable=False, index=True)
    queue_id: int = Field(nullable=False, index=True)
    deliver_at: int
    delivered_at: int = Field(nullable=False, default=0)
    tries: int
    max_tries: int
    message: str

    @classmethod
    def to_model(cls, conn) -> ActualMessage:
        """
        Trying to do several things here:
        - The message would be put into bytes
        - Get all the KV pair from the KV table

        I hate the connection injection like that but yeah, it's ORM
        """

        with Session(conn) as session:
            statement = select(KV).where(
                KV.message_id == cls.message_id,
                KV.queue_id == cls.queue_id,
                KV.tenant_id == cls.tenant_id,
            )
            list_kv = session.exec(statement).all()
            return ActualMessage(
                message_id=cls.message_id,
                tenant_id=cls.tenant_id,
                queue_id=cls.queue_id,
                deliver_at=cls.deliver_at,
                delivered_at=cls.delivered_at,
                tries=cls.tries,
                max_tries=cls.max_tries,
                message=bytes(cls.message, encoding="utf-8"),
                KV={i.K: i.V for i in list_kv},
            )


class RateLimit(BaseSchema, table=True):
    tenant_id: int = Field(primary_key=True, nullable=False, unique=True)
    queue_id: int = Field(index=True, nullable=False)
    ts: int = Field(index=True, nullable=False)
    n: int = Field(nullable=False, default=0)
