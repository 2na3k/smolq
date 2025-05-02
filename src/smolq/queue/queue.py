from snowflake import SnowflakeGenerator
from sqlmodel import Session, select, create_engine
from smolq.queue.models import *
from typing import List
import time


class QueueConfig:
    PATH: str = "sqlite:///smolq.db"
    CONFIG_STRING: str = "?_journal_mode=WAL&_foreign_keys=off&_auto_vacuum=full"

    @classmethod
    def get_connection_string(cls) -> str:
        return cls.PATH + cls.CONFIG_STRING


class QueueManager:
    def __init__(self, config: QueueConfig):
        self.config = config
        self.engine = create_engine(QueueConfig.get_connection_string())

        # when init, started to deal with everything
        SQLModel.metadata.create_all(self.engine)

    def get_queue(self, tenant_id: int, queue_name: str) -> Queue:
        with Session(self.engine) as session:
            statement = select(Queue).where(
                Queue.tenant_id == tenant_id, Queue.name == queue_name
            )
            queue = session.exec(statement).first()
            l_queue = [q for q in queue]
            if len(l_queue) == 0:
                raise ValueError(
                    f"Queue '{queue_name}' not found for tenant_id {tenant_id}"
                )
            return queue

    def create_queue(self, tenant_id: int, properties: QueueProperties) -> None:
        with Session(self.engine) as session:
            queue = Queue(
                tenant_id=tenant_id,
                name=properties.name,
                rate_limit=properties.rate_limit,
                max_retry=properties.max_retries,
                visibility_timeout=properties.visibility_timeout,
            )
            session.add(queue)
            session.commit()

    def update_queue(
        self, tenant_id: int, queue_name: str, properties: QueueProperties
    ) -> None:
        with Session(self.engine) as session:
            statement = select(Queue).where(
                Queue.tenant_id == tenant_id, Queue.name == queue_name
            )
            queue = session.exec(statement).first()
            if not queue:
                raise ValueError(
                    f"Queue '{queue_name}' not found for tenant_id {tenant_id}"
                )
            queue.rate_limit = properties.rate_limit
            queue.max_retry = properties.max_retries
            queue.visibility_timeout = properties.visibility_timeout
            session.add(queue)
            session.commit()

    def delete_queue(self, tenant_id: int, queue_name: str) -> None:
        with Session(self.engine) as session:
            statement = select(Queue).where(
                Queue.tenant_id == tenant_id, Queue.name == queue_name
            )
            queue = session.exec(statement).first()
            if not queue:
                raise ValueError(
                    f"Queue '{queue_name}' not found for tenant_id {tenant_id}"
                )
            session.delete(queue)
            session.commit()

    def list_queue(self, tenant_id: int) -> List[str]:
        """
        Return the list of the queue, with the name of the queue
        """
        with Session(self.engine) as session:
            statement = select(Queue.name).where(Queue.tenant_id == tenant_id)
            return [row for row in session.exec(statement).all()]

    def enqueue(
        self,
        tenant_id: int,
        queue_name: str,
        message: str,
        kv: Dict[str, str],
        delay: int,
    ) -> int:
        """
        WAIT HOW TO ROLL BACK??????????
        """
        with Session(self.engine) as session:
            # generate the message id with snowflake
            message_id = int(next(SnowflakeGenerator(0)))
            queue = self.get_queue(tenant_id, queue_name)
            deliver_at = int(time.time()) + delay
            new_message = Message(
                message_id=message_id,
                tenant_id=tenant_id,
                queue_id=queue.id,
                deliver_at=deliver_at,
                tries=0,
                max_tries=queue.max_retry,
                message=message.encode("utf-8")
            )
            session.add(new_message)

            # add to the KV table
            for k, v in kv.items():
                new_kv = KV(
                    tenant_id=new_message.tenant_id,
                    queue_id=new_message.queue_id,
                    message_id=new_message.message_id,
                    K=k,
                    V=v
                )
                session.add(new_kv)
            session.commit()
            return new_message.message_id

    def dequeue(
        self, tenant_id: int, queue_name: str, num_to_dequeue: int, requeue_in: int
    ) -> List[Message]:
        with Session(self.engine) as session:
            queue = self.get_queue(tenant_id, queue_name)
            now = int(time.time())
            if requeue_in >= -1:
                delivered_at = now + requeue_in
            else:
                delivered_at = now
            statement = (
                select(Message)
                .where(
                    Message.tenant_id == tenant_id,
                    Message.queue_id == queue.id,
                    Message.deliver_at <= now,
                    Message.delivered_at is None,
                )
                .order_by(Message.deliver_at)
                .limit(num_to_dequeue)
            )
            messages = session.exec(statement).all()
            for message in messages:
                message.deliver_at = delivered_at
                message.deliver_at = now
                message.tries += 1
                session.add(message)
            session.commit()
            return messages

    def shutdown(self):
        """
        Turn off the connection, no SIGTERM
        """
        self.engine.dispose()
