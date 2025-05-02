import time
from typing import List, Union, Dict

from snowflake import SnowflakeGenerator
from sqlmodel import Session, select, create_engine, or_, delete, update

from smolq.queue.models import *


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

        # when init, started to init all the model into the db
        SQLModel.metadata.create_all(self.engine)

    @staticmethod
    def _get_status_message(message: Union[Message, ActualMessage]) -> MessageStatus:
        now = int(time.time())
        if (message.tries == message.max_tries) & (now > int(message.deliver_at)):
            return MessageStatus.FAILED
        elif (now >= int(message.deliver_at)) & (now < int(message.deliver_at)):
            return MessageStatus.DEQUEUED
        else:
            return MessageStatus.QUEUED

    def get_queue(self, tenant_id: int, queue_name: str) -> Queue:
        with Session(self.engine) as session:
            statement = select(Queue).where(
                Queue.tenant_id == tenant_id, Queue.name == queue_name
            )
            queue = session.exec(statement).first()
            # Check if queue is None before trying to iterate
            if queue is None:
                raise ValueError(
                    f"Queue '{queue_name}' not found for tenant_id {tenant_id}"
                )
            # The original check `l_queue = [q for q in queue]` seems incorrect for a single object.
            # If .first() returns None, the check above handles it. If it returns an object, it exists.
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
            # TODO: can we handle this part?
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
            # Use get_queue to find the queue first
            queue = self.get_queue(tenant_id=tenant_id, queue_name=queue_name)
            # No need for explicit check here, get_queue raises ValueError if not found
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
                message=message.encode("utf-8"),
            )
            session.add(new_message)

            # add to the KV table
            for k, v in kv.items():
                new_kv = KV(
                    tenant_id=new_message.tenant_id,
                    queue_id=new_message.queue_id,
                    message_id=new_message.message_id,
                    K=k,
                    V=v,
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
                    Message.delivered_at <= now,
                    or_(Message.tries < Message.max_tries, Message.tries == -1),
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

    def update_message(
        self,
        tenant_id: int,
        queue_id: str,  # Changed back to queue_name for consistency
        message_id: int,
        updated_message_data: ActualMessage,
    ) -> None:
        """
        Updates an existing message and its associated KV pairs.
        """
        with Session(self.engine) as session:
            # 1. Get the queue using the existing method
            queue = self.get_queue(tenant_id=tenant_id, queue_name=queue_id)
            queue_id = queue.id  # Get queue_id from the retrieved queue object

            # 2. Find the existing message
            message_statement = select(Message).where(
                Message.tenant_id == tenant_id,
                Message.queue_id == queue_id,
                Message.message_id == message_id,
            )
            db_message = session.exec(message_statement).first()

            # 3. Check if message exists
            if not db_message:
                raise ValueError(
                    f"Message with id '{message_id}' not found in queue '{queue_id}' for tenant_id {tenant_id}"
                )

            # 4. Update message attributes
            db_message.deliver_at = updated_message_data.deliver_at
            db_message.delivered_at = updated_message_data.delivered_at
            db_message.tries = updated_message_data.tries
            # Assuming the message in ActualMessage is bytes, decode to store as string
            db_message.message = updated_message_data.message.decode("utf-8")

            # 5b. Add new KV pairs
            if updated_message_data.KV:
                for k, v in updated_message_data.KV.items():
                    kv_update = select(KV).where(
                        KV.tenant_id == tenant_id,
                        KV.queue_id == queue_id,
                        KV.message_id == message_id,
                    )
                    kv = session.exec(kv_update).one()
                    kv.K = k
                    kv.V = v
                    session.add(kv)
            # 6. Add the updated message object back to the session (SQLModel tracks changes)
            session.add(db_message)

            # 7. Commit the transaction
            session.commit()

    def delete_message(self, tenant_id: int, queue_name: str, message_id: int) -> None:
        """
        Deletes a specific message and its associated KV pairs.
        Uses self.get_queue to find the queue.
        """
        with Session(self.engine) as session:
            # 1. Get the queue using the existing method
            queue = self.get_queue(tenant_id=tenant_id, queue_name=queue_name)
            queue_id = queue.id  # Get queue_id from the retrieved queue object

            # 2. Find the existing message
            message_statement = select(Message).where(
                Message.tenant_id == tenant_id,
                Message.queue_id == queue_id,
                Message.message_id == message_id,
            )
            db_message = session.exec(message_statement).first()

            # 4. Delete associated KV pairs first (due to potential foreign key constraints)
            kv_delete_statement = delete(KV).where(
                KV.tenant_id == tenant_id,
                KV.queue_id == queue_id,
                KV.message_id == message_id,
            )
            db_message = session.exec(message_statement).first()

            if not db_message:
                # Optionally, consider if not finding the message should be an error or silent success
                raise ValueError(
                    f"Message with id '{message_id}' not found in queue '{queue_name}' for tenant_id {tenant_id}"
                )
            kv_delete_statement = delete(KV).where(
                KV.tenant_id == tenant_id,
                KV.queue_id == queue_id,
                KV.message_id == message_id,
            )
            session.exec(kv_delete_statement)
            session.delete(db_message)
            session.commit()

    def shutdown(self):
        """
        Turn off the connection, no SIGTERM
        """
        self.engine.dispose()
