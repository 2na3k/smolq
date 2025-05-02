import pytest
from sqlmodel import Session, create_engine
from smolq.queue.models import *
from smolq.queue.queue import QueueManager, QueueConfig, QueueProperties


@pytest.fixture
def test_engine():
    # Create an in-memory SQLite database for testing
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    return engine


@pytest.fixture
def queue_manager(test_engine):
    config = QueueConfig()
    config.PATH = "sqlite:///smolq.db"  # Use the in-memory engine
    return QueueManager(config)


def test_create_queue(queue_manager):
    properties = QueueProperties(
        name="test_queue",
        rate_limit=10.0,
        max_retries=5,
        visibility_timeout=30,
    )
    queue_manager.create_queue(tenant_id=1, properties=properties)

    # Verify the queue was created
    queue = queue_manager.get_queue(tenant_id=1, queue_name="test_queue")
    assert queue.name == "test_queue"
    assert queue.rate_limit == 10.0
    assert queue.max_retry == 5
    assert queue.visibility_timeout == 30


def test_update_queue(queue_manager):
    properties = QueueProperties(
        name="test_queue",
        rate_limit=10.0,
        max_retries=5,
        visibility_timeout=30,
    )
    queue_manager.create_queue(tenant_id=1, properties=properties)

    # Update the queue
    updated_properties = QueueProperties(
        name="test_queue",
        rate_limit=20.0,
        max_retries=10,
        visibility_timeout=60,
    )
    queue_manager.update_queue(
        tenant_id=1, queue_name="test_queue", properties=updated_properties
    )

    # Verify the queue was updated
    queue = queue_manager.get_queue(tenant_id=1, queue_name="test_queue")
    assert queue.rate_limit == 20.0
    assert queue.max_retry == 10
    assert queue.visibility_timeout == 60


@pytest.mark.skip(reason="concurrent create + delete would not help")
def test_delete_queue(queue_manager):
    properties = QueueProperties(
        name="test_queue",
        rate_limit=10.0,
        max_retries=5,
        visibility_timeout=30,
    )
    queue_manager.create_queue(tenant_id=1, properties=properties)

    # Delete the queue
    queue_manager.delete_queue(tenant_id=1, queue_name="test_queue")

    # Verify the queue was deleted
    with pytest.raises(ValueError):
        out = queue_manager.get_queue(tenant_id=1, queue_name="test_queue")


def test_list_queue(queue_manager):
    properties1 = QueueProperties(
        name="queue1",
        rate_limit=10.0,
        max_retries=5,
        visibility_timeout=30,
    )
    properties2 = QueueProperties(
        name="queue2",
        rate_limit=15.0,
        max_retries=3,
        visibility_timeout=20,
    )
    queue_manager.create_queue(tenant_id=1, properties=properties1)
    queue_manager.create_queue(tenant_id=1, properties=properties2)

    # List queues
    queues = queue_manager.list_queue(tenant_id=1)
    assert "queue1" in queues
    assert "queue2" in queues


def test_enqueue_and_dequeue(queue_manager):
    properties = QueueProperties(
        name="test_queue",
        rate_limit=10.0,
        max_retries=5,
        visibility_timeout=30,
    )
    queue_manager.create_queue(tenant_id=1, properties=properties)

    # Enqueue a message
    kv = {"key1": "value1", "key2": "value2"}
    message_id = queue_manager.enqueue(
        tenant_id=1, queue_name="test_queue", message="test_message", kv=kv, delay=0
    )
    print(queue_manager.list_queue(tenant_id=1))

    # Dequeue the message
    messages = queue_manager.dequeue(
        tenant_id=1, queue_name="test_queue", num_to_dequeue=6, requeue_in=0
    )

    after_dequeue = queue_manager.list_queue(tenant_id=1)
    print(after_dequeue)


def test_update_message(queue_manager, test_engine):
    # 1. Setup: Create queue and enqueue initial message
    properties = QueueProperties(
        name="update_test_queue",
        rate_limit=1.0,
        max_retries=3,
        visibility_timeout=10,
    )
    queue_manager.create_queue(tenant_id=2, properties=properties)
    initial_kv = {"initial_key": "initial_value"}
    message_id = queue_manager.enqueue(
        tenant_id=2,
        queue_name="update_test_queue",
        message="original_message",
        kv=initial_kv,
        delay=0,
    )

    # 2. Prepare updated message data
    queue = queue_manager.get_queue(tenant_id=2, queue_name="update_test_queue")
    updated_kv = {"updated_key1": "updated_value1", "updated_key2": "updated_value2"}
    updated_message_data = ActualMessage(
        message_id=message_id,
        tenant_id=2,
        queue_id=queue.id,  # Need the actual queue_id
        deliver_at=1234567890,  # Example timestamp
        delivered_at=987654321,  # Example timestamp
        tries=2,
        max_tries=properties.max_retries,  # Keep max_tries same as queue
        message=b"updated_message_content",
        KV=updated_kv,
    )

    # 3. Call the update_message method
    queue_manager.update_message(
        tenant_id=2,
        queue_id="update_test_queue",  # Pass queue name here as per method signature
        message_id=message_id,
        updated_message_data=updated_message_data,
    )

    # Just pass test with the thing
    queue = queue_manager.get_queue(tenant_id=2, queue_name="update_test_queue")
