import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from sqlmodel import Session, SQLModel, create_engine

from smolq.queue.models import *

@pytest.fixture
def setup_db():
    """Setup an in-memory SQLite database for testing"""
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    return engine


class TestPydanticModels:
    """Test cases for the Pydantic models (non-SQL)"""

    def test_queue_properties(self):
        """Test QueueProperties model creation and validation"""
        queue_props = QueueProperties(
            name="test_queue",
            rate_limit=10.5,
            max_retries=3,
            visibility_timeout=60
        )

        assert queue_props.name == "test_queue"
        assert queue_props.rate_limit == 10.5
        assert queue_props.max_retries == 3
        assert queue_props.visibility_timeout == 60

        # Test validation errors
        with pytest.raises(ValueError):
            QueueProperties(
                name="test_queue",
                rate_limit="invalid",  # Should be float
                max_retries=3,
                visibility_timeout=60
            )

    def test_actual_message(self):
        """Test ActualMessage model creation and validation"""
        message_data = {
            "message_id": 123,
            "tenant_id": 456,
            "queue_id": 789,
            "deliver_at": int(datetime.now().timestamp()),
            "delivered_at": int(datetime.now().timestamp()) + 10,
            "tries": 1,
            "max_tries": 5,
            "message": b"test message content",
            "KV": {"key1": "value1", "key2": "value2"}
        }

        message = ActualMessage(**message_data)

        assert message.message_id == 123
        assert message.tenant_id == 456
        assert message.queue_id == 789
        assert message.tries == 1
        assert message.max_tries == 5
        assert message.message == b"test message content"
        assert message.KV == {"key1": "value1", "key2": "value2"}

        # Test validation errors
        invalid_data = message_data.copy()
        invalid_data["KV"] = "not_a_dict"  # Should be Dict[str, str]
        with pytest.raises(ValueError):
            ActualMessage(**invalid_data)


class TestSQLModels:
    """Test cases for the SQLModel ORM classes"""

    def test_queue_model(self, setup_db):
        """Test Queue model creation and CRUD operations"""
        engine = setup_db

        # Create a new queue
        queue = Queue(
            tenant_id=1,
            name="test_queue",
            rate_limit=15.0,
            max_retry=3,
            visibility_timeout=60
        )

        with Session(engine) as session:
            session.add(queue)
            session.commit()
            session.refresh(queue)

            # Check the queue was created with an ID
            assert queue.id is not None

            # Retrieve the queue
            retrieved_queue = session.get(Queue, queue.id)
            assert retrieved_queue is not None
            assert retrieved_queue.tenant_id == 1
            assert retrieved_queue.name == "test_queue"
            assert retrieved_queue.rate_limit == 15.0
            assert retrieved_queue.max_retry == 3
            assert retrieved_queue.visibility_timeout == 60

            # Test camelCase serialization
            queue_dict = retrieved_queue.model_dump(by_alias=True)
            assert "tenantId" in queue_dict
            assert "rateLimit" in queue_dict
            assert "maxRetry" in queue_dict
            assert "visibilityTimeout" in queue_dict

    def test_message_model(self, setup_db):
        """Test Message model creation and CRUD operations"""
        engine = setup_db

        # Create a new message
        message = Message(
            message_id=1,
            tenant_id=1,
            queue_id=1,
            deliver_at=int(datetime.now().timestamp()),
            delivered_at=int(datetime.now().timestamp()) + 30,
            tries=0,
            max_tries=3,
            message="Hello, world!"
        )

        with Session(engine) as session:
            session.add(message)
            session.commit()

            # Retrieve the message
            retrieved_message = session.get(Message, message.message_id)
            assert retrieved_message is not None
            assert retrieved_message.message == "Hello, world!"
            assert retrieved_message.tries == 0
            assert retrieved_message.max_tries == 3

            # Test camelCase serialization
            message_dict = retrieved_message.model_dump(by_alias=True)
            assert "messageId" in message_dict
            assert "tenantId" in message_dict
            assert "queueId" in message_dict
            assert "deliverAt" in message_dict
            assert "deliveredAt" in message_dict

    def test_kv_model(self, setup_db):
        """Test KV model creation and CRUD operations"""
        engine = setup_db

        # Create a message first (as KV references message)
        message = Message(
            message_id=1,
            tenant_id=1,
            queue_id=1,
            deliver_at=int(datetime.now().timestamp()),
            delivered_at=int(datetime.now().timestamp()) + 30,
            tries=0,
            max_tries=3,
            message="Hello, world!"
        )

        # Create KV pairs for the message
        kv1 = KV(
            tenant_id=1,
            queue_id=1,
            message_id=1,
            K="priority",
            V="high"
        )

        kv2 = KV(
            tenant_id=1,
            queue_id=1,
            message_id=1,
            K="source",
            V="test"
        )

        with Session(engine) as session:
            session.add(message)
            session.commit()

            session.add(kv1)
            session.add(kv2)
            session.commit()
            session.refresh(kv1)
            session.refresh(kv2)

            # Check KVs were created successfully
            assert kv1.id is not None
            assert kv2.id is not None
            assert kv1.K == "priority"
            assert kv1.V == "high"
            assert kv2.K == "source"
            assert kv2.V == "test"

            # Test camelCase serialization
            kv_dict = kv1.model_dump(by_alias=True)
            assert "tenantId" in kv_dict
            assert "queueId" in kv_dict
            assert "messageId" in kv_dict

    def test_rate_limit_model(self, setup_db):
        """Test RateLimit model creation and CRUD operations"""
        engine = setup_db

        rate_limit = RateLimit(
            tenant_id=1,
            queue_id=1,
            ts=int(datetime.now().timestamp()),
            n=5
        )

        with Session(engine) as session:
            session.add(rate_limit)
            session.commit()

            # Retrieve the rate limit
            retrieved_rate_limit = session.get(RateLimit, rate_limit.tenant_id)
            assert retrieved_rate_limit is not None
            assert retrieved_rate_limit.queue_id == 1
            assert retrieved_rate_limit.n == 5

            # Update the rate limit
            retrieved_rate_limit.n = 10
            session.commit()

            # Check the update worked
            updated_rate_limit = session.get(RateLimit, rate_limit.tenant_id)
            assert updated_rate_limit.n == 10

            # Test camelCase serialization
            rate_limit_dict = updated_rate_limit.model_dump(by_alias=True)
            assert "tenantId" in rate_limit_dict
            assert "queueId" in rate_limit_dict


class TestMessageConversion:
    """Test cases for message conversion methods"""

    @patch('models.Session')
    def test_message_to_model(self, mock_session):
        """Test Message.to_model conversion method"""
        # Setup the message
        message = Message(
            message_id=1,
            tenant_id=1,
            queue_id=1,
            deliver_at=100,
            delivered_at=200,
            tries=1,
            max_tries=3,
            message="Test message"
        )

        # Mock the session and query results
        mock_session_instance = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_session_instance

        # Mock KV results
        mock_kv1 = MagicMock(K="key1", V="value1")
        mock_kv2 = MagicMock(K="key2", V="value2")
        mock_session_instance.exec.return_value.all.return_value = [mock_kv1, mock_kv2]

        # Call the method
        mock_conn = MagicMock()
        actual_message = message.to_model(mock_conn)

        # Verify the result
        assert isinstance(actual_message, ActualMessage)
        assert actual_message.message_id == 1
        assert actual_message.tenant_id == 1
        assert actual_message.queue_id == 1
        assert actual_message.deliver_at == 100
        assert actual_message.delivered_at == 200
        assert actual_message.tries == 1
        assert actual_message.max_tries == 3
        assert actual_message.message == b"Test message"
        assert actual_message.KV == {"key1": "value1", "key2": "value2"}

        # Verify the query was constructed correctly
        mock_session_instance.exec.assert_called_once()

    def test_message_to_model_integration(self, setup_db):
        """Test Message.to_model with actual database integration"""
        engine = setup_db

        # Create a message and KVs
        message = Message(
            message_id=2,
            tenant_id=2,
            queue_id=2,
            deliver_at=300,
            delivered_at=400,
            tries=2,
            max_tries=4,
            message="Integration test message"
        )

        kv1 = KV(
            tenant_id=2,
            queue_id=2,
            message_id=2,
            K="test_key1",
            V="test_value1"
        )

        kv2 = KV(
            tenant_id=2,
            queue_id=2,
            message_id=2,
            K="test_key2",
            V="test_value2"
        )

        with Session(engine) as session:
            session.add(message)
            session.commit()

            session.add(kv1)
            session.add(kv2)
            session.commit()

            # Now test the to_model method with a real session
            # We need to patch the Message class temporarily to use our session object
            with patch.object(Message, 'message_id', 2), \
                    patch.object(Message, 'tenant_id', 2), \
                    patch.object(Message, 'queue_id', 2), \
                    patch.object(Message, 'deliver_at', 300), \
                    patch.object(Message, 'delivered_at', 400), \
                    patch.object(Message, 'tries', 2), \
                    patch.object(Message, 'max_tries', 4), \
                    patch.object(Message, 'message', "Integration test message"):
                actual_message = Message.to_model(engine)

                # Verify the result
                assert isinstance(actual_message, ActualMessage)
                assert actual_message.message_id == 2
                assert actual_message.tenant_id == 2
                assert actual_message.queue_id == 2
                assert actual_message.deliver_at == 300
                assert actual_message.delivered_at == 400
                assert actual_message.tries == 2
                assert actual_message.max_tries == 4
                assert actual_message.message == b"Integration test message"
                assert actual_message.KV == {"test_key1": "test_value1", "test_key2": "test_value2"}