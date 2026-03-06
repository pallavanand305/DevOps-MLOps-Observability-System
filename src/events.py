"""
Event bus implementation for inter-component communication
Supports both AWS SNS/SQS and Redis Pub/Sub
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from typing import Dict, List, Callable, Optional, Any
import json
import uuid
import asyncio
from concurrent.futures import ThreadPoolExecutor

import boto3
import redis.asyncio as redis
from src.logging_config import get_logger

logger = get_logger(__name__)


class EventType(Enum):
    """Event types for the control plane"""
    DRIFT_DETECTED = "drift.detected"
    DATA_QUALITY_ALERT = "data.quality.alert"
    RETRAINING_TRIGGERED = "retraining.triggered"
    RETRAINING_FAILED = "retraining.failed"
    DEPLOYMENT_STARTED = "deployment.started"
    ROLLBACK_INITIATED = "deployment.rollback"
    HEALTH_ALERT = "health.alert"
    BACKUP_FAILED = "backup.failed"


@dataclass
class Event:
    """Base event class"""
    event_id: str
    event_type: EventType
    timestamp: datetime
    model_id: str
    payload: Dict[str, Any]
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict:
        """Convert event to dictionary"""
        return {
            'event_id': self.event_id,
            'event_type': self.event_type.value,
            'timestamp': self.timestamp.isoformat(),
            'model_id': self.model_id,
            'payload': self.payload,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Event':
        """Create event from dictionary"""
        return cls(
            event_id=data['event_id'],
            event_type=EventType(data['event_type']),
            timestamp=datetime.fromisoformat(data['timestamp']),
            model_id=data['model_id'],
            payload=data['payload'],
            metadata=data['metadata']
        )
    
    def to_json(self) -> str:
        """Serialize event to JSON"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Event':
        """Deserialize event from JSON"""
        return cls.from_dict(json.loads(json_str))


class EventBus(ABC):
    """Abstract event bus interface"""
    
    @abstractmethod
    async def publish(self, event: Event) -> None:
        """Publish an event"""
        pass
    
    @abstractmethod
    async def subscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Subscribe to an event type"""
        pass
    
    @abstractmethod
    async def unsubscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Unsubscribe from an event type"""
        pass
    
    @abstractmethod
    async def start(self) -> None:
        """Start the event bus"""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Stop the event bus"""
        pass


class RedisEventBus(EventBus):
    """Redis Pub/Sub implementation of event bus"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        self.subscribers: Dict[EventType, List[Callable]] = {}
        self.running = False
        self._listener_task: Optional[asyncio.Task] = None
        
    async def start(self) -> None:
        """Start Redis connection and listener"""
        self.redis_client = await redis.from_url(self.redis_url, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        self.running = True
        self._listener_task = asyncio.create_task(self._listen())
        logger.info("Redis event bus started")
    
    async def stop(self) -> None:
        """Stop Redis connection"""
        self.running = False
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        
        if self.pubsub:
            await self.pubsub.close()
        if self.redis_client:
            await self.redis_client.close()
        logger.info("Redis event bus stopped")
    
    async def publish(self, event: Event) -> None:
        """Publish event to Redis channel"""
        if not self.redis_client:
            raise RuntimeError("Event bus not started")
        
        channel = f"events:{event.event_type.value}"
        await self.redis_client.publish(channel, event.to_json())
        logger.info(f"Published event {event.event_id} to {channel}")
    
    async def subscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Subscribe to event type"""
        if event_type not in self.subscribers:
            self.subscribers[event_type] = []
            channel = f"events:{event_type.value}"
            await self.pubsub.subscribe(channel)
            logger.info(f"Subscribed to channel {channel}")
        
        self.subscribers[event_type].append(handler)
    
    async def unsubscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Unsubscribe from event type"""
        if event_type in self.subscribers:
            self.subscribers[event_type].remove(handler)
            if not self.subscribers[event_type]:
                channel = f"events:{event_type.value}"
                await self.pubsub.unsubscribe(channel)
                del self.subscribers[event_type]
                logger.info(f"Unsubscribed from channel {channel}")
    
    async def _listen(self) -> None:
        """Listen for messages on subscribed channels"""
        while self.running:
            try:
                message = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message['type'] == 'message':
                    await self._handle_message(message)
            except Exception as e:
                logger.error(f"Error in event listener: {e}")
                await asyncio.sleep(1)
    
    async def _handle_message(self, message: Dict) -> None:
        """Handle incoming message"""
        try:
            event = Event.from_json(message['data'])
            handlers = self.subscribers.get(event.event_type, [])
            
            for handler in handlers:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(event)
                    else:
                        await asyncio.get_event_loop().run_in_executor(None, handler, event)
                except Exception as e:
                    logger.error(f"Error in event handler: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")


class SNSSQSEventBus(EventBus):
    """AWS SNS/SQS implementation of event bus"""
    
    def __init__(self, region: str = "us-east-1", topic_prefix: str = "mlops-events"):
        self.region = region
        self.topic_prefix = topic_prefix
        self.sns_client = boto3.client('sns', region_name=region)
        self.sqs_client = boto3.client('sqs', region_name=region)
        self.topics: Dict[EventType, str] = {}
        self.queues: Dict[EventType, str] = {}
        self.subscribers: Dict[EventType, List[Callable]] = {}
        self.running = False
        self._listener_tasks: List[asyncio.Task] = []
        
    async def start(self) -> None:
        """Start SNS/SQS event bus"""
        self.running = True
        logger.info("SNS/SQS event bus started")
    
    async def stop(self) -> None:
        """Stop SNS/SQS event bus"""
        self.running = False
        for task in self._listener_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("SNS/SQS event bus stopped")
    
    async def publish(self, event: Event) -> None:
        """Publish event to SNS topic"""
        topic_arn = await self._get_or_create_topic(event.event_type)
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            await loop.run_in_executor(
                executor,
                lambda: self.sns_client.publish(
                    TopicArn=topic_arn,
                    Message=event.to_json(),
                    MessageAttributes={
                        'event_type': {'DataType': 'String', 'StringValue': event.event_type.value},
                        'model_id': {'DataType': 'String', 'StringValue': event.model_id}
                    }
                )
            )
        logger.info(f"Published event {event.event_id} to SNS topic {topic_arn}")
    
    async def subscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Subscribe to event type"""
        if event_type not in self.subscribers:
            self.subscribers[event_type] = []
            topic_arn = await self._get_or_create_topic(event_type)
            queue_url = await self._get_or_create_queue(event_type)
            await self._subscribe_queue_to_topic(queue_url, topic_arn)
            
            # Start listener task for this queue
            task = asyncio.create_task(self._listen_queue(event_type, queue_url))
            self._listener_tasks.append(task)
        
        self.subscribers[event_type].append(handler)
    
    async def unsubscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Unsubscribe from event type"""
        if event_type in self.subscribers:
            self.subscribers[event_type].remove(handler)
    
    async def _get_or_create_topic(self, event_type: EventType) -> str:
        """Get or create SNS topic"""
        if event_type in self.topics:
            return self.topics[event_type]
        
        topic_name = f"{self.topic_prefix}-{event_type.value.replace('.', '-')}"
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            response = await loop.run_in_executor(
                executor,
                lambda: self.sns_client.create_topic(Name=topic_name)
            )
        
        topic_arn = response['TopicArn']
        self.topics[event_type] = topic_arn
        return topic_arn
    
    async def _get_or_create_queue(self, event_type: EventType) -> str:
        """Get or create SQS queue"""
        if event_type in self.queues:
            return self.queues[event_type]
        
        queue_name = f"{self.topic_prefix}-{event_type.value.replace('.', '-')}-queue"
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            response = await loop.run_in_executor(
                executor,
                lambda: self.sqs_client.create_queue(QueueName=queue_name)
            )
        
        queue_url = response['QueueUrl']
        self.queues[event_type] = queue_url
        return queue_url
    
    async def _subscribe_queue_to_topic(self, queue_url: str, topic_arn: str) -> None:
        """Subscribe SQS queue to SNS topic"""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            # Get queue ARN
            queue_attrs = await loop.run_in_executor(
                executor,
                lambda: self.sqs_client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=['QueueArn']
                )
            )
            queue_arn = queue_attrs['Attributes']['QueueArn']
            
            # Subscribe queue to topic
            await loop.run_in_executor(
                executor,
                lambda: self.sns_client.subscribe(
                    TopicArn=topic_arn,
                    Protocol='sqs',
                    Endpoint=queue_arn
                )
            )
    
    async def _listen_queue(self, event_type: EventType, queue_url: str) -> None:
        """Listen for messages on SQS queue"""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            while self.running:
                try:
                    messages = await loop.run_in_executor(
                        executor,
                        lambda: self.sqs_client.receive_message(
                            QueueUrl=queue_url,
                            MaxNumberOfMessages=10,
                            WaitTimeSeconds=5
                        )
                    )
                    
                    if 'Messages' in messages:
                        for message in messages['Messages']:
                            await self._handle_sqs_message(event_type, message, queue_url)
                except Exception as e:
                    logger.error(f"Error listening to queue: {e}")
                    await asyncio.sleep(1)
    
    async def _handle_sqs_message(self, event_type: EventType, message: Dict, queue_url: str) -> None:
        """Handle SQS message"""
        try:
            # Parse SNS message
            sns_message = json.loads(message['Body'])
            event = Event.from_json(sns_message['Message'])
            
            handlers = self.subscribers.get(event_type, [])
            for handler in handlers:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(event)
                    else:
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, handler, event)
                except Exception as e:
                    logger.error(f"Error in event handler: {e}")
            
            # Delete message from queue
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                await loop.run_in_executor(
                    executor,
                    lambda: self.sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                )
        except Exception as e:
            logger.error(f"Error handling SQS message: {e}")


def create_event_bus(bus_type: str = "redis", **kwargs) -> EventBus:
    """Factory function to create event bus"""
    if bus_type == "redis":
        return RedisEventBus(**kwargs)
    elif bus_type == "sns_sqs":
        return SNSSQSEventBus(**kwargs)
    else:
        raise ValueError(f"Unknown event bus type: {bus_type}")
