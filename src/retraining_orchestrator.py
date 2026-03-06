"""
Retraining Orchestrator component for managing model retraining workflows
Integrates with Apache Airflow for workflow orchestration
"""
from datetime import datetime, timedelta
from typing import Dict, Optional
import redis.asyncio as redis
from airflow_client.client import ApiClient, Configuration
from airflow_client.client.api import dag_run_api

from src.events import Event, EventType, EventBus
from src.models import RetrainingJob
from src.logging_config import get_logger
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)


class RetrainingOrchestrator:
    """
    Retraining Orchestrator service for triggering and managing model retraining
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        db_session: AsyncSession,
        airflow_base_url: str,
        airflow_username: str,
        airflow_password: str,
        redis_url: str = "redis://localhost:6379",
        deduplication_window_hours: int = 6,
        retraining_dag_id: str = "model_retraining_workflow"
    ):
        self.event_bus = event_bus
        self.db_session = db_session
        self.airflow_base_url = airflow_base_url
        self.airflow_username = airflow_username
        self.airflow_password = airflow_password
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.deduplication_window_hours = deduplication_window_hours
        self.retraining_dag_id = retraining_dag_id
        self.airflow_client: Optional[ApiClient] = None
        
    async def start(self) -> None:
        """Initialize connections"""
        self.redis_client = await redis.from_url(self.redis_url, decode_responses=True)
        
        # Initialize Airflow client
        config = Configuration(
            host=self.airflow_base_url,
            username=self.airflow_username,
            password=self.airflow_password
        )
        self.airflow_client = ApiClient(config)
        
        # Subscribe to drift events
        await self.event_bus.subscribe(EventType.DRIFT_DETECTED, self.handle_drift_event)
        
        logger.info("Retraining Orchestrator started")
    
    async def stop(self) -> None:
        """Cleanup connections"""
        if self.redis_client:
            await self.redis_client.close()
        if self.airflow_client:
            await self.airflow_client.close()
        logger.info("Retraining Orchestrator stopped")
    
    async def handle_drift_event(self, event: Event) -> None:
        """
        Handle drift detection event
        
        Args:
            event: Drift detection event
        """
        logger.info(f"Handling drift event for model {event.model_id}")
        
        model_id = event.model_id
        model_version = event.payload.get('model_version')
        drift_score = event.payload.get('data_drift_score', 0.0)
        
        # Evaluate retraining criteria
        should_retrain = await self._evaluate_retraining_criteria(
            model_id,
            drift_score
        )
        
        if should_retrain:
            # Check deduplication
            if await self._is_duplicate_trigger(model_id):
                logger.info(f"Skipping duplicate retraining trigger for model {model_id}")
                return
            
            # Trigger retraining
            await self.trigger_retraining(
                model_id=model_id,
                trigger_reason=f"Drift detected: {drift_score:.4f}",
                triggered_by="system",
                manual=False
            )
    
    async def trigger_retraining(
        self,
        model_id: str,
        trigger_reason: str,
        triggered_by: str,
        manual: bool = False,
        hyperparameters: Optional[Dict] = None
    ) -> RetrainingJob:
        """
        Trigger model retraining workflow
        
        Args:
            model_id: Model identifier
            trigger_reason: Reason for triggering retraining
            triggered_by: User or system that triggered retraining
            manual: Whether this is a manual trigger
            hyperparameters: Optional hyperparameters for training
            
        Returns:
            RetrainingJob object
        """
        logger.info(f"Triggering retraining for model {model_id}: {trigger_reason}")
        
        # Check deduplication (unless manual)
        if not manual and await self._is_duplicate_trigger(model_id):
            raise ValueError(f"Retraining already triggered for model {model_id} within deduplication window")
        
        # Create retraining job record
        job_id = f"retrain-{model_id}-{datetime.utcnow().timestamp()}"
        training_data_path = f"s3://mlops-training-data/{model_id}/latest/"
        
        retraining_job = RetrainingJob(
            job_id=job_id,
            model_id=model_id,
            trigger_reason=trigger_reason,
            triggered_by=triggered_by,
            triggered_at=datetime.utcnow(),
            airflow_dag_run_id="",  # Will be set after triggering
            status="pending",
            training_data_path=training_data_path,
            hyperparameters=hyperparameters or {}
        )
        
        try:
            # Trigger Airflow DAG
            dag_run_id = await self._trigger_airflow_dag(
                model_id=model_id,
                job_id=job_id,
                training_data_path=training_data_path,
                hyperparameters=hyperparameters or {}
            )
            
            retraining_job.airflow_dag_run_id = dag_run_id
            retraining_job.status = "running"
            
            # Store in database
            self.db_session.add(retraining_job)
            await self.db_session.commit()
            
            # Set deduplication key
            await self._set_deduplication_key(model_id)
            
            # Publish retraining triggered event
            await self._publish_retraining_event(retraining_job)
            
            logger.info(f"Retraining triggered successfully: job_id={job_id}, dag_run_id={dag_run_id}")
            
            return retraining_job
            
        except Exception as e:
            logger.error(f"Error triggering retraining: {e}")
            retraining_job.status = "failed"
            self.db_session.add(retraining_job)
            await self.db_session.commit()
            
            # Publish failure event
            await self._publish_failure_event(model_id, job_id, str(e))
            
            raise
    
    async def _evaluate_retraining_criteria(
        self,
        model_id: str,
        drift_score: float
    ) -> bool:
        """
        Evaluate whether retraining should be triggered
        
        Args:
            model_id: Model identifier
            drift_score: Current drift score
            
        Returns:
            True if retraining should be triggered
        """
        # Simple criteria: drift score above threshold
        # In production, this would be more sophisticated
        drift_threshold = 0.15
        
        if drift_score > drift_threshold:
            logger.info(f"Retraining criteria met for model {model_id}: drift={drift_score:.4f}")
            return True
        
        return False
    
    async def _is_duplicate_trigger(self, model_id: str) -> bool:
        """
        Check if retraining was recently triggered for this model
        
        Args:
            model_id: Model identifier
            
        Returns:
            True if duplicate trigger
        """
        if not self.redis_client:
            return False
        
        key = f"retraining:dedup:{model_id}"
        exists = await self.redis_client.exists(key)
        return bool(exists)
    
    async def _set_deduplication_key(self, model_id: str) -> None:
        """Set deduplication key in Redis"""
        if not self.redis_client:
            return
        
        key = f"retraining:dedup:{model_id}"
        ttl_seconds = self.deduplication_window_hours * 3600
        await self.redis_client.setex(key, ttl_seconds, "1")
    
    async def _trigger_airflow_dag(
        self,
        model_id: str,
        job_id: str,
        training_data_path: str,
        hyperparameters: Dict
    ) -> str:
        """
        Trigger Airflow DAG
        
        Args:
            model_id: Model identifier
            job_id: Retraining job ID
            training_data_path: Path to training data
            hyperparameters: Training hyperparameters
            
        Returns:
            DAG run ID
        """
        if not self.airflow_client:
            raise RuntimeError("Airflow client not initialized")
        
        try:
            api_instance = dag_run_api.DAGRunApi(self.airflow_client)
            
            dag_run_id = f"{self.retraining_dag_id}-{job_id}"
            
            dag_run = {
                "dag_run_id": dag_run_id,
                "conf": {
                    "model_id": model_id,
                    "job_id": job_id,
                    "training_data_path": training_data_path,
                    "hyperparameters": hyperparameters
                }
            }
            
            response = api_instance.post_dag_run(
                self.retraining_dag_id,
                dag_run
            )
            
            return response.dag_run_id
            
        except Exception as e:
            logger.error(f"Error triggering Airflow DAG: {e}")
            # Retry logic for transient failures
            await asyncio.sleep(5)
            raise
    
    async def _publish_retraining_event(self, job: RetrainingJob) -> None:
        """Publish retraining triggered event"""
        event = Event(
            event_id=f"retrain-{job.job_id}",
            event_type=EventType.RETRAINING_TRIGGERED,
            timestamp=datetime.utcnow(),
            model_id=job.model_id,
            payload={
                'job_id': job.job_id,
                'trigger_reason': job.trigger_reason,
                'triggered_by': job.triggered_by,
                'airflow_dag_run_id': job.airflow_dag_run_id,
                'training_data_path': job.training_data_path
            },
            metadata={
                'triggered_at': job.triggered_at.isoformat()
            }
        )
        
        await self.event_bus.publish(event)
    
    async def _publish_failure_event(
        self,
        model_id: str,
        job_id: str,
        error_message: str
    ) -> None:
        """Publish retraining failure event"""
        event = Event(
            event_id=f"retrain-fail-{job_id}",
            event_type=EventType.RETRAINING_FAILED,
            timestamp=datetime.utcnow(),
            model_id=model_id,
            payload={
                'job_id': job_id,
                'error_message': error_message
            },
            metadata={}
        )
        
        await self.event_bus.publish(event)
    
    async def get_job_status(self, job_id: str) -> Optional[RetrainingJob]:
        """
        Get retraining job status
        
        Args:
            job_id: Job identifier
            
        Returns:
            RetrainingJob object or None
        """
        result = await self.db_session.execute(
            self.db_session.query(RetrainingJob).filter(
                RetrainingJob.job_id == job_id
            )
        )
        return result.scalar_one_or_none()
    
    async def list_jobs(
        self,
        model_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> list[RetrainingJob]:
        """
        List retraining jobs
        
        Args:
            model_id: Filter by model ID (optional)
            status: Filter by status (optional)
            limit: Maximum number of jobs to return
            
        Returns:
            List of RetrainingJob objects
        """
        query = self.db_session.query(RetrainingJob)
        
        if model_id:
            query = query.filter(RetrainingJob.model_id == model_id)
        if status:
            query = query.filter(RetrainingJob.status == status)
        
        query = query.order_by(RetrainingJob.triggered_at.desc()).limit(limit)
        
        result = await self.db_session.execute(query)
        return result.scalars().all()
