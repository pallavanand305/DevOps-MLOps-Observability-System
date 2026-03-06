"""
Data Validator component for data quality validation
Uses Great Expectations for validation suite execution
"""
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
from cachetools import TTLCache
import asyncio
from concurrent.futures import ThreadPoolExecutor

import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.data_context import DataContext

from src.events import Event, EventType, EventBus
from src.models import ValidationResult
from src.logging_config import get_logger
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)


class DataValidator:
    """
    Data Validator service for validating data quality
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        db_session: AsyncSession,
        ge_context: Optional[DataContext] = None,
        quality_threshold: float = 0.95,
        validation_timeout_ms: int = 500,
        cache_ttl_seconds: int = 300
    ):
        self.event_bus = event_bus
        self.db_session = db_session
        self.ge_context = ge_context or gx.get_context()
        self.quality_threshold = quality_threshold
        self.validation_timeout_ms = validation_timeout_ms
        self.validation_cache = TTLCache(maxsize=1000, ttl=cache_ttl_seconds)
        self.executor = ThreadPoolExecutor(max_workers=4)
        
    async def validate(
        self,
        model_id: str,
        data: pd.DataFrame,
        suite_name: str,
        suite_version: str = "1.0"
    ) -> ValidationResult:
        """
        Validate data against Great Expectations suite
        
        Args:
            model_id: Model identifier
            data: Data to validate
            suite_name: Name of validation suite
            suite_version: Version of validation suite
            
        Returns:
            ValidationResult object
        """
        logger.info(f"Validating data for model {model_id} with suite {suite_name}")
        
        start_time = datetime.utcnow()
        
        # Check cache
        cache_key = f"{model_id}:{suite_name}:{hash(str(data.values.tobytes()))}"
        if cache_key in self.validation_cache:
            logger.info(f"Returning cached validation result for {cache_key}")
            return self.validation_cache[cache_key]
        
        try:
            # Run validation with timeout
            validation_results = await asyncio.wait_for(
                self._run_validation(data, suite_name),
                timeout=self.validation_timeout_ms / 1000.0
            )
            
            execution_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            # Parse results
            success = validation_results['success']
            results_dict = validation_results['results']
            statistics = validation_results.get('statistics', {})
            failed_expectations = [
                exp for exp in results_dict
                if not exp.get('success', True)
            ]
            
            # Create validation result record
            validation_result = ValidationResult(
                validation_id=f"val-{model_id}-{datetime.utcnow().timestamp()}",
                model_id=model_id,
                timestamp=datetime.utcnow(),
                suite_name=suite_name,
                suite_version=suite_version,
                success=success,
                results=results_dict,
                statistics=statistics,
                failed_expectations=failed_expectations,
                execution_time_ms=execution_time_ms
            )
            
            # Store in database
            self.db_session.add(validation_result)
            await self.db_session.commit()
            
            # Cache result
            self.validation_cache[cache_key] = validation_result
            
            # Check quality threshold
            quality_score = self._calculate_quality_score(validation_results)
            if quality_score < self.quality_threshold:
                await self._publish_quality_alert(model_id, validation_result, quality_score)
            
            logger.info(
                f"Validation complete for {model_id}: "
                f"success={success}, quality_score={quality_score:.4f}, "
                f"execution_time={execution_time_ms:.2f}ms"
            )
            
            return validation_result
            
        except asyncio.TimeoutError:
            logger.error(f"Validation timeout for model {model_id}")
            raise ValueError(f"Validation exceeded timeout of {self.validation_timeout_ms}ms")
        except Exception as e:
            logger.error(f"Validation error for model {model_id}: {e}")
            raise
    
    async def _run_validation(
        self,
        data: pd.DataFrame,
        suite_name: str
    ) -> Dict:
        """
        Run Great Expectations validation
        
        Args:
            data: Data to validate
            suite_name: Name of validation suite
            
        Returns:
            Validation results dictionary
        """
        loop = asyncio.get_event_loop()
        
        def _validate():
            try:
                # Get expectation suite
                suite = self.ge_context.get_expectation_suite(suite_name)
                
                # Create batch
                batch = self.ge_context.sources.pandas_default.read_dataframe(data)
                
                # Run validation
                results = batch.validate(suite)
                
                return {
                    'success': results.success,
                    'results': [r.to_json_dict() for r in results.results],
                    'statistics': results.statistics
                }
            except Exception as e:
                logger.error(f"Error in GE validation: {e}")
                return {
                    'success': False,
                    'results': [],
                    'statistics': {},
                    'error': str(e)
                }
        
        return await loop.run_in_executor(self.executor, _validate)
    
    def _calculate_quality_score(self, validation_results: Dict) -> float:
        """
        Calculate overall quality score from validation results
        
        Args:
            validation_results: Validation results dictionary
            
        Returns:
            Quality score between 0 and 1
        """
        results = validation_results.get('results', [])
        if not results:
            return 0.0
        
        successful = sum(1 for r in results if r.get('success', False))
        return successful / len(results)
    
    async def _publish_quality_alert(
        self,
        model_id: str,
        validation_result: ValidationResult,
        quality_score: float
    ) -> None:
        """Publish data quality alert event"""
        event = Event(
            event_id=f"quality-{model_id}-{datetime.utcnow().timestamp()}",
            event_type=EventType.DATA_QUALITY_ALERT,
            timestamp=datetime.utcnow(),
            model_id=model_id,
            payload={
                'validation_id': validation_result.validation_id,
                'suite_name': validation_result.suite_name,
                'quality_score': quality_score,
                'threshold': self.quality_threshold,
                'failed_expectations_count': len(validation_result.failed_expectations),
                'failed_expectations': validation_result.failed_expectations[:5]  # First 5
            },
            metadata={
                'timestamp': validation_result.timestamp.isoformat(),
                'execution_time_ms': validation_result.execution_time_ms
            }
        )
        
        await self.event_bus.publish(event)
        logger.info(f"Published quality alert for model {model_id}")
    
    async def validate_multiple_suites(
        self,
        model_id: str,
        data: pd.DataFrame,
        suite_names: List[str]
    ) -> List[ValidationResult]:
        """
        Validate data against multiple suites in parallel
        
        Args:
            model_id: Model identifier
            data: Data to validate
            suite_names: List of suite names
            
        Returns:
            List of ValidationResult objects
        """
        logger.info(f"Validating data for model {model_id} with {len(suite_names)} suites")
        
        tasks = [
            self.validate(model_id, data, suite_name)
            for suite_name in suite_names
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        valid_results = [r for r in results if isinstance(r, ValidationResult)]
        
        logger.info(f"Completed {len(valid_results)}/{len(suite_names)} validations")
        
        return valid_results
    
    async def list_suites(self) -> List[Dict]:
        """
        List available validation suites
        
        Returns:
            List of suite information dictionaries
        """
        try:
            suites = self.ge_context.list_expectation_suite_names()
            return [
                {
                    'name': suite_name,
                    'expectations_count': len(
                        self.ge_context.get_expectation_suite(suite_name).expectations
                    )
                }
                for suite_name in suites
            ]
        except Exception as e:
            logger.error(f"Error listing suites: {e}")
            return []
    
    async def get_validation_history(
        self,
        model_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[ValidationResult]:
        """
        Get validation history for a model
        
        Args:
            model_id: Model identifier
            start_time: Start time for history (optional)
            end_time: End time for history (optional)
            limit: Maximum number of records to return
            
        Returns:
            List of ValidationResult records
        """
        query = self.db_session.query(ValidationResult).filter(
            ValidationResult.model_id == model_id
        )
        
        if start_time:
            query = query.filter(ValidationResult.timestamp >= start_time)
        if end_time:
            query = query.filter(ValidationResult.timestamp <= end_time)
        
        query = query.order_by(ValidationResult.timestamp.desc()).limit(limit)
        
        result = await self.db_session.execute(query)
        return result.scalars().all()
    
    def create_expectation_suite(
        self,
        suite_name: str,
        expectations: List[Dict]
    ) -> ExpectationSuite:
        """
        Create a new expectation suite
        
        Args:
            suite_name: Name for the suite
            expectations: List of expectation configurations
            
        Returns:
            Created ExpectationSuite
        """
        suite = self.ge_context.add_expectation_suite(suite_name)
        
        for exp_config in expectations:
            suite.add_expectation(**exp_config)
        
        self.ge_context.save_expectation_suite(suite)
        logger.info(f"Created expectation suite: {suite_name}")
        
        return suite
