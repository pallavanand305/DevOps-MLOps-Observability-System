"""
Model Monitor component for drift detection
Uses EvidentlyAI for drift calculation and Prometheus for metrics export
"""
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import pandas as pd
from prometheus_client import Gauge, Counter
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, DataQualityPreset
from evidently.metrics import DatasetDriftMetric, ColumnDriftMetric

from src.events import Event, EventType, EventBus
from src.models import DriftMetrics
from src.logging_config import get_logger
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)


# Prometheus metrics
drift_score_gauge = Gauge(
    'model_drift_score',
    'Model drift score',
    ['model_id', 'model_version', 'drift_type']
)

drift_detection_counter = Counter(
    'model_drift_detections_total',
    'Total number of drift detections',
    ['model_id', 'model_version']
)


class ModelMonitor:
    """
    Model Monitor service for detecting model drift
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        db_session: AsyncSession,
        drift_threshold: float = 0.15,
        check_interval_seconds: int = 3600
    ):
        self.event_bus = event_bus
        self.db_session = db_session
        self.drift_threshold = drift_threshold
        self.check_interval_seconds = check_interval_seconds
        self.reference_data: Dict[str, pd.DataFrame] = {}
        self.custom_metrics: Dict[str, callable] = {}
        
    async def analyze_predictions(
        self,
        model_id: str,
        model_version: str,
        current_data: pd.DataFrame,
        reference_data: Optional[pd.DataFrame] = None
    ) -> DriftMetrics:
        """
        Analyze predictions for drift
        
        Args:
            model_id: Model identifier
            model_version: Model version
            current_data: Current prediction data
            reference_data: Reference data for comparison (optional)
            
        Returns:
            DriftMetrics object with drift analysis results
        """
        logger.info(f"Analyzing drift for model {model_id} version {model_version}")
        
        # Use stored reference data if not provided
        if reference_data is None:
            reference_data = self.reference_data.get(model_id)
            if reference_data is None:
                raise ValueError(f"No reference data available for model {model_id}")
        
        # Calculate drift using EvidentlyAI
        drift_report = self._calculate_drift(reference_data, current_data)
        
        # Extract drift scores
        prediction_drift_score = drift_report.get('prediction_drift', 0.0)
        data_drift_score = drift_report.get('dataset_drift', 0.0)
        feature_drift_scores = drift_report.get('feature_drifts', {})
        
        # Calculate custom metrics if configured
        for metric_name, metric_func in self.custom_metrics.items():
            try:
                custom_score = metric_func(reference_data, current_data)
                feature_drift_scores[metric_name] = custom_score
            except Exception as e:
                logger.error(f"Error calculating custom metric {metric_name}: {e}")
        
        # Determine if drift detected
        drift_detected = (
            prediction_drift_score > self.drift_threshold or
            data_drift_score > self.drift_threshold
        )
        
        # Create drift metrics record
        now = datetime.utcnow()
        drift_metrics = DriftMetrics(
            model_id=model_id,
            model_version=model_version,
            timestamp=now,
            prediction_drift_score=prediction_drift_score,
            data_drift_score=data_drift_score,
            feature_drift_scores=feature_drift_scores,
            reference_period_start=now - timedelta(days=7),
            reference_period_end=now - timedelta(days=1),
            current_period_start=now - timedelta(hours=1),
            current_period_end=now,
            drift_detected=drift_detected,
            threshold=self.drift_threshold
        )
        
        # Store in database
        self.db_session.add(drift_metrics)
        await self.db_session.commit()
        
        # Export metrics to Prometheus
        self._export_metrics(model_id, model_version, drift_metrics)
        
        # Publish drift event if detected
        if drift_detected:
            await self._publish_drift_event(model_id, model_version, drift_metrics)
        
        logger.info(
            f"Drift analysis complete for {model_id}: "
            f"prediction_drift={prediction_drift_score:.4f}, "
            f"data_drift={data_drift_score:.4f}, "
            f"detected={drift_detected}"
        )
        
        return drift_metrics
    
    def _calculate_drift(
        self,
        reference_data: pd.DataFrame,
        current_data: pd.DataFrame
    ) -> Dict:
        """
        Calculate drift using EvidentlyAI
        
        Args:
            reference_data: Reference dataset
            current_data: Current dataset
            
        Returns:
            Dictionary with drift scores
        """
        try:
            # Create Evidently report
            report = Report(metrics=[
                DatasetDriftMetric(),
                DataDriftPreset(),
            ])
            
            report.run(
                reference_data=reference_data,
                current_data=current_data
            )
            
            # Extract results
            results = report.as_dict()
            
            # Parse drift scores
            dataset_drift = results['metrics'][0]['result'].get('dataset_drift', False)
            drift_share = results['metrics'][0]['result'].get('drift_share', 0.0)
            
            # Get feature-level drift
            feature_drifts = {}
            if 'metrics' in results and len(results['metrics']) > 1:
                for metric in results['metrics'][1:]:
                    if 'result' in metric and 'column_name' in metric['result']:
                        col_name = metric['result']['column_name']
                        drift_score = metric['result'].get('drift_score', 0.0)
                        feature_drifts[col_name] = drift_score
            
            return {
                'dataset_drift': drift_share,
                'prediction_drift': drift_share,  # Use same for now
                'feature_drifts': feature_drifts
            }
            
        except Exception as e:
            logger.error(f"Error calculating drift with EvidentlyAI: {e}")
            return {
                'dataset_drift': 0.0,
                'prediction_drift': 0.0,
                'feature_drifts': {}
            }
    
    def _export_metrics(
        self,
        model_id: str,
        model_version: str,
        drift_metrics: DriftMetrics
    ) -> None:
        """Export drift metrics to Prometheus"""
        drift_score_gauge.labels(
            model_id=model_id,
            model_version=model_version,
            drift_type='prediction'
        ).set(drift_metrics.prediction_drift_score)
        
        drift_score_gauge.labels(
            model_id=model_id,
            model_version=model_version,
            drift_type='data'
        ).set(drift_metrics.data_drift_score)
        
        if drift_metrics.drift_detected:
            drift_detection_counter.labels(
                model_id=model_id,
                model_version=model_version
            ).inc()
    
    async def _publish_drift_event(
        self,
        model_id: str,
        model_version: str,
        drift_metrics: DriftMetrics
    ) -> None:
        """Publish drift detection event"""
        event = Event(
            event_id=f"drift-{model_id}-{datetime.utcnow().timestamp()}",
            event_type=EventType.DRIFT_DETECTED,
            timestamp=datetime.utcnow(),
            model_id=model_id,
            payload={
                'model_version': model_version,
                'prediction_drift_score': drift_metrics.prediction_drift_score,
                'data_drift_score': drift_metrics.data_drift_score,
                'threshold': drift_metrics.threshold,
                'feature_drift_scores': drift_metrics.feature_drift_scores
            },
            metadata={
                'metric_id': drift_metrics.metric_id,
                'timestamp': drift_metrics.timestamp.isoformat()
            }
        )
        
        await self.event_bus.publish(event)
        logger.info(f"Published drift event for model {model_id}")
    
    def set_reference_data(self, model_id: str, reference_data: pd.DataFrame) -> None:
        """Set reference data for a model"""
        self.reference_data[model_id] = reference_data
        logger.info(f"Set reference data for model {model_id} with {len(reference_data)} rows")
    
    def add_custom_metric(self, name: str, metric_func: callable) -> None:
        """
        Add custom drift metric
        
        Args:
            name: Metric name
            metric_func: Function that takes (reference_data, current_data) and returns float
        """
        self.custom_metrics[name] = metric_func
        logger.info(f"Added custom drift metric: {name}")
    
    async def get_drift_history(
        self,
        model_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[DriftMetrics]:
        """
        Get drift history for a model
        
        Args:
            model_id: Model identifier
            start_time: Start time for history (optional)
            end_time: End time for history (optional)
            limit: Maximum number of records to return
            
        Returns:
            List of DriftMetrics records
        """
        query = self.db_session.query(DriftMetrics).filter(
            DriftMetrics.model_id == model_id
        )
        
        if start_time:
            query = query.filter(DriftMetrics.timestamp >= start_time)
        if end_time:
            query = query.filter(DriftMetrics.timestamp <= end_time)
        
        query = query.order_by(DriftMetrics.timestamp.desc()).limit(limit)
        
        result = await self.db_session.execute(query)
        return result.scalars().all()


# Custom drift metrics implementations
def calculate_psi(reference_data: pd.DataFrame, current_data: pd.DataFrame, column: str = 'prediction') -> float:
    """
    Calculate Population Stability Index (PSI)
    
    Args:
        reference_data: Reference dataset
        current_data: Current dataset
        column: Column to calculate PSI for
        
    Returns:
        PSI score
    """
    try:
        import numpy as np
        
        # Create bins
        bins = np.histogram_bin_edges(reference_data[column], bins=10)
        
        # Calculate distributions
        ref_dist, _ = np.histogram(reference_data[column], bins=bins)
        curr_dist, _ = np.histogram(current_data[column], bins=bins)
        
        # Normalize
        ref_dist = ref_dist / len(reference_data) + 1e-10
        curr_dist = curr_dist / len(current_data) + 1e-10
        
        # Calculate PSI
        psi = np.sum((curr_dist - ref_dist) * np.log(curr_dist / ref_dist))
        
        return float(psi)
    except Exception as e:
        logger.error(f"Error calculating PSI: {e}")
        return 0.0


def calculate_kl_divergence(reference_data: pd.DataFrame, current_data: pd.DataFrame, column: str = 'prediction') -> float:
    """
    Calculate Kullback-Leibler divergence
    
    Args:
        reference_data: Reference dataset
        current_data: Current dataset
        column: Column to calculate KL divergence for
        
    Returns:
        KL divergence score
    """
    try:
        import numpy as np
        from scipy.stats import entropy
        
        # Create bins
        bins = np.histogram_bin_edges(reference_data[column], bins=10)
        
        # Calculate distributions
        ref_dist, _ = np.histogram(reference_data[column], bins=bins)
        curr_dist, _ = np.histogram(current_data[column], bins=bins)
        
        # Normalize
        ref_dist = ref_dist / len(reference_data) + 1e-10
        curr_dist = curr_dist / len(current_data) + 1e-10
        
        # Calculate KL divergence
        kl_div = entropy(curr_dist, ref_dist)
        
        return float(kl_div)
    except Exception as e:
        logger.error(f"Error calculating KL divergence: {e}")
        return 0.0
