"""SQLAlchemy database models"""
from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional
from sqlalchemy import (
    Column, String, Integer, Float, Boolean, DateTime, JSON, Text,
    ForeignKey, Index, Enum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class ModelStatus(PyEnum):
    HEALTHY = "healthy"
    DRIFTED = "drifted"
    TRAINING = "training"
    DEPLOYING = "deploying"
    FAILED = "failed"


class DeploymentStrategy(PyEnum):
    CANARY = "canary"
    BLUE_GREEN = "blue_green"
    ROLLING = "rolling"


class Model(Base):
    __tablename__ = "models"
    
    model_id = Column(String(255), primary_key=True)
    name = Column(String(255), nullable=False)
    version = Column(String(50), nullable=False)
    status = Column(Enum(ModelStatus), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    framework = Column(String(50), nullable=False)
    artifact_uri = Column(Text, nullable=False)
    config = Column(JSON)
    metadata = Column(JSON)
    
    __table_args__ = (
        Index('idx_models_status', 'status'),
        Index('idx_models_created_at', 'created_at'),
    )


class DriftMetrics(Base):
    __tablename__ = "drift_metrics"
    
    metric_id = Column(Integer, primary_key=True, autoincrement=True)
    model_id = Column(String(255), ForeignKey('models.model_id'), nullable=False)
    model_version = Column(String(50), nullable=False)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    prediction_drift_score = Column(Float, nullable=False)
    data_drift_score = Column(Float, nullable=False)
    feature_drift_scores = Column(JSON)
    reference_period_start = Column(DateTime, nullable=False)
    reference_period_end = Column(DateTime, nullable=False)
    current_period_start = Column(DateTime, nullable=False)
    current_period_end = Column(DateTime, nullable=False)
    drift_detected = Column(Boolean, nullable=False)
    threshold = Column(Float, nullable=False)
    
    __table_args__ = (
        Index('idx_drift_metrics_model_timestamp', 'model_id', 'timestamp'),
        Index('idx_drift_metrics_drift_detected', 'drift_detected', 'timestamp'),
    )


class ValidationResult(Base):
    __tablename__ = "validation_results"
    
    validation_id = Column(String(255), primary_key=True)
    model_id = Column(String(255), ForeignKey('models.model_id'), nullable=False)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    suite_name = Column(String(255), nullable=False)
    suite_version = Column(String(50), nullable=False)
    success = Column(Boolean, nullable=False)
    results = Column(JSON)
    statistics = Column(JSON)
    failed_expectations = Column(JSON)
    execution_time_ms = Column(Float, nullable=False)
    
    __table_args__ = (
        Index('idx_validation_results_model_timestamp', 'model_id', 'timestamp'),
        Index('idx_validation_results_success', 'success', 'timestamp'),
    )


class RetrainingJob(Base):
    __tablename__ = "retraining_jobs"
    
    job_id = Column(String(255), primary_key=True)
    model_id = Column(String(255), ForeignKey('models.model_id'), nullable=False)
    trigger_reason = Column(Text, nullable=False)
    triggered_by = Column(String(255), nullable=False)
    triggered_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    airflow_dag_run_id = Column(String(255), nullable=False)
    status = Column(String(50), nullable=False)
    training_data_path = Column(Text, nullable=False)
    hyperparameters = Column(JSON)
    completed_at = Column(DateTime)
    new_model_version = Column(String(50))
    performance_metrics = Column(JSON)
    
    __table_args__ = (
        Index('idx_retraining_jobs_model_id', 'model_id', 'triggered_at'),
        Index('idx_retraining_jobs_status', 'status'),
    )


class Deployment(Base):
    __tablename__ = "deployments"
    
    deployment_id = Column(String(255), primary_key=True)
    model_id = Column(String(255), ForeignKey('models.model_id'), nullable=False)
    model_version = Column(String(50), nullable=False)
    strategy = Column(Enum(DeploymentStrategy), nullable=False)
    environment = Column(String(50), nullable=False)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime)
    status = Column(String(50), nullable=False)
    canary_config = Column(JSON)
    traffic_splits = Column(JSON, nullable=False)
    health_metrics = Column(JSON)
    rollback_version = Column(String(50))
    
    __table_args__ = (
        Index('idx_deployments_model_id', 'model_id', 'started_at'),
        Index('idx_deployments_status', 'status'),
    )


class FeatureLineage(Base):
    __tablename__ = "feature_lineage"
    
    feature_name = Column(String(255), primary_key=True)
    feature_version = Column(String(50), primary_key=True)
    source_dataset = Column(String(255), nullable=False)
    source_dataset_version = Column(String(50), nullable=False)
    transformation_logic = Column(Text)
    transformation_code_commit = Column(String(255))
    parent_features = Column(JSON)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    schema = Column(JSON)
    statistics = Column(JSON)
    
    __table_args__ = (
        Index('idx_feature_lineage_source', 'source_dataset', 'source_dataset_version'),
    )


class ModelLineage(Base):
    __tablename__ = "model_lineage"
    
    model_id = Column(String(255), ForeignKey('models.model_id'), primary_key=True)
    model_version = Column(String(50), primary_key=True)
    training_dataset_version = Column(String(255), nullable=False)
    code_commit_hash = Column(String(255), nullable=False)
    hyperparameters = Column(JSON)
    training_started_at = Column(DateTime, nullable=False)
    training_completed_at = Column(DateTime, nullable=False)
    parent_model_version = Column(String(50))
    features_used = Column(JSON, nullable=False)
    performance_metrics = Column(JSON, nullable=False)
    artifact_s3_uri = Column(Text, nullable=False)
    deployment_history = Column(JSON)
    
    __table_args__ = (
        Index('idx_model_lineage_parent', 'model_id', 'parent_model_version'),
        Index('idx_model_lineage_commit', 'code_commit_hash'),
    )


class Alert(Base):
    __tablename__ = "alerts"
    
    alert_id = Column(String(255), primary_key=True)
    alert_type = Column(String(100), nullable=False)
    severity = Column(String(50), nullable=False)
    model_id = Column(String(255), ForeignKey('models.model_id'))
    model_version = Column(String(50))
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    title = Column(Text, nullable=False)
    message = Column(Text, nullable=False)
    details = Column(JSON)
    destinations = Column(JSON, nullable=False)
    delivery_status = Column(JSON)
    
    __table_args__ = (
        Index('idx_alerts_timestamp', 'timestamp'),
        Index('idx_alerts_severity', 'severity', 'timestamp'),
        Index('idx_alerts_model_id', 'model_id', 'timestamp'),
    )


class AuditLog(Base):
    __tablename__ = "audit_logs"
    
    log_id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    action = Column(String(255), nullable=False)
    user_id = Column(String(255))
    model_id = Column(String(255))
    model_version = Column(String(50))
    resource_type = Column(String(100))
    resource_id = Column(String(255))
    old_value = Column(JSON)
    new_value = Column(JSON)
    metadata = Column(JSON)
    
    __table_args__ = (
        Index('idx_audit_logs_timestamp', 'timestamp'),
        Index('idx_audit_logs_action', 'action'),
        Index('idx_audit_logs_user', 'user_id', 'timestamp'),
    )
