"""Configuration management module"""
import os
from typing import Dict, List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings
import yaml


class SystemConfig(BaseSettings):
    """System configuration"""
    environment: str = Field(default="development")
    log_level: str = Field(default="INFO")
    api_port: int = Field(default=8000)
    metrics_port: int = Field(default=9090)


class ModelConfig(BaseSettings):
    """Model monitoring configuration"""
    default_drift_threshold: float = Field(default=0.15)
    drift_check_interval_seconds: int = Field(default=3600)
    max_concurrent_retraining_jobs: int = Field(default=5)
    retraining_deduplication_window_hours: int = Field(default=6)


class DataValidationConfig(BaseSettings):
    """Data validation configuration"""
    default_quality_threshold: float = Field(default=0.95)
    validation_timeout_ms: int = Field(default=500)
    max_dataset_rows: int = Field(default=10000)


class CanaryConfig(BaseSettings):
    """Canary deployment configuration"""
    initial_traffic_percentage: int = Field(default=5)
    observation_period_minutes: int = Field(default=30)
    traffic_increment_percentage: int = Field(default=25)
    max_rollout_hours: int = Field(default=4)
    error_rate_threshold: float = Field(default=0.05)
    latency_threshold_multiplier: float = Field(default=1.5)


class KubernetesConfig(BaseSettings):
    """Kubernetes configuration"""
    namespace: str = Field(default="mlops-prod")
    service_account: str = Field(default="mlops-deployer")
    image_pull_policy: str = Field(default="IfNotPresent")


class SlackConfig(BaseSettings):
    """Slack alerting configuration"""
    enabled: bool = Field(default=True)
    webhook_url: Optional[str] = Field(default=None)
    default_channel: str = Field(default="#mlops-alerts")
    rate_limit_per_model_per_hour: int = Field(default=10)


class WebhookConfig(BaseSettings):
    """Webhook configuration"""
    enabled: bool = Field(default=True)
    endpoints: List[Dict] = Field(default_factory=list)


class S3Config(BaseSettings):
    """S3 storage configuration"""
    bucket_prefix: str = Field(default="mlops-control-plane")
    model_artifacts_bucket: Optional[str] = Field(default=None)
    lineage_data_bucket: Optional[str] = Field(default=None)
    audit_logs_bucket: Optional[str] = Field(default=None)
    backup_bucket: Optional[str] = Field(default=None)
    region: str = Field(default="us-east-1")


class DatabaseConfig(BaseSettings):
    """PostgreSQL database configuration"""
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    database: str = Field(default="mlops_control_plane")
    username: str = Field(default="mlops")
    password: str = Field(default="")
    pool_size: int = Field(default=20)
    max_overflow: int = Field(default=10)


class PrometheusConfig(BaseSettings):
    """Prometheus monitoring configuration"""
    scrape_interval_seconds: int = Field(default=30)
    retention_days: int = Field(default=30)


class HealthCheckConfig(BaseSettings):
    """Health check configuration"""
    interval_seconds: int = Field(default=60)
    timeout_seconds: int = Field(default=10)
    unhealthy_threshold_minutes: int = Field(default=5)


class AirflowConfig(BaseSettings):
    """Airflow configuration"""
    base_url: Optional[str] = Field(default=None)
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    retraining_dag_id: str = Field(default="model_retraining_workflow")


class AWSConfig(BaseSettings):
    """AWS configuration"""
    region: str = Field(default="us-east-1")
    training_state_machine_arn: Optional[str] = Field(default=None)


class SecurityConfig(BaseSettings):
    """Security configuration"""
    api_keys: List[Dict] = Field(default_factory=list)
    cors_enabled: bool = Field(default=True)
    allowed_origins: List[str] = Field(default_factory=list)


class DisasterRecoveryConfig(BaseSettings):
    """Disaster recovery configuration"""
    backup_enabled: bool = Field(default=True)
    backup_schedule_cron: str = Field(default="0 2 * * *")
    retention_days: int = Field(default=30)
    verification_schedule_cron: str = Field(default="0 3 * * 0")


class Config:
    """Main configuration class"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.system = SystemConfig()
        self.models = ModelConfig()
        self.data_validation = DataValidationConfig()
        self.canary = CanaryConfig()
        self.kubernetes = KubernetesConfig()
        self.slack = SlackConfig()
        self.webhooks = WebhookConfig()
        self.s3 = S3Config()
        self.database = DatabaseConfig()
        self.prometheus = PrometheusConfig()
        self.health_check = HealthCheckConfig()
        self.airflow = AirflowConfig()
        self.aws = AWSConfig()
        self.security = SecurityConfig()
        self.disaster_recovery = DisasterRecoveryConfig()
        
        if config_file and os.path.exists(config_file):
            self.load_from_file(config_file)
    
    def load_from_file(self, config_file: str):
        """Load configuration from YAML file"""
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Update configurations from file
        if 'system' in config_data:
            self.system = SystemConfig(**config_data['system'])
        if 'models' in config_data:
            self.models = ModelConfig(**config_data['models'])
        if 'data_validation' in config_data:
            self.data_validation = DataValidationConfig(**config_data['data_validation'])
        if 'deployment' in config_data and 'canary' in config_data['deployment']:
            self.canary = CanaryConfig(**config_data['deployment']['canary'])
        if 'deployment' in config_data and 'kubernetes' in config_data['deployment']:
            self.kubernetes = KubernetesConfig(**config_data['deployment']['kubernetes'])
        if 'alerting' in config_data and 'slack' in config_data['alerting']:
            self.slack = SlackConfig(**config_data['alerting']['slack'])
        if 'alerting' in config_data and 'webhooks' in config_data['alerting']:
            self.webhooks = WebhookConfig(**config_data['alerting']['webhooks'])
        if 'storage' in config_data and 's3' in config_data['storage']:
            self.s3 = S3Config(**config_data['storage']['s3'])
        if 'database' in config_data and 'postgres' in config_data['database']:
            self.database = DatabaseConfig(**config_data['database']['postgres'])
        if 'monitoring' in config_data and 'prometheus' in config_data['monitoring']:
            self.prometheus = PrometheusConfig(**config_data['monitoring']['prometheus'])
        if 'monitoring' in config_data and 'health_check' in config_data['monitoring']:
            self.health_check = HealthCheckConfig(**config_data['monitoring']['health_check'])
        if 'airflow' in config_data:
            self.airflow = AirflowConfig(**config_data['airflow'])
        if 'aws' in config_data:
            self.aws = AWSConfig(**config_data['aws'])
        if 'security' in config_data:
            self.security = SecurityConfig(**config_data['security'])
        if 'disaster_recovery' in config_data:
            self.disaster_recovery = DisasterRecoveryConfig(**config_data['disaster_recovery'])
    
    def validate(self) -> bool:
        """Validate configuration"""
        # Add validation logic here
        return True


# Global configuration instance
config = Config(config_file=os.getenv('CONFIG_FILE', 'config.yaml'))
