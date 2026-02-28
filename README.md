# MLOps Observability Control Plane

A production-grade distributed system for monitoring, managing, and maintaining machine learning models in production environments. This control plane provides automated drift detection, data quality validation, intelligent retraining orchestration, safe deployment strategies, and comprehensive observability for ML operations at scale.

## Features

### Core Capabilities

- **Automated Drift Detection**: Continuous monitoring of model predictions and input data distributions using EvidentlyAI
- **Data Quality Validation**: Real-time validation of inference data against configurable quality rules using Great Expectations
- **Intelligent Retraining**: Automated model retraining workflows triggered by drift detection and performance degradation
- **Canary Deployments**: Progressive rollout strategy with automated health checks and rollback capabilities
- **Comprehensive Alerting**: Multi-channel notifications via Slack and webhooks with rate limiting
- **Lineage Tracking**: Complete feature and model lineage with DAG visualization
- **Multi-Model Support**: Manage and monitor multiple models with isolated configurations
- **Infrastructure as Code**: Full Terraform modules for AWS infrastructure provisioning

### Architecture Highlights

- **Event-Driven**: Loosely coupled microservices communicating via AWS SNS/SQS
- **Cloud-Native**: Containerized deployment on Kubernetes/EKS with Helm charts
- **Observable**: Prometheus metrics, structured logging, and audit trails
- **Resilient**: Automatic rollback, health checks, and disaster recovery capabilities
- **Scalable**: Horizontal scaling with Redis caching and connection pooling

## Technology Stack

- **API Framework**: FastAPI with async support
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Caching**: Redis for rate limiting and deduplication
- **Monitoring**: Prometheus + EvidentlyAI for drift detection
- **Data Quality**: Great Expectations validation suites
- **Orchestration**: Apache Airflow + AWS Step Functions
- **Deployment**: Kubernetes/EKS with Helm charts
- **Infrastructure**: Terraform for AWS resource management
- **Testing**: Pytest + Hypothesis for property-based testing

## Project Structure

```
.
├── src/                          # Source code
│   ├── config.py                 # Configuration management
│   ├── logging_config.py         # Structured logging setup
│   ├── models.py                 # SQLAlchemy database models
│   └── __init__.py
├── config.yaml                   # System configuration
├── docker-compose.yml            # Local development environment
├── prometheus.yml                # Prometheus configuration
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## Getting Started

### Prerequisites

- Python 3.11+
- Docker and Docker Compose
- PostgreSQL 14+
- Redis 7+
- AWS Account (for production deployment)
- Kubernetes cluster (for production deployment)

### Local Development Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd mlops-observability-control-plane
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Start local infrastructure**
   ```bash
   docker-compose up -d
   ```
   This starts PostgreSQL, Redis, and Prometheus locally.

5. **Configure the system**
   Edit `config.yaml` to match your environment. Key settings:
   - Database connection details
   - AWS credentials and region
   - Airflow connection details
   - Slack webhook URL (optional)
   - Model-specific thresholds

6. **Run database migrations**
   ```bash
   alembic upgrade head
   ```

7. **Start the API server**
   ```bash
   uvicorn src.main:app --reload --port 8000
   ```

### Configuration

The system is configured via `config.yaml`. Key configuration sections:

- **models**: Drift thresholds, retraining settings
- **data_validation**: Quality thresholds, validation timeouts
- **deployment**: Canary deployment parameters, rollback thresholds
- **alerting**: Slack and webhook configurations
- **storage**: S3 bucket names and regions
- **database**: PostgreSQL connection details
- **monitoring**: Prometheus and health check settings

See `config.yaml` for detailed configuration options.

## API Endpoints

### Health & Metrics
- `GET /health` - System health check
- `GET /metrics` - Prometheus metrics

### Model Monitoring
- `POST /api/v1/monitor/predictions` - Submit prediction data for drift detection
- `GET /api/v1/monitor/drift/{model_id}` - Get drift metrics for a model

### Data Validation
- `POST /api/v1/validate/data` - Validate inference data
- `GET /api/v1/validate/suites` - List available validation suites

### Retraining
- `POST /api/v1/retrain/trigger` - Manually trigger retraining
- `GET /api/v1/retrain/status/{job_id}` - Get retraining job status

### Deployment
- `POST /api/v1/deploy/canary` - Start canary deployment
- `POST /api/v1/deploy/rollback` - Trigger manual rollback
- `GET /api/v1/deploy/status/{id}` - Get deployment status

### Lineage
- `POST /api/v1/lineage/feature` - Record feature lineage
- `POST /api/v1/lineage/model` - Record model lineage
- `GET /api/v1/lineage/query` - Query lineage relationships
- `GET /api/v1/lineage/visualize/{id}` - Generate lineage DAG visualization

### Model Management
- `GET /api/v1/models` - List all registered models
- `GET /api/v1/models/{id}` - Get model details
- `POST /api/v1/models` - Register new model
- `PUT /api/v1/models/{id}/config` - Update model configuration

## Testing

### Run Unit Tests
```bash
pytest tests/unit -v
```

### Run Integration Tests
```bash
pytest tests/integration -v
```

### Run Property-Based Tests
```bash
pytest tests/properties -v
```

### Run with Coverage
```bash
pytest --cov=src --cov-report=html
```

### Performance Testing
```bash
locust -f tests/performance/locustfile.py
```

## Deployment

### Docker Build
```bash
docker build -t mlops-control-plane:latest .
```

### Kubernetes Deployment with Helm
```bash
helm install mlops-control-plane ./helm/mlops-control-plane \
  --namespace mlops \
  --values helm/values-prod.yaml
```

### Terraform Infrastructure Provisioning
```bash
cd terraform/environments/prod
terraform init
terraform plan
terraform apply
```

## Monitoring & Observability

### Prometheus Metrics
- Drift scores per model
- Data quality metrics per validation suite
- Deployment status per environment
- Alert delivery success rates
- Retraining job status

### Logs
- Structured JSON logs with correlation IDs
- Audit logs stored in S3 with daily rotation
- Component-specific log levels

### Alerts
- Drift detection alerts
- Data quality failures
- Deployment failures and rollbacks
- System health degradation

## Security

- **API Authentication**: API key-based authentication with role-based access control
- **Secrets Management**: AWS Secrets Manager integration
- **Encryption**: S3 encryption at rest, TLS in transit
- **Network Security**: VPC isolation, security groups, least-privilege IAM policies
- **Audit Logging**: Complete audit trail of all operations

## Disaster Recovery

- **Automated Backups**: Daily backups of configuration, lineage data, and model registry
- **Point-in-Time Recovery**: 30-day retention with versioning
- **Backup Verification**: Weekly automated verification
- **Restore Procedures**: Documented runbooks for recovery scenarios

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Follow PEP 8 style guide
- Write unit tests for new features
- Add property tests for critical correctness properties
- Update documentation for API changes
- Run linting and type checking before committing

## License

[Add your license here]

## Support

For issues, questions, or contributions, please open an issue on GitHub.

## Roadmap

- [ ] Multi-cloud support (Azure, GCP)
- [ ] Advanced drift detection algorithms
- [ ] A/B testing framework
- [ ] Model explainability integration
- [ ] Cost optimization recommendations
- [ ] Self-healing capabilities

## Acknowledgments

Built with:
- [FastAPI](https://fastapi.tiangolo.com/)
- [EvidentlyAI](https://www.evidentlyai.com/)
- [Great Expectations](https://greatexpectations.io/)
- [Apache Airflow](https://airflow.apache.org/)
- [Prometheus](https://prometheus.io/)
