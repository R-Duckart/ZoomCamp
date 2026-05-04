# Kestra Workflow Orchestration - GCP Setup

Google Cloud Platform-specific Kestra orchestration setup with GCP services integration.

## Prerequisites

- Docker and Docker Compose
- GCP Service Account with appropriate permissions
- GCP resources:
  - Cloud Storage buckets
  - BigQuery datasets
  - GCP Project credentials

## Setup

### 1. Configure GCP Credentials

Create a `.env` file in this directory:

```bash
GCP_CREDENTIALS_BASE64="<your-base64-encoded-service-account-json>"
KESTRA_PASSWORD="your-secure-password"
```

To encode your service account JSON:
```bash
cat service-account.json | base64
```

### 2. Start Services

```bash
docker-compose up -d
```

### 3. Access Kestra UI

- **URL**: http://localhost:8080
- **Username**: admin@kestra.io
- **Password**: Admin1234! (or custom if set in .env)

## Available Workflows

### GCP Configuration
- `01_gcp_kv.yaml` - Setup GCP Key Vault and secrets
- `02_gcp_setup.yaml` - Initialize GCP environment and resources
- `03_gcp_taxi.yaml` - Load taxi data to GCP Cloud Storage and BigQuery

## Architecture

```
Kestra → GCP Cloud Storage
      → GCP BigQuery
      → GCP Cloud Functions (optional)
```

## Environment Variables

Configure the following in `.env`:

| Variable | Description |
|----------|-------------|
| `GCP_CREDENTIALS_BASE64` | Service Account JSON (base64 encoded) |
| `KESTRA_PASSWORD` | Kestra UI admin password |

## Security Best Practices

⚠️ **IMPORTANT**: 

1. Never commit `.env` file to version control
2. Use `.env.example` as a template
3. Store credentials in CI/CD secrets or secure vaults
4. Rotate service account keys regularly
5. Limit service account permissions to minimum required scope

## Stopping Services

```bash
docker-compose down
```

## Troubleshooting

- **Connection refused?** Verify GCP credentials are valid and have network access
- **BigQuery access denied?** Ensure service account has `BigQuery Admin` or appropriate roles
- **Storage bucket not found?** Check project ID and bucket names
- **Authentication failed?** Validate base64 encoded credentials

## Further Reading

- [GCP Service Accounts](https://cloud.google.com/iam/docs/service-accounts)
- [Kestra GCP Integration](https://kestra.io/docs)
- [Cloud Storage Documentation](https://cloud.google.com/storage/docs)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
