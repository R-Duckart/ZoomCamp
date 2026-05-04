# Workflow Orchestration with Kestra

This directory contains Kestra workflow orchestration setups for three different environments: **Default (Local)**, **Azure**, and **Google Cloud Platform (GCP)**.

## Directory Structure

```
02-workflow-orchestration/
├── default/              # Local PostgreSQL + Kestra setup
│   ├── flows/            # Workflow definitions
│   ├── scripts/          # Helper scripts
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   ├── pyproject.toml
│   └── README.md
│
├── azure/                # Azure-specific setup
│   ├── flow/             # Workflow definitions for Azure
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   ├── .env.example
│   └── README.md
│
├── gcp/                  # GCP-specific setup
│   ├── flows/            # Workflow definitions for GCP
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   ├── .env.example
│   └── README.md
│
└── README.md             # This file
```

## Quick Start by Environment

### 1. Local Setup (Default)

```bash
cd default
docker-compose up -d
```

Access Kestra at: http://localhost:8080

**Use this for:**
- Development and testing
- Learning Kestra
- Local workflow testing

See [default/README.md](default/README.md) for details.

### 2. Azure Setup

```bash
cd azure
cp .env.example .env
# Edit .env with your Azure credentials
docker-compose up -d
```

Access Kestra at: http://localhost:8080

**Use this for:**
- Azure cloud workflows
- Integrating with Azure services
- Production pipelines on Azure

See [azure/README.md](azure/README.md) for details.

### 3. GCP Setup

```bash
cd gcp
cp .env.example .env
# Edit .env with your GCP credentials
docker-compose up -d
```

Access Kestra at: http://localhost:8080

**Use this for:**
- GCP cloud workflows
- Integrating with Google Cloud services
- Production pipelines on GCP

See [gcp/README.md](gcp/README.md) for details.

## Common Tasks

### Stop All Services

```bash
cd <environment>
docker-compose down
```

### View Logs

```bash
cd <environment>
docker-compose logs -f kestra
```

### Access PostgreSQL

```bash
# Local setup
docker exec -it postgres psql -U root -d ny_taxi

# Azure/GCP setup
docker exec -it postgres psql -U kestra -d kestra
```

### Create New Workflow

1. Open Kestra UI
2. Click **Flows** → **Create**
3. Define your workflow in YAML
4. Save and trigger

Or add YAML files to the `flows/` directory.

## Security Notes

⚠️ **Important**:

- Never commit `.env` files with real credentials
- Use `.env.example` as a template
- Store sensitive data in environment variables or secret managers
- Rotate credentials regularly
- Use service accounts with minimal required permissions

## Kestra Features

- **Declarative Workflows**: Define workflows in YAML
- **Rich Task Library**: 100+ built-in tasks
- **Scheduling**: Cron expressions for scheduled workflows
- **Error Handling**: Retry policies, conditional execution
- **Monitoring**: Built-in UI for workflow monitoring
- **Scalability**: Distributed task execution

## Default Credentials

| Service | Default | Production |
|---------|---------|-----------|
| Kestra UI | admin@kestra.io / Admin1234! | Set in `.env` |
| PostgreSQL (local) | root / root | Change in docker-compose.yaml |
| PostgreSQL (cloud) | kestra / k3str4 | Change in docker-compose.yaml |

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Port already in use | Change ports in docker-compose.yaml |
| Docker socket error | Check docker permissions or use rootless Docker |
| Credential errors | Verify .env file and base64 encoding |
| Workflow not executing | Check logs: `docker-compose logs kestra` |

## Learning Resources

- [Kestra Official Documentation](https://kestra.io/docs)
- [Kestra Workflows](https://kestra.io/flows)
- [Cloud Platform Docs](#)
  - [Azure Documentation](https://learn.microsoft.com/en-us/azure/)
  - [GCP Documentation](https://cloud.google.com/docs)

## Support

For issues or questions:
1. Check the environment-specific README
2. Review Kestra documentation
3. Check Docker logs: `docker-compose logs`
4. Verify credentials and permissions

---

**Last Updated**: May 2026
**Kestra Version**: Latest Docker image
