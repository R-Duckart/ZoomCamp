# Kestra Workflow Orchestration - Azure Setup

Azure-specific Kestra orchestration setup with Azure Blob Storage, Azure SQL Database, and Azure Synapse Analytics.

## Prerequisites

- Docker and Docker Compose
- Azure Credentials (Service Principal)
- Azure resources:
  - Storage Account (Blob Storage)
  - Azure SQL Database
  - Azure Synapse Analytics Workspace (optional)

## Setup

### 1. Configure Azure Credentials

Create a `.env` file in this directory:

```bash
AZURE_CREDENTIALS_JSON="<your-base64-encoded-service-principal-json>"
```

To encode your service principal JSON:
```bash
cat azure-credentials.json | base64
```

### 2. Start Services

```bash
docker-compose up -d
```

### 3. Access Kestra UI

- **URL**: http://localhost:8080
- **Username**: admin@kestra.io
- **Password**: Admin1234!

## Available Workflows

### Connectivity Tests
- `00_azure_blob_test.yaml` - Test Azure Blob Storage connectivity
- `99_azure_connectivity_test.yaml` - Comprehensive connectivity verification

### Azure Resource Configuration
- `01_azure_ressources_kv.yaml` - Setup and manage resources via Key Vault
- `02_azure_db_sql_kv.yaml` - Configure SQL Database connections

### Data Pipelines
- `03_azure_taxi_staging.yaml` - Stage taxi data in Blob Storage
- `04_azure_taxi_json.yaml` - Process taxi data in JSON format

### Azure Synapse Integration
- `05_azure_synapse_kv.yaml` - Synapse authentication via Key Vault
- `06_azure_synapse_table.yaml` - Create Synapse tables
- `07_azure_synapse_view.yaml` - Create Synapse views

## Architecture

```
Kestra → Azure Blob Storage
      → Azure SQL Database
      → Azure Synapse Analytics
```

## Stopping Services

```bash
docker-compose down
```

## Environment Variables

Configure the following in `.env`:

| Variable | Description |
|----------|-------------|
| `AZURE_CREDENTIALS_JSON` | Service Principal JSON (base64 encoded) |
| `AZURE_SUBSCRIPTION_ID` | Azure Subscription ID |
| `AZURE_RESOURCE_GROUP` | Azure Resource Group name |

## Troubleshooting

- **Connection refused?** Verify Azure credentials are valid
- **Storage account not found?** Check resource group and storage account names
- **Authentication failed?** Ensure service principal has necessary permissions

## Further Reading

- [Azure Credentials Setup](../docs/Azure.md)
- [Kestra Azure Integration](https://kestra.io/docs)
- [Azure Service Principal](https://docs.microsoft.com/en-us/azure/active-directory/develop/app-objects-and-service-principals)
