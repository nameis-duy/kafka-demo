# Kafka Data Export Demo

A small .NET 9 sample that demonstrates an async export workflow using Kafka.

## Quick Start (5 commands)

1. Start Kafka and Kafka UI

```powershell
podman compose -f .\compose.yaml up -d
```

2. Run API (Terminal 1)

```powershell
dotnet run --project .\KafkaDataExport\KafkaDataExport.Api\KafkaDataExport.Api.csproj
```

3. Run Worker (Terminal 2)

```powershell
dotnet run --project .\KafkaDataExport\KafkaDataExport.Worker\KafkaDataExport.Worker.csproj
```

4. Submit export request (Terminal 3)

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:5145/api/export/request" -ContentType "application/json" -Body '{"customerId":"CUST-001","format":"json","requestedBy":"demo-user"}'
```

5. Check processed exports (Terminal 3)

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:5001/exports"
```

## API Usage Examples

### 1) Request export

- Method: POST
- URL: http://localhost:5145/api/export/request

Sample body:

```json
{
  "customerId": "CUST-001",
  "format": "json",
  "requestedBy": "demo-user"
}
```

PowerShell:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:5145/api/export/request" -ContentType "application/json" -Body '{"customerId":"CUST-001","format":"json","requestedBy":"demo-user"}'
```

Expected response:

- Status: 202 Accepted
- Body:

```json
{
  "jobId": "<generated-id>",
  "status": "Queued"
}
```

### 2) Get all exports

- Method: GET
- URL: http://localhost:5001/exports

PowerShell:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:5001/exports"
```

### 3) Get export by job id

- Method: GET
- URL: http://localhost:5001/exports/{jobId}

PowerShell:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:5001/exports/<jobId>"
```

## What this project contains

- API service: receives export requests and publishes messages to Kafka
- Worker service: consumes requests, generates export data, publishes results, and stores a summary in an in-memory database
- Kafka + Kafka UI: local messaging infrastructure via compose

Workspace structure:

- KafkaDataExport/KafkaDataExport.Api
- KafkaDataExport/KafkaDataExport.Worker
- compose.yaml

## High-level flow

1. Client calls API endpoint POST /api/export/request
2. API publishes an ExportRequestMessage to topic export-requests
3. Worker ExportRequestConsumer reads export-requests, generates export data, publishes to export-data
4. Worker ExportDataConsumer reads export-data and stores records in ExportDbContext (in-memory)
5. Client queries Worker endpoints to view stored export summaries

## Prerequisites

- .NET SDK 9.x
- Podman installed and running
- PowerShell terminal

## Kafka stack (compose)

Start Kafka and Kafka UI:

```powershell
podman compose -f .\compose.yaml up -d
```

Check status:

```powershell
podman compose -f .\compose.yaml ps
```

Open Kafka UI:

- http://localhost:8080
- If localhost forwarding is unavailable on your machine, use the Podman VM IP shown by:

```powershell
podman machine ssh "ip -4 addr show"
```

## Run the applications

Open two terminals from project root.

Terminal 1 (API):

```powershell
dotnet run --project .\KafkaDataExport\KafkaDataExport.Api\KafkaDataExport.Api.csproj
```

Terminal 2 (Worker):

```powershell
dotnet run --project .\KafkaDataExport\KafkaDataExport.Worker\KafkaDataExport.Worker.csproj
```

Default local ports:

- API: http://localhost:5145 and https://localhost:7020
- Worker: http://localhost:5001

## API endpoints

### Request an export

POST http://localhost:5145/api/export/request

Sample body:

```json
{
  "customerId": "CUST-001",
  "format": "json",
  "requestedBy": "demo-user"
}
```

Expected result:

- HTTP 202 Accepted
- JSON with JobId and Status=Queued

## Worker endpoints

### Get all stored exports

GET http://localhost:5001/exports

### Get export by job id

GET http://localhost:5001/exports/{jobId}

## Kafka topics

- export-requests
- export-data

List topics:

```powershell
podman exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Describe a topic:

```powershell
podman exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic export-requests
```

Read sample messages:

```powershell
podman exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic export-requests --from-beginning --max-messages 10
```

## Configuration notes

Current appsettings use a host-reachable Kafka endpoint:

- KafkaDataExport/KafkaDataExport.Api/appsettings.json
- KafkaDataExport/KafkaDataExport.Worker/appsettings.json

If Podman host forwarding for localhost is unavailable, use Podman VM IP (example: 172.30.165.194:9092).

Important: Podman VM IP may change after restart. If connection errors return, update:

- compose.yaml (KAFKA_ADVERTISED_LISTENERS external endpoint)
- API appsettings BootstrapServers
- Worker appsettings BootstrapServers

## Common troubleshooting

### Error: 1/1 brokers are down

- Ensure Kafka is up: podman compose ps
- Ensure endpoint is reachable from host
- Confirm API/Worker BootstrapServers matches reachable Kafka address

### Failed to load Worker endpoint

If http://localhost:5001/exports returns connection refused, Worker is not running. Start Worker project.

### Compose validation or startup issues

Validate compose file:

```powershell
podman compose -f .\compose.yaml config
```

## Notes

- Worker uses an in-memory database, so stored export summaries are reset when Worker restarts.
- This project is suitable for local development/demo, not production hardening.
