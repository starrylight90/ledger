param(
    [string]$ProjectName = "ledger",
    [string]$Topic = "order.created"
)

$ErrorActionPreference = "Stop"

Write-Host "[1/4] Creating topic $Topic"
docker compose -p $ProjectName exec -T redpanda rpk topic create $Topic 2>$null | Out-Null

$event = '{"event_id":"00000000-0000-0000-0000-000000000001","event_type":"OrderCreated","timestamp":"2026-02-18T22:50:00Z","correlation_id":"00000000-0000-0000-0000-000000000001","payload":{"order_id":"00000000-0000-0000-0000-000000000001","customer_id":"demo","items":[{"sku":"sku-1","qty":1}]}}'

Write-Host "[2/4] Producing event"
$event | docker compose -p $ProjectName exec -T redpanda rpk topic produce $Topic

Write-Host "[3/4] Consuming one event"
$result = docker compose -p $ProjectName exec -T redpanda rpk topic consume $Topic -n 1 -f '%v'

Write-Host "[4/4] Verifying payload"
if ($result -match 'OrderCreated') {
    Write-Host "Round-trip successful"
    exit 0
}

Write-Error "Round-trip failed: expected OrderCreated payload"
exit 1
