# Concurrency & Overselling Test Script
# Fires 10 concurrent order requests for Product #55 (stock = 2) via API Gateway (port 8080)

param (
    [string]$GatewayUrl = "http://localhost:8080",
    [int]$ProductId = 55,
    [int]$InitialStock = 2,
    [int]$ConcurrentRequests = 10
)

Write-Host "=== CONCURRENCY & OVERSELLING TEST ===" -ForegroundColor Cyan

# 1. Login to obtain JWT
Write-Host "Step 1: Authenticating as admin..."
$loginBody = @{ username = "admin"; password = "admin123" } | ConvertTo-Json
try {
    $loginRes = Invoke-RestMethod -Uri "$GatewayUrl/auth/login" -Method Post -Body $loginBody -ContentType "application/json"
    $token = $loginRes.token
    Write-Host "JWT obtained successfully." -ForegroundColor Green
} catch {
    Write-Error "Failed to obtain JWT from $GatewayUrl/auth/login. Ensure microservices are running."
    exit 1
}

$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type" = "application/json"
}

# 2. Reset product stock to initial stock (2 units)
Write-Host "Step 2: Resetting stock of Product $ProductId to $InitialStock units..."
$stockBody = @{ availableQuantity = $InitialStock; reservedQuantity = 0 } | ConvertTo-Json
$stockRes = Invoke-RestMethod -Uri "$GatewayUrl/api/products/$ProductId/stock" -Method Put -Headers $headers -Body $stockBody
Write-Host "Product $ProductId stock set to: Available=$($stockRes.availableQuantity), Reserved=$($stockRes.reservedQuantity)" -ForegroundColor Green

# 3. Fire concurrent order requests
Write-Host "Step 3: Firing $ConcurrentRequests concurrent orders for 1 unit each..."
$jobs = @()
for ($i = 1; $i -le $ConcurrentRequests; $i++) {
    $orderBody = @{ customerId = 101; productId = $ProductId; quantity = 1 } | ConvertTo-Json
    $job = Start-Job -ScriptBlock {
        param($url, $hdr, $body, $index)
        try {
            $res = Invoke-RestMethod -Uri "$url/api/orders" -Method Post -Headers $hdr -Body $body
            return [PSCustomObject]@{ Index = $index; Success = $true; OrderId = $res.orderId; Status = $res.status; Error = $null }
        } catch {
            return [PSCustomObject]@{ Index = $index; Success = $false; OrderId = $null; Status = "FAILED"; Error = $_.Exception.Message }
        }
    } -ArgumentList $GatewayUrl, $headers, $orderBody, $i
    $jobs += $job
}

$results = $jobs | Wait-Job | Receive-Job
$jobs | Remove-Job

Write-Host "Step 4: All $ConcurrentRequests requests completed."
$results | Format-Table Index, Success, OrderId, Status, Error

# 4. Wait for Kafka asynchronous event processing
Write-Host "Waiting 3 seconds for Kafka event processing..."
Start-Sleep -Seconds 3

# 5. Check product inventory
$finalProduct = Invoke-RestMethod -Uri "$GatewayUrl/api/products/$ProductId" -Method Get -Headers $headers
Write-Host "=== FINAL INVENTORY STATUS ===" -ForegroundColor Yellow
Write-Host "Product ID: $ProductId"
Write-Host "Available Quantity: $($finalProduct.availableQuantity)"
Write-Host "Reserved Quantity:  $($finalProduct.reservedQuantity)"

# 6. Check customer orders
$orders = Invoke-RestMethod -Uri "$GatewayUrl/api/orders/customer/101" -Method Get -Headers $headers
$confirmed = ($orders | Where-Object { $_.status -eq "CONFIRMED" }).Count
$outOfStock = ($orders | Where-Object { $_.status -eq "OUT_OF_STOCK" }).Count
Write-Host "Orders CONFIRMED:    $confirmed" -ForegroundColor Green
Write-Host "Orders OUT_OF_STOCK: $outOfStock" -ForegroundColor Red
