#!/usr/bin/env bash
# Concurrency & Overselling Test Script
# Fires 10 concurrent order requests for Product #55 (stock = 2) via API Gateway (port 8080)

GATEWAY_URL=${GATEWAY_URL:-"http://localhost:8080"}
PRODUCT_ID=${PRODUCT_ID:-55}
INITIAL_STOCK=${INITIAL_STOCK:-2}
CONCURRENT_REQUESTS=${CONCURRENT_REQUESTS:-10}

echo "=== CONCURRENCY & OVERSELLING TEST ==="

# 1. Obtain JWT
echo "Step 1: Authenticating as admin..."
LOGIN_RESP=$(curl -s -X POST "$GATEWAY_URL/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}')

TOKEN=$(echo "$LOGIN_RESP" | grep -o '"token":"[^"]*' | cut -d'"' -f4)

if [ -z "$TOKEN" ]; then
  echo "Failed to obtain JWT token."
  exit 1
fi
echo "JWT obtained successfully."

# 2. Reset product stock to 2 units
echo "Step 2: Resetting stock of Product $PRODUCT_ID to $INITIAL_STOCK units..."
curl -s -X PUT "$GATEWAY_URL/api/products/$PRODUCT_ID/stock" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"availableQuantity\":$INITIAL_STOCK,\"reservedQuantity\":0}" > /dev/null

echo "Stock reset to $INITIAL_STOCK units."

# 3. Fire 10 concurrent requests
echo "Step 3: Firing $CONCURRENT_REQUESTS concurrent orders..."
for i in $(seq 1 $CONCURRENT_REQUESTS); do
  curl -s -X POST "$GATEWAY_URL/api/orders" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d "{\"customerId\":101,\"productId\":$PRODUCT_ID,\"quantity\":1}" &
done
wait

echo "Step 4: All requests dispatched. Waiting 3s for Kafka processing..."
sleep 3

# 4. Check Final Stock
echo "=== FINAL INVENTORY STATUS ==="
curl -s -X GET "$GATEWAY_URL/api/products/$PRODUCT_ID" \
  -H "Authorization: Bearer $TOKEN"
echo ""
