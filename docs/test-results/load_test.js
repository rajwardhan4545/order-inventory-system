import http from 'k6/http';
import { check, sleep } from 'k6';

// k6 Load Test: 50 Virtual Users for 60 seconds against Order Service
export const options = {
  vus: 50,
  duration: '60s',
};

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:8080';

export function setup() {
  // Step 1: Login to acquire Bearer JWT
  const loginRes = http.post(
    `${BASE_URL}/auth/login`,
    JSON.stringify({ username: 'admin', password: 'admin123' }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  const token = loginRes.json('token');

  // Step 2: Ensure product 56 has huge stock (50,000 units) so stock never runs out during test
  http.put(
    `${BASE_URL}/api/products/56/stock`,
    JSON.stringify({ availableQuantity: 50000, reservedQuantity: 0 }),
    {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
    }
  );

  return { token };
}

export default function (data) {
  const payload = JSON.stringify({
    customerId: 101,
    productId: 56,
    quantity: 1,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${data.token}`,
    },
  };

  const res = http.post(`${BASE_URL}/api/orders`, payload, params);

  check(res, {
    'status is 201': (r) => r.status === 201,
    'orderId returned': (r) => r.json('orderId') !== undefined,
  });

  sleep(0.02); // 20ms pause between requests per VU
}
