import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";

export const options = {
  vus: 40,
  duration: "45s",
  thresholds: {
    http_req_failed: ["rate<0.02"],
    http_req_duration: ["p(95)<800", "p(99)<1500"],
  },
};

function payload(iteration) {
  return JSON.stringify({
    customer_id: `burst-customer-${__VU}`,
    idempotency_key: `burst-${__VU}-${iteration}-${Date.now()}`,
    items: [{ sku: "sku-burst", qty: 1 }],
  });
}

export default function () {
  const res = http.post(`${BASE_URL}/orders`, payload(__ITER), {
    headers: { "Content-Type": "application/json" },
    timeout: "10s",
  });

  check(res, {
    "status is accepted/conflict/service-unavailable": (r) => [202, 409, 503].includes(r.status),
  });

  sleep(0.15);
}
