import http from "k6/http";
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";
const FAILURE_CUSTOMER = __ENV.FAILURE_CUSTOMER || "phase6-failure-demo";

export const options = {
  stages: [
    { duration: "20s", target: 10 },
    { duration: "40s", target: 25 },
    { duration: "60s", target: 45 },
    { duration: "25s", target: 20 },
    { duration: "20s", target: 0 },
  ],
  thresholds: {
    http_req_failed: ["rate<0.05"],
    http_req_duration: ["p(95)<1200", "p(99)<2200"],
  },
};

function payload(iteration) {
  const forceFailure = __ITER % 9 === 0;
  return JSON.stringify({
    customer_id: forceFailure ? FAILURE_CUSTOMER : `ramp-customer-${__VU}`,
    idempotency_key: `ramp-${__VU}-${iteration}-${Date.now()}`,
    items: [{ sku: forceFailure ? "sku-fail" : "sku-ramp", qty: 1 }],
  });
}

export default function () {
  const res = http.post(`${BASE_URL}/orders`, payload(__ITER), {
    headers: { "Content-Type": "application/json" },
    timeout: "10s",
  });

  check(res, {
    "status is known": (r) => [202, 409, 503].includes(r.status),
  });

  sleep(0.2);
}
