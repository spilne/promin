/**
 * Fetch user profile from an unreliable API.
 * Retry 3x with jitter, timeout after 5s, return safe result.
 */

import { Pipeline } from "@promin/core";

interface User {
  id: string;
  name: string;
  email: string;
}

const fetchUser = (id: string) =>
  Pipeline.fn(async () => {
    const res = await fetch(`https://api.example.com/users/${id}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return (await res.json()) as User;
  })
    .retry({ maxRetries: 3, baseDelayMs: 500, jitter: true })
    .timeout(5_000);

// Usage
const { data, error } = await fetchUser("u_42").runSafe();

if (data) {
  console.log(`Hello, ${data.name}`);
} else {
  console.log(`Failed: ${error}`);
}
