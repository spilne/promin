/**
 * Build a dashboard by fetching 3 APIs in parallel.
 * Each branch has its own resilience: retry, fallback, or cache.
 * Total timeout of 10s cancels all in-flight requests.
 */

import { Pipeline, PipelineCache } from "@promin/core";

interface User {
  id: string;
  name: string;
}
interface Video {
  title: string;
}
interface Stats {
  views: number;
}

const subscriptionCache = new PipelineCache<{ tier: string }>(5 * 60_000);

async function getDashboard(userId: string) {
  const fetchUser = Pipeline.fn(() =>
    fetch(`/api/users/${userId}`).then((r) => r.json() as Promise<User>),
  );

  const fetchVideos = Pipeline.fn(() =>
    fetch(`/api/users/${userId}/videos`).then((r) => r.json() as Promise<Video[]>),
  ).retry(3);

  const fetchStats = Pipeline.fn(() =>
    fetch(`/api/users/${userId}/stats`).then((r) => r.json() as Promise<Stats>),
  ).orElse({ views: 0 });

  const fetchSubscription = Pipeline.fn(() =>
    fetch(`/api/users/${userId}/sub`).then((r) => r.json() as Promise<{ tier: string }>),
  ).cached(subscriptionCache);

  return Pipeline.all(fetchUser, fetchVideos, fetchStats, fetchSubscription)
    .map(([user, videos, stats, sub]) => ({ user, videos, stats, sub }))
    .timeout(10_000)
    .runPromise();
}

const dashboard = await getDashboard("u_42");
console.log(dashboard);
