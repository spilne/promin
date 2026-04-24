import { useEffect, useState } from "preact/hooks";
import { Navbar } from "./components/layout/navbar.tsx";
import { RunList } from "./components/runs/run-list.tsx";
import { RunDetail } from "./components/runs/run-detail.tsx";
import { WorkerGrid } from "./components/workers/worker-grid.tsx";

export function App() {
  const [route, setRoute] = useState(locationToRoute());

  useEffect(() => {
    const onHash = () => setRoute(locationToRoute());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const navigate = (path: string) => {
    location.hash = path;
  };

  return (
    <div>
      <Navbar route={route} onNavigate={navigate} />
      {renderRoute(route, navigate)}
    </div>
  );
}

function locationToRoute(): string {
  const hash = location.hash.replace(/^#/, "");
  return hash || "/";
}

function renderRoute(route: string, navigate: (p: string) => void) {
  if (route === "/workers") {
    return <WorkerGrid />;
  }
  const runMatch = /^\/runs\/([^/]+)$/.exec(route);
  if (runMatch) {
    return <RunDetail id={decodeURIComponent(runMatch[1]!)} onBack={() => navigate("/")} />;
  }
  return <RunList onOpen={(id) => navigate(`/runs/${encodeURIComponent(id)}`)} />;
}
