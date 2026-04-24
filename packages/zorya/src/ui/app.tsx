import { useEffect, useState } from "preact/hooks";
import { Sidebar } from "./components/layout/sidebar.tsx";
import { RunList } from "./components/runs/run-list.tsx";
import { RunDetail } from "./components/runs/run-detail.tsx";
import { WorkerGrid } from "./components/workers/worker-grid.tsx";
import { ScheduleList } from "./components/schedules/schedule-list.tsx";
import { WorkflowList } from "./components/workflows/workflow-list.tsx";
import { DialogHost, ToastHost } from "./components/ui/dialog-host.tsx";

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
    <div class="flex min-h-screen">
      <Sidebar route={route} onNavigate={navigate} />
      <main class="flex-1 min-w-0">{renderRoute(route, navigate)}</main>
      <DialogHost />
      <ToastHost />
    </div>
  );
}

function locationToRoute(): string {
  const hash = location.hash.replace(/^#/, "");
  return hash || "/";
}

function renderRoute(route: string, navigate: (p: string) => void) {
  const [path, query] = route.split("?");
  const params = new URLSearchParams(query ?? "");
  if (path === "/workers") {
    return <WorkerGrid />;
  }
  if (path === "/schedules") {
    return <ScheduleList onNavigate={navigate} />;
  }
  if (path === "/workflows") {
    return (
      <WorkflowList
        onOpenRun={(id) => navigate(`/runs/${encodeURIComponent(id)}`)}
        onOpenWorkflow={(name) => navigate(`/?name=${encodeURIComponent(name)}`)}
      />
    );
  }
  const runMatch = /^\/runs\/([^/]+)$/.exec(path ?? "");
  if (runMatch) {
    return (
      <RunDetail
        id={decodeURIComponent(runMatch[1]!)}
        queryParams={params}
        onBack={() => navigate("/")}
        onOpenRun={(id) => navigate(`/runs/${encodeURIComponent(id)}`)}
        onQueryChange={(qp) => {
          const qs = qp.toString();
          const base = `/runs/${encodeURIComponent(decodeURIComponent(runMatch[1]!))}`;
          const next = qs ? `${base}?${qs}` : base;
          const current = locationToRoute();
          if (next === current) return;
          history.replaceState(null, "", `#${next}`);
          window.dispatchEvent(new HashChangeEvent("hashchange"));
        }}
      />
    );
  }
  return (
    <RunList
      queryParams={params}
      onOpen={(id) => navigate(`/runs/${encodeURIComponent(id)}`)}
      onQueryChange={(qp) => {
        // Replace hash without adding a history entry so rapid typing doesn't
        // pollute back-button history.
        const qs = qp.toString();
        const next = qs ? `/?${qs}` : "/";
        const current = locationToRoute();
        if (next === current) return;
        history.replaceState(null, "", `#${next}`);
        // replaceState doesn't fire hashchange, so notify listeners manually.
        window.dispatchEvent(new HashChangeEvent("hashchange"));
      }}
    />
  );
}
