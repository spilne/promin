import { useEffect, useState } from "preact/hooks";
import { Sidebar } from "./components/layout/sidebar.tsx";
import { RunList } from "./components/runs/run-list.tsx";
import { RunDetail } from "./components/runs/run-detail.tsx";
import { WorkerGrid } from "./components/workers/worker-grid.tsx";
import { ScheduleList } from "./components/schedules/schedule-list.tsx";
import { ScheduleDetail } from "./components/schedules/schedule-detail.tsx";
import { WorkflowList } from "./components/workflows/workflow-list.tsx";
import { WorkflowDetail } from "./components/workflows/workflow-detail.tsx";
import { AgentList } from "./components/agents/agent-list.tsx";
import { AgentDetail } from "./components/agents/agent-detail.tsx";
import { InstanceList } from "./components/agents/instance-list.tsx";
import { SignalList } from "./components/signals/signal-list.tsx";
import { SharedSignalPage } from "./components/signals/shared-signal-page.tsx";
import { SecretsPage } from "./components/secrets/secrets-page.tsx";
import { ToolsPage } from "./components/tools/tools-page.tsx";
import { SkillsPage } from "./components/skills/skills-page.tsx";
import { RolesPage } from "./components/roles/roles-page.tsx";
import { DagsPage } from "./components/dags/dags-page.tsx";
import { DialogHost, ToastHost } from "./components/ui/dialog-host.tsx";

export function App() {
  const [route, setRoute] = useState(locationToRoute());

  useEffect(() => {
    // Keep route navigation as a plain state update. The page-level
    // component remount already triggers `.anim-page` (180ms fade-in)
    // on every navigation, which is enough cross-fade affordance.
    //
    // Tried wrapping this in `document.startViewTransition` for a
    // browser-native cross-fade, but the API's callback holds the DOM
    // frozen until the new tree settles — pages with useEffect
    // data-fetching trip its 4s internal timeout and surface as a
    // visible 2-second hang plus a console "Transition was aborted"
    // error. Not worth it for the route boundary.
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
  // Public share page — combined credential in the path segment:
  // /share/<tokenId>.<bearer>. The describe + complete endpoints are
  // bearer-authed only, so this route is reachable without dashboard auth.
  const shareMatch = /^\/share\/([^/]+)$/.exec(path ?? "");
  if (shareMatch) {
    const combined = decodeURIComponent(shareMatch[1]!);
    const dotIdx = combined.indexOf(".");
    const tokenId = dotIdx > 0 ? combined.slice(0, dotIdx) : "";
    const bearer = dotIdx > 0 ? combined.slice(dotIdx + 1) : "";
    return <SharedSignalPage tokenId={tokenId} bearer={bearer} />;
  }
  if (path === "/workers") {
    return <WorkerGrid onOpenRun={(id) => navigate(`/runs/${encodeURIComponent(id)}`)} />;
  }
  if (path === "/schedules") {
    return <ScheduleList onNavigate={navigate} />;
  }
  if (path === "/signals") {
    return <SignalList onOpenRun={(id) => navigate(`/runs/${encodeURIComponent(id)}`)} />;
  }
  if (path === "/secrets") {
    return <SecretsPage />;
  }
  if (path === "/tools") {
    return <ToolsPage />;
  }
  if (path === "/skills") {
    return <SkillsPage />;
  }
  if (path === "/roles") {
    return <RolesPage onOpenAgent={(id) => navigate(`/agents/${encodeURIComponent(id)}`)} />;
  }
  if (path === "/dags") {
    return <DagsPage />;
  }
  const scheduleMatch = /^\/schedules\/([^/]+)$/.exec(path ?? "");
  if (scheduleMatch) {
    return (
      <ScheduleDetail
        id={decodeURIComponent(scheduleMatch[1]!)}
        onBack={() => navigate("/schedules")}
        onOpenRun={(id) => navigate(`/runs/${encodeURIComponent(id)}`)}
      />
    );
  }
  if (path === "/agents") {
    return <AgentList onOpen={(id) => navigate(`/agents/${encodeURIComponent(id)}`)} />;
  }
  if (path === "/instances") {
    return <InstanceList onOpenAgent={(id) => navigate(`/agents/${encodeURIComponent(id)}`)} />;
  }
  const agentMatch = /^\/agents\/([^/]+)$/.exec(path ?? "");
  if (agentMatch) {
    return (
      <AgentDetail
        id={decodeURIComponent(agentMatch[1]!)}
        onBack={() => navigate("/agents")}
        onOpenAgent={(id) => navigate(`/agents/${encodeURIComponent(id)}`)}
      />
    );
  }
  if (path === "/workflows") {
    return (
      <WorkflowList
        onOpenRun={(id) => navigate(`/runs/${encodeURIComponent(id)}`)}
        onOpenWorkflow={(name) => navigate(`/workflows/${encodeURIComponent(name)}`)}
        onOpenWorkflowRuns={(name) => navigate(`/?name=${encodeURIComponent(name)}`)}
      />
    );
  }
  const workflowMatch = /^\/workflows\/([^/]+)$/.exec(path ?? "");
  if (workflowMatch) {
    const workflowName = decodeURIComponent(workflowMatch[1]!);
    return (
      <WorkflowDetail
        name={workflowName}
        onBack={() => navigate("/workflows")}
        onOpenRun={(id) => navigate(`/runs/${encodeURIComponent(id)}`)}
        onOpenVersionRuns={(version) =>
          navigate(
            `/?name=${encodeURIComponent(workflowName)}&version=${encodeURIComponent(version)}`,
          )
        }
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
