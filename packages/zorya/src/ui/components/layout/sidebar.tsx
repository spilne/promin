import { prompt } from "../../lib/dialogs.ts";
import { NamespaceSwitcher } from "./namespace-switcher.tsx";

interface SidebarProps {
  route: string;
  onNavigate: (path: string) => void;
}

interface NavItem {
  path: string;
  label: string;
  icon: string;
  disabled?: boolean;
}

const SECTIONS: Array<{ title?: string; items: NavItem[] }> = [
  {
    title: "Workflows",
    items: [
      { path: "/", label: "Runs", icon: "R" },
      { path: "/workflows", label: "Workflows", icon: "W" },
      { path: "/schedules", label: "Schedules", icon: "S" },
      { path: "/signals", label: "Signals", icon: "Q" },
      { path: "/workers", label: "Workers", icon: "N" },
    ],
  },
  {
    title: "Agents",
    items: [
      { path: "/agents", label: "Registry", icon: "A" },
      { path: "/roles", label: "Roles", icon: "P" },
      { path: "/instances", label: "Instances", icon: "I" },
      { path: "/tools", label: "Tools", icon: "T" },
      { path: "/skills", label: "Skills", icon: "K" },
      { path: "/fragments", label: "Fragments", icon: "F" },
      { path: "/dags", label: "DAGs", icon: "D" },
    ],
  },
  {
    title: "Settings",
    items: [{ path: "/secrets", label: "Secrets", icon: "V" }],
  },
  {
    title: "Coming soon",
    items: [{ path: "/alerts", label: "Alerts", icon: "!", disabled: true }],
  },
];

export function Sidebar({ route, onNavigate }: SidebarProps) {
  return (
    <aside class="sticky top-0 h-screen w-60 shrink-0 border-r border-base-content/10 bg-base-300/95 backdrop-blur flex flex-col">
      <div class="px-4 py-4 flex items-center gap-3 border-b border-base-content/10">
        <span class="grid h-8 w-8 place-items-center rounded bg-primary/15 text-primary font-semibold">
          Z
        </span>
        <div class="min-w-0">
          <div class="font-semibold tracking-tight text-lg leading-tight">Zorya</div>
          <div class="text-[11px] text-base-content/45 leading-tight">workflow control plane</div>
        </div>
      </div>

      <NamespaceSwitcher />

      <nav class="flex-1 py-3 space-y-4 overflow-y-auto">
        {SECTIONS.map((section) => (
          <div>
            {section.title && (
              <div class="px-4 mb-1 text-[10px] uppercase tracking-[0.12em] text-base-content/40">
                {section.title}
              </div>
            )}
            <ul class="px-2 space-y-1">
              {section.items.map((item) => (
                <li>
                  {item.disabled ? (
                    <span class="opacity-40 cursor-not-allowed flex items-center gap-2 rounded px-2 py-2 text-sm">
                      <NavIcon label={item.icon} active={false} />
                      <span>{item.label}</span>
                    </span>
                  ) : (
                    <a
                      href={item.path}
                      onClick={(e) => {
                        e.preventDefault();
                        onNavigate(item.path);
                      }}
                      class={`group flex items-center gap-2 rounded px-2 py-2 text-sm hover:bg-base-100/70 ${
                        isActive(route, item.path)
                          ? "bg-primary/12 text-primary border border-primary/20"
                          : "border border-transparent text-base-content/78"
                      }`}
                    >
                      <NavIcon label={item.icon} active={isActive(route, item.path)} />
                      <span>{item.label}</span>
                    </a>
                  )}
                </li>
              ))}
            </ul>
          </div>
        ))}
      </nav>

      <div class="px-2 py-3 border-t border-base-content/10">
        <ApiKeyButton />
      </div>
    </aside>
  );
}

function NavIcon({ label, active }: { label: string; active: boolean }) {
  return (
    <span
      class={`grid h-6 w-6 shrink-0 place-items-center rounded text-[11px] font-semibold ${
        active ? "bg-primary text-primary-content" : "bg-base-100 text-base-content/55"
      }`}
    >
      {label}
    </span>
  );
}

function isActive(route: string, path: string): boolean {
  if (path === "/") return route === "/" || route.startsWith("/runs");
  return route.startsWith(path);
}

function ApiKeyButton() {
  const setKey = async () => {
    const current = localStorage.getItem("zorya_api_key") ?? "";
    const next = await prompt({
      title: "API key",
      label: "Bearer token",
      initial: current,
      placeholder: "leave blank to clear",
    });
    if (next === null) return;
    if (next) localStorage.setItem("zorya_api_key", next);
    else localStorage.removeItem("zorya_api_key");
    location.reload();
  };
  const hasKey = !!localStorage.getItem("zorya_api_key");
  return (
    <button class="btn btn-ghost btn-sm w-full justify-start gap-2" onClick={setKey}>
      <span class={`w-2 h-2 rounded-full ${hasKey ? "bg-success" : "bg-base-content/30"}`} />
      <span class="text-sm">API key</span>
      <span class="ml-auto text-[10px] uppercase text-base-content/40">
        {hasKey ? "set" : "none"}
      </span>
    </button>
  );
}
