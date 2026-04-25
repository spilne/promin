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
    items: [
      { path: "/", label: "Runs", icon: "▦" },
      { path: "/workflows", label: "Workflows", icon: "◈" },
      { path: "/schedules", label: "Schedules", icon: "⏱" },
      { path: "/agents", label: "Agents", icon: "✦" },
      { path: "/workers", label: "Workers", icon: "◉" },
    ],
  },
  {
    title: "Coming soon",
    items: [
      { path: "/deployments", label: "Deployments", icon: "☁", disabled: true },
      { path: "/alerts", label: "Alerts", icon: "⚠", disabled: true },
    ],
  },
];

export function Sidebar({ route, onNavigate }: SidebarProps) {
  return (
    <aside class="w-56 shrink-0 border-r border-base-content/10 bg-base-300 flex flex-col">
      <div class="px-4 py-4 flex items-center gap-2 border-b border-base-content/10">
        <span class="text-primary text-xl">⚡</span>
        <span class="font-semibold tracking-tight text-lg">Zorya</span>
      </div>

      <NamespaceSwitcher />

      <nav class="flex-1 py-3 space-y-4 overflow-y-auto">
        {SECTIONS.map((section) => (
          <div>
            {section.title && (
              <div class="px-4 mb-1 text-[10px] uppercase tracking-wider text-base-content/40">
                {section.title}
              </div>
            )}
            <ul class="menu menu-sm px-2">
              {section.items.map((item) => (
                <li>
                  {item.disabled ? (
                    <span class="opacity-40 cursor-not-allowed flex items-center gap-2">
                      <span class="w-4 text-center">{item.icon}</span>
                      <span>{item.label}</span>
                    </span>
                  ) : (
                    <a
                      href={item.path}
                      onClick={(e) => {
                        e.preventDefault();
                        onNavigate(item.path);
                      }}
                      class={`flex items-center gap-2 ${isActive(route, item.path) ? "active" : ""}`}
                    >
                      <span class="w-4 text-center">{item.icon}</span>
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
    </button>
  );
}
