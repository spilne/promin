interface NavbarProps {
  route: string;
  onNavigate: (path: string) => void;
}

export function Navbar({ route, onNavigate }: NavbarProps) {
  const tabs: Array<{ path: string; label: string }> = [
    { path: "/", label: "Runs" },
    { path: "/workers", label: "Workers" },
  ];

  return (
    <div class="navbar bg-base-300 border-b border-base-content/10 shadow-sm sticky top-0 z-10">
      <div class="flex-1 gap-2">
        <a
          href="/"
          onClick={(e) => {
            e.preventDefault();
            onNavigate("/");
          }}
          class="btn btn-ghost text-xl gap-2 normal-case"
        >
          <span class="text-primary">⚡</span>
          <span class="font-semibold tracking-tight">Zorya</span>
        </a>
        <div class="tabs tabs-boxed bg-base-200 ml-4">
          {tabs.map((t) => (
            <a
              href={t.path}
              onClick={(e) => {
                e.preventDefault();
                onNavigate(t.path);
              }}
              class={`tab ${isActive(route, t.path) ? "tab-active" : ""}`}
            >
              {t.label}
            </a>
          ))}
        </div>
      </div>
      <div class="flex-none">
        <ApiKeyButton />
      </div>
    </div>
  );
}

function isActive(route: string, path: string): boolean {
  if (path === "/") return route === "/" || route.startsWith("/runs");
  return route.startsWith(path);
}

function ApiKeyButton() {
  const setKey = () => {
    const current = localStorage.getItem("zorya_api_key") ?? "";
    const next = prompt("API key (blank to clear)", current);
    if (next === null) return;
    if (next) localStorage.setItem("zorya_api_key", next);
    else localStorage.removeItem("zorya_api_key");
    location.reload();
  };
  const hasKey = !!localStorage.getItem("zorya_api_key");
  return (
    <button class="btn btn-ghost btn-sm gap-2" onClick={setKey}>
      <span class={`w-2 h-2 rounded-full ${hasKey ? "bg-success" : "bg-base-content/30"}`} />
      API key
    </button>
  );
}
