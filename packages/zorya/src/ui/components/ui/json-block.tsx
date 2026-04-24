import { useState } from "preact/hooks";

interface JsonBlockProps {
  value: unknown;
  /** Tailwind max-height class. Default "max-h-60". */
  maxH?: string;
  /** When true, renders with subtle error-themed background. */
  variant?: "default" | "error";
  /** When true, shows a copy button in the top-right. */
  copyable?: boolean;
}

export function JsonBlock({
  value,
  maxH = "max-h-60",
  variant = "default",
  copyable = true,
}: JsonBlockProps) {
  const [copied, setCopied] = useState(false);
  const text = formatJson(value);
  const bg = variant === "error" ? "bg-error/10 text-error" : "bg-base-200";

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // clipboard write can fail in some permission contexts — swallow
    }
  };

  return (
    <div class="relative group">
      <pre
        class={`${bg} p-2 pr-10 rounded text-sm overflow-x-auto ${maxH} whitespace-pre-wrap break-words font-mono leading-relaxed`}
      >
        {text}
      </pre>
      {copyable && (
        <button
          class="absolute top-1 right-1 btn btn-xs btn-ghost opacity-0 group-hover:opacity-100 transition-opacity"
          onClick={copy}
          title="Copy"
        >
          {copied ? "✓" : "⎘"}
        </button>
      )}
    </div>
  );
}

function formatJson(v: unknown): string {
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v);
  }
}
