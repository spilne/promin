// ---------------------------------------------------------------------------
// Lightweight markdown renderer for chat bubbles.
//
// Why not just `marked.parse()` + dangerouslySetInnerHTML?
//   The LLM output is not trusted (the model can be prompted to emit
//   <script> tags). We'd need DOMPurify on top, ~50KB total. Instead we
//   tokenize with `marked.lexer()` and walk the tokens emitting Preact
//   JSX — Preact escapes string children automatically, so the XSS
//   surface is just URL schemes (allowlisted to http/https/mailto).
//
// Covers what assistant replies actually emit: paragraphs, headings,
// fenced + inline code, bold/italic, links, lists, blockquotes, hr.
// Tables / images / footnotes are rendered as a fallback `<pre>` of the
// raw markdown (rare in chat, costs ~0 to add later if needed).
// ---------------------------------------------------------------------------

import type { ComponentChildren } from "preact";
import { lexer } from "marked";
import type { Tokens, Token } from "marked";

interface MarkdownProps {
  text: string;
  /** Extra Tailwind classes on the wrapper. */
  className?: string;
}

export function Markdown({ text, className = "" }: MarkdownProps) {
  // Tokenize once per render. Marked's lexer is fast enough that
  // memoizing here would just add complexity.
  const tokens: Token[] = lexer(text, { gfm: true, breaks: true });
  return <div class={`markdown-body ${className}`}>{tokens.map((t, i) => renderBlock(t, i))}</div>;
}

function renderBlock(token: Token, key: number): ComponentChildren {
  switch (token.type) {
    case "space":
      return null;
    case "paragraph":
      return (
        <p key={key} class="my-1.5 leading-relaxed">
          {renderInline((token as Tokens.Paragraph).tokens)}
        </p>
      );
    case "heading": {
      const t = token as Tokens.Heading;
      const Tag = `h${Math.min(6, Math.max(1, t.depth))}` as unknown as "h1";
      const sizeClass =
        t.depth <= 2
          ? "text-lg font-semibold mt-2 mb-1"
          : t.depth === 3
            ? "text-base font-semibold mt-2 mb-1"
            : "text-sm font-semibold mt-1.5 mb-1";
      return (
        <Tag key={key} class={sizeClass}>
          {renderInline(t.tokens)}
        </Tag>
      );
    }
    case "code": {
      const t = token as Tokens.Code;
      return (
        <pre
          key={key}
          class="bg-base-300 rounded p-2 my-1.5 text-xs overflow-x-auto whitespace-pre"
        >
          <code class={t.lang ? `language-${t.lang}` : ""}>{t.text}</code>
        </pre>
      );
    }
    case "blockquote": {
      const t = token as Tokens.Blockquote;
      return (
        <blockquote
          key={key}
          class="border-l-2 border-base-content/30 pl-3 my-1.5 text-base-content/80 italic"
        >
          {t.tokens.map((sub, i) => renderBlock(sub, i))}
        </blockquote>
      );
    }
    case "list": {
      const t = token as Tokens.List;
      const Tag = t.ordered ? "ol" : "ul";
      const listClass = t.ordered ? "list-decimal ml-5 my-1.5" : "list-disc ml-5 my-1.5";
      return (
        <Tag key={key} class={listClass} start={t.ordered && t.start ? Number(t.start) : undefined}>
          {t.items.map((item, i) => (
            <li key={i} class="my-0.5 leading-relaxed">
              {item.tokens.map((sub, j) => renderBlock(sub, j))}
            </li>
          ))}
        </Tag>
      );
    }
    case "hr":
      return <hr key={key} class="my-2 border-base-content/20" />;
    case "html":
      // Treat as plain text — never feed unsanitized HTML to the DOM.
      return <span key={key}>{(token as Tokens.HTML).text}</span>;
    case "text": {
      // Top-level loose text (rare — usually wrapped in paragraph).
      const t = token as Tokens.Text & { tokens?: Token[] };
      return <span key={key}>{t.tokens ? renderInline(t.tokens) : t.text}</span>;
    }
    default:
      // Fallback: render the raw markdown so we never silently swallow
      // content we don't know how to format.
      return (
        <pre
          key={key}
          class="bg-base-200 rounded p-2 my-1.5 text-xs whitespace-pre-wrap break-words"
        >
          {(token as { raw?: string }).raw ?? ""}
        </pre>
      );
  }
}

function renderInline(tokens: Token[] | undefined): ComponentChildren {
  if (!tokens) return null;
  return tokens.map((t, i) => renderInlineToken(t, i));
}

function renderInlineToken(token: Token, key: number): ComponentChildren {
  switch (token.type) {
    case "text": {
      const t = token as Tokens.Text & { tokens?: Token[] };
      return t.tokens ? <span key={key}>{renderInline(t.tokens)}</span> : t.text;
    }
    case "strong":
      return <strong key={key}>{renderInline((token as Tokens.Strong).tokens)}</strong>;
    case "em":
      return <em key={key}>{renderInline((token as Tokens.Em).tokens)}</em>;
    case "codespan":
      return (
        <code key={key} class="bg-base-300 rounded px-1 py-0.5 text-xs font-mono">
          {(token as Tokens.Codespan).text}
        </code>
      );
    case "del":
      return <del key={key}>{renderInline((token as Tokens.Del).tokens)}</del>;
    case "link": {
      const t = token as Tokens.Link;
      const href = sanitizeUrl(t.href);
      if (!href) {
        // Unsafe URL — render the link text as plain text rather than swallow.
        return <span key={key}>{renderInline(t.tokens)}</span>;
      }
      return (
        <a
          key={key}
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          class="link link-primary"
        >
          {renderInline(t.tokens)}
        </a>
      );
    }
    case "br":
      return <br key={key} />;
    case "image": {
      // Don't load arbitrary images — render alt text instead.
      const t = token as Tokens.Image;
      return (
        <span key={key} class="text-base-content/60 italic">
          [image: {t.text || t.href}]
        </span>
      );
    }
    case "html":
      return <span key={key}>{(token as Tokens.HTML).text}</span>;
    case "escape":
      return (token as Tokens.Escape).text;
    default:
      return (token as { raw?: string }).raw ?? "";
  }
}

const SAFE_URL_SCHEMES = ["http:", "https:", "mailto:", "tel:"];

function sanitizeUrl(raw: string): string | null {
  const trimmed = raw.trim();
  // Relative URLs and # anchors are safe.
  if (trimmed.startsWith("#") || trimmed.startsWith("/")) return trimmed;
  try {
    const url = new URL(trimmed);
    return SAFE_URL_SCHEMES.includes(url.protocol) ? trimmed : null;
  } catch {
    // Not parseable as an absolute URL — treat as relative.
    return trimmed.includes(":") ? null : trimmed;
  }
}
