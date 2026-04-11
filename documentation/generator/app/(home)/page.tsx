import Link from "next/link";
import { PipelineAnimation } from "./pipeline-animation";

export default function HomePage() {
  return (
    <main
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        minHeight: "100vh",
        gap: "1.5rem",
        textAlign: "center",
        padding: "2rem",
      }}
    >
      <PipelineAnimation />

      <h1 style={{ fontSize: "3rem", fontWeight: 700, marginTop: "0.5rem" }}>
        Promin
      </h1>
      <p
        style={{
          fontSize: "1.25rem",
          opacity: 0.7,
          maxWidth: "40rem",
          lineHeight: 1.6,
        }}
      >
        TypeScript toolkit for resilient async operations, durable workflows,
        stream processing, and analytics.
      </p>

      <div style={{ display: "flex", gap: "1rem", marginTop: "0.5rem" }}>
        <Link
          href="/docs"
          style={{
            padding: "0.75rem 2rem",
            borderRadius: "0.5rem",
            background: "var(--fd-primary)",
            color: "var(--fd-primary-foreground)",
            textDecoration: "none",
            fontWeight: 500,
          }}
        >
          Read the Docs
        </Link>
        <a
          href="https://github.com/spilne/promin"
          style={{
            padding: "0.75rem 2rem",
            borderRadius: "0.5rem",
            border: "1px solid var(--fd-border)",
            color: "var(--fd-foreground)",
            textDecoration: "none",
            fontWeight: 500,
          }}
        >
          GitHub
        </a>
      </div>
    </main>
  );
}
