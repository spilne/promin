import Link from 'next/link';

export default function HomePage() {
  return (
    <main
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        minHeight: '100vh',
        gap: '1rem',
        textAlign: 'center',
        padding: '2rem',
      }}
    >
      <h1 style={{ fontSize: '3rem', fontWeight: 700 }}>Promin</h1>
      <p style={{ fontSize: '1.25rem', opacity: 0.7, maxWidth: '40rem' }}>
        TypeScript toolkit for resilient async operations, durable workflows,
        stream processing, and analytics.
      </p>
      <Link
        href="/docs"
        style={{
          padding: '0.75rem 2rem',
          borderRadius: '0.5rem',
          background: 'var(--fd-primary)',
          color: 'var(--fd-primary-foreground)',
          textDecoration: 'none',
          fontWeight: 500,
        }}
      >
        Read the Docs
      </Link>
    </main>
  );
}
