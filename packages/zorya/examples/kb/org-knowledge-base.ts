// Small in-memory "org" knowledge base for the knowledge-bot demo.
// Five short markdown-flavoured docs covering the kinds of things a
// real engineering org actually documents — enough surface area for the
// agent to demonstrate search + cite + multi-doc synthesis through the
// managed Knowledge Bases service without turning the demo into a CMS.

interface KnowledgeDoc {
  title: string;
  tags: string[];
  body: string;
}

export const ORG_KNOWLEDGE_BASE: ReadonlyMap<string, KnowledgeDoc> = new Map<string, KnowledgeDoc>([
  [
    "engineering-handbook",
    {
      title: "Engineering Handbook",
      tags: ["culture", "engineering", "principles"],
      body: `# Engineering Handbook

## Principles
- **Boring is better.** Reach for proven tools first. Novelty must earn its place.
- **Small PRs.** Aim for <400 lines. If a change is bigger, split it.
- **Tests are part of the change.** "I'll write tests later" never happens.
- **Type errors block merge.** No \`any\`, no \`@ts-ignore\` without a comment explaining why.

## Stack
- Runtime: Bun
- Language: TypeScript (strict)
- Framework: Effect for FP/concurrency, Zod for validation
- Linting: oxlint, formatting: oxfmt
- Monorepo: Nx

## Working hours
Async-first. Core overlap is 14:00–17:00 UTC. Don't expect synchronous replies outside that window.`,
    },
  ],
  [
    "oncall-runbook",
    {
      title: "On-Call Runbook",
      tags: ["oncall", "incident", "sre", "runbook"],
      body: `# On-Call Runbook

## Rotation
- Weekly rotation, Monday 10:00 UTC handoff.
- Primary + secondary. Secondary covers if primary doesn't ack within 10 min.
- Schedule lives in PagerDuty; check the #oncall channel topic for the current pair.

## SLOs
- **API availability**: 99.9% (43.8 min/month error budget)
- **API p95 latency**: 250ms
- **Workflow scheduler tick lag**: <5s p99

## When you get paged
1. Acknowledge in PagerDuty within 10 min.
2. Open an incident channel: \`#inc-YYYY-MM-DD-short-title\`.
3. Post a status: investigating / mitigating / resolved.
4. If customer-facing > 5 min, post to status page (status.spilne.io).

## Escalation
- Level 1: on-call primary
- Level 2: on-call secondary + team lead
- Level 3: VP Eng (only for SEV-1: full outage, data loss, security)

## Common pages
- **scheduler-tick-lag-high**: usually a slow query in the step queue. Check pg_stat_statements.
- **redis-memory-pressure**: cache eviction storm. Bump the redis instance class temporarily.
- **anthropic-rate-limit**: spike in agent traffic. Check the agent gateway dashboard for runaway loops.

## Postmortem
Required for any SEV-1 or SEV-2 within 5 business days. Template lives at \`/runbook/postmortem-template.md\`.`,
    },
  ],
  [
    "security-policy",
    {
      title: "Security Policy",
      tags: ["security", "secrets", "auth", "compliance"],
      body: `# Security Policy

## Secrets
- **Never** commit secrets to git. Pre-commit hook scans for high-entropy strings.
- Production secrets live in 1Password ("Engineering — Prod" vault) and are rotated quarterly.
- Local dev: \`.env.local\` (gitignored). Pull from 1Password CLI: \`op inject\`.
- API keys for third-party services (Anthropic, OpenAI, etc.) are per-environment, never shared between dev/staging/prod.

## Auth
- Internal services: mTLS between pods, plus a JWT signed by the auth service.
- External APIs: bearer tokens with scoped permissions, expiry max 24h.
- Customer auth: OAuth 2.0 with PKCE; refresh tokens rotate on use.
- MFA mandatory for all employees. Hardware key (YubiKey) required for prod access.

## Vulnerability management
- Dependabot enabled on every repo. Auto-merge for patch versions on green CI.
- Critical CVEs (CVSS >= 9): patched within 48h.
- High CVEs (CVSS 7–8.9): patched within 7 days.
- Run \`bun audit\` weekly; report findings in #security.

## Incident reporting
Suspected breach: page security on-call immediately, do NOT discuss in public channels. Use the encrypted #sec-private channel (request access from the CISO).`,
    },
  ],
  [
    "deploy-pipeline",
    {
      title: "Deploy Pipeline",
      tags: ["deploy", "ci", "release", "rollback"],
      body: `# Deploy Pipeline

## Pipeline stages
1. **PR opens** → CI runs typecheck + lint + tests on every push.
2. **Merge to main** → CI builds container images, pushes to registry, triggers staging deploy.
3. **Staging soak** → 30 min minimum. Smoke tests + canary metrics must stay green.
4. **Prod deploy** → manual approval required (any of: SRE on-call, team lead, VP Eng).
5. **Prod rollout** → blue/green; 5 min canary at 5% traffic, then 50%, then 100%.

## Rollback
- One-click rollback in the deploy dashboard restores the previous image tag.
- Database migrations are forward-only by convention. If a migration is the problem, rollback the app to a version that's compatible with the new schema, then plan the schema fix forward.
- Feature flags are the preferred rollback mechanism for product changes — flip the flag, no deploy needed.

## Release cadence
- Default: continuous (multiple deploys/day per service).
- Mobile: weekly release branch, cut Thursday, ships Monday.
- Freeze window: last 3 business days of each quarter (auditors don't like prod changes during close).

## Emergency hotfix
1. Branch from the latest prod tag (not main).
2. Apply minimal fix.
3. CI bypass requires SEV-1 incident + VP Eng sign-off.
4. Cherry-pick to main after the fact.`,
    },
  ],
  [
    "rfc-001-streaming",
    {
      title: "RFC-001: SSE over WebSockets for agent streaming",
      tags: ["rfc", "streaming", "sse", "websockets", "architecture"],
      body: `# RFC-001: SSE over WebSockets for agent streaming

**Status**: Accepted (2026-Q1)
**Author**: Platform team
**Reviewers**: SRE, Frontend, API

## Decision
Use Server-Sent Events (SSE) for streaming agent responses to the dashboard. Reject WebSockets.

## Context
The dashboard's chat UI needs to stream tokens from in-flight agent turns. Two viable transports: SSE and WebSockets.

## Trade-offs

### SSE — chosen
- Pros: HTTP/1.1 compatible, plays well with existing reverse-proxy + auth middleware, one-way (server → client) which matches our needs, automatic reconnection in EventSource API.
- Cons: text-only (binary requires base64 wrapping), max 6 connections per origin in HTTP/1.1 (HTTP/2 fixes this), no built-in client → server messaging (we use a separate POST for user input).

### WebSockets — rejected
- Pros: full duplex, binary-native, lower per-message overhead.
- Cons: full duplex is a complication we don't need, our auth/proxy stack needs custom WebSocket support, harder to debug, harder to scale (sticky sessions).

## Migration
n/a — greenfield.

## Open questions
- HTTP/2 multiplexing should make the 6-connection limit a non-issue, but we should verify under load before declaring victory.
- If we ever need bidi (e.g. user can interrupt mid-token), revisit.`,
    },
  ],
  [
    "code-review-guide",
    {
      title: "Code Review Guide",
      tags: ["review", "engineering", "process"],
      body: `# Code Review Guide

## Author responsibilities
- PR title is a one-liner; description explains the why, not the what.
- Self-review before requesting reviewers.
- Keep PRs <400 LOC of diff. Split if larger.
- Tag reviewers explicitly; don't rely on team auto-assign for non-trivial changes.

## Reviewer responsibilities
- Respond within 1 business day. If you can't, say so.
- Distinguish blocking (must fix) from non-blocking (nice-to-have, future cleanup).
- Approve only when you'd be comfortable being paged about the change.
- Push back on scope creep — a bug fix shouldn't carry a refactor.

## What to look for (in order)
1. Does it work? Read the test cases first.
2. Is it correct under failure? Network errors, retries, partial state.
3. Is it observable? Logs, metrics, traces at the right level.
4. Is it idiomatic? Matches existing patterns in this codebase.
5. Is it readable? Could a new hire understand the WHY in 6 months?

## Don't
- Bikeshed style. We have oxlint + oxfmt for that.
- Block on personal preference. Cite a written convention or accept the author's choice.
- LGTM without reading. The "Approve" button is a signature.`,
    },
  ],
]);
