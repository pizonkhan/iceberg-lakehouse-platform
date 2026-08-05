# ADR-003: Local-only deployment, no cloud path

## Status

Accepted, 2026-08-03.

## Context

The project's budget was a literal $0, and "never provision a paid resource" was a stated
constraint, not just a preference. Before ruling cloud out, the free tiers of the plausible object
storage options were researched and their real overage behavior checked, not just their advertised
free-tier limits (full findings in `.notes/cloud-tiers.md`).

The two storage options that would actually fit this project's needs (S3-compatible object storage
for the Iceberg warehouse) were Cloudflare R2 and AWS S3:

- **Cloudflare R2**: 10 GB-month storage, 1M Class A / 10M Class B ops free, zero egress. Requires
  a card on file to enable R2 at all. Overage auto-bills pay-as-you-go with no hard stop.
- **AWS S3**: accounts created after 2025-07-15 no longer get a 12-month free tier; they get a
  one-time $100 to $200 credit pool shared across all AWS services, valid 6 to 12 months. Requires
  a card at signup. Overage auto-charges standard rates once the credit pool is exhausted, no hard
  stop.

Neither offers a true guarantee against a charge landing on the card on file. Other services
researched at the same time (Neon, Supabase) do offer a genuine hard stop on overage, but neither
is a fit for an S3-compatible object store, which is what an Iceberg warehouse actually needs.

## Decision

Deploy nothing to the cloud. Run the entire stack locally in Docker (MinIO, Nessie, Postgres,
Trino). Cloud free-tier research stays documented in `.notes/cloud-tiers.md` for reference, not for
use, and Terraform was dropped from scope entirely since there are no cloud resources for it to
manage.

Treating R2's and AWS S3's auto-billing risk as disqualifying, rather than as an acceptable small
risk, was the deciding move: staying 100% local satisfies "never provision a paid resource"
literally, not just nominally.

## Alternatives Considered

- **Cloudflare R2 with a spend alert and a low-limit card.** Genuinely free at this project's
  storage scale and the option this project would reach for first if a cloud path were ever added
  (per the closing note in `.notes/cloud-tiers.md`). Rejected for the deployed path because a spend
  alert is a mitigation, not a guarantee: it depends on the alert firing and someone acting on it
  before a charge posts, which is not the same thing as a platform-enforced hard stop.
- **AWS S3 on the post-2025 credit-pool model.** Rejected outright as the highest-risk option
  checked: no hard stop, and the credit pool itself is time-limited and shared across every AWS
  service the account might ever use, not scoped to this project.
- **A hybrid path** (local dev, optional cloud deployment for demonstration purposes only). Not
  pursued: once R2 and AWS were both found to carry real billing risk, there was no object-storage
  option left that met the $0 guarantee, so a "cloud if convenient" branch was never built.

## Consequences

- Nothing in this project has ever been exercised against real network latency, IAM, or a
  multi-tenant object store; every finding about performance, memory ceilings, and failure modes
  (Trino's 1.5GB per-query cap, the various OOM crashes during full-scale builds) is specific to a
  single Docker host and may not transfer directly to a cloud deployment.
- The Docker Compose stack is the only infrastructure-as-code this project has. If a cloud path is
  ever added, Terraform (or equivalent) needs to be introduced from scratch, not resumed from an
  existing partial setup.
- Nessie's S3 endpoint is hardcoded to the compose-network address (`http://minio:9000`), which
  ingestion works around with a host-side DNS patch (`ingestion/network.py`) rather than an
  environment-driven endpoint. That workaround is itself a direct consequence of staying local; a
  real cloud endpoint would not need it, but revisiting it was out of scope for this decision.
- If this constraint is ever revisited, the research already done narrows the field: Neon or
  Supabase for anything relational (both have genuine hard-stop overage behavior), R2 only behind a
  spend alert and a low-limit card, AWS S3 avoided.
