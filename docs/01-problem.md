# The problem

This project models a subscription streaming service: a Netflix-shaped business, not a real
company. Subscribers sign up, pick a plan, watch movies and series across devices, sometimes
change plan or pause, and sometimes cancel. The business runs on the recurring-revenue mechanics
common to any subscription product: acquisition has a cost, retention determines whether that
cost pays off, and the content catalog is the reason anyone stays.

This document describes the business domain and the questions the data model exists to answer.
How those answers get produced, the schema design, the SCD mechanics, the pipeline, is the
subject of the documents that follow (`04-model.md`, `05-implementation.md`). Here the concern is
only what the business needs to know and why.

## The domain, concretely

A subscriber's path through the product looks like this:

1. **Signup.** A visitor starts a signup attempt: registers, verifies email, adds a payment
   method, selects a plan, and (ideally) starts their first stream. Any step can be where they
   drop off, and not every step happens in the same order for every attempt.
2. **Subscription.** Once registered, a subscriber holds a plan tier (basic, standard, premium,
   premium_plus) and a status (trial, active, paused, churned). Both can change over the life of
   the account, and the business cares about exactly when they changed, not just the current
   state.
3. **Engagement.** Subscribers watch titles (movies and series) on devices (tv, mobile, tablet,
   web, console). Each viewing is a session with a start and end time, a completion percentage,
   and playback quality. Titles carry genre associations with weights, so a title can be
   "70% drama, 30% thriller" rather than belonging to one bucket.
4. **Billing.** The subscription generates recurring charges, and sometimes refunds, credits, or
   prorations, against a payment method. Charges can be retried, credited back, or adjusted
   mid-cycle.
5. **Watchlisting.** Subscribers add titles they intend to watch later. This is pure intent
   signal: no measure, just an event.
6. **Churn.** At some point a subscriber's status may become churned. The business wants to know
   who, when, and whether the pattern correlates with plan tier, acquisition channel, or content
   engagement.

None of this is exotic. It is the same shape of business as any subscription product with a
funnel, a recurring charge, and a content or feature-usage signal to explain retention. That
familiarity is deliberate: the domain should not be the hard part of reading this project, the
data engineering should be.

## The questions the business needs answered

A subscription streaming business runs on five recurring questions. Each one is tied here to the
specific fact table built to answer it.

**Signup funnel conversion and drop-off.** Of everyone who starts a signup attempt, what fraction
registers, verifies email, adds a payment method, selects a plan, and starts their first stream?
Where in that sequence do attempts stall, and how long does each step take? `fct_signup_funnel`
is an accumulating snapshot, one row per attempt, updated in place as it crosses each milestone,
built specifically to answer "conversion rate at each stage" and "time between stages" without
forcing an analyst to reconstruct funnel state from a raw event log.

**Monthly recurring revenue and its drivers.** What is MRR today, how did it get there, and what
would it become if a cohort of trial subscribers converts or a tier of paying subscribers churns?
`fct_daily_subscription_snapshot` holds one row per subscriber per calendar day, with plan,
status, and MRR contribution as of end of day. This is what lets MRR be sliced by plan tier,
computed as a trend over time, and reconciled against subscriber counts, without re-deriving
subscription state from change events on every query.

**Subscriber churn and retention by cohort.** What fraction of subscribers who signed up in a
given month are still active six months later? Does a paused status predict eventual churn?
Does tenure correlate with plan tier? The same `fct_daily_subscription_snapshot`, joined to
`dim_subscriber` for signup date and acquisition channel, answers cohort retention questions: a
daily snapshot is what makes "still active N days after signup" a straightforward filter rather
than a point-in-time reconstruction.

**Content and genre engagement.** What do subscribers actually watch, how much of it do they
finish, and which genres or titles are driving retention versus which are dead weight in the
catalog? `fct_playback_events` is the one row per completed session record: title, subscriber,
device, watch duration, and completion percentage. Genre-level rollups go through
`bridge_title_genre`, which exists because a title is not one genre: it is a weighted mix (a
title might be 70% drama, 30% thriller), and a genre engagement query that joined naively to a
single "primary genre" column would either double-count exposure across genres or discard the
minority-genre signal entirely. The bridge's `allocation_weight` sums to exactly 1.0 per title,
so a weighted engagement query is arithmetically correct without a separate normalization step.

**Billing health.** How much is actually being charged versus refunded or credited, is the
retry rate on failed payments rising, and which payment methods carry the most friction?
`fct_billing_transactions` holds one row per billing ledger event (charge, refund, credit,
proration) with a signed amount, so `sum(amount_usd)` is net revenue with no conditional logic,
and each row can be attributed to a payment method combination for friction analysis.

Watchlist adds (`fct_watchlist_adds`) support a sixth, smaller question: intent signal ahead of
consumption, useful for recommendation and catalog-acquisition questions, though it carries no
measures of its own, only the event.

## Why a dimensional model, not the OLTP shape

A real streaming service's operational systems, subscriber accounts, the billing ledger, the
playback telemetry pipeline, are OLTP-shaped: normalized, optimized for a single record read or
write, and organized around the transaction that created each row rather than the question an
analyst will later ask of it. That shape is right for the system that has to process one
subscriber's payment or start one playback session quickly and safely.

It is the wrong shape for the questions above. Every one of them is a scan-and-aggregate query
over a large population, filtered and grouped by business attributes (plan tier, cohort month,
genre, acquisition channel), not a lookup by primary key. Answering "MRR by plan tier over the
last twelve months" against a normalized OLTP schema means joining subscribers to plan history to
billing events to a calendar, at query time, on every dashboard refresh, with the join topology
worked out fresh by whoever wrote the query. A dimensional model does that decomposition once, at
load time, and hands the analyst a small number of wide fact tables with pre-resolved keys into
plainly labeled dimensions.

The specific shape chosen here is a star schema with conformed dimensions (`dim_subscriber`,
`dim_title`, `dim_plan`, `dim_device`, `dim_date` and its role-playing variants,
`dim_payment_method`) shared across the fact tables in the bus matrix. That conformance is what
lets a question like churn-by-genre span two facts (`fct_daily_subscription_snapshot` and
`fct_playback_events`) through `dim_subscriber` without a bespoke join path invented per query.
It also lets the model answer not just "what is true now" but "what was true on the day of this
event," because the subscriber and title dimensions carry history (type 2 slowly changing
dimensions) rather than only current state, so a playback session or a billing charge resolves
against the subscriber's plan tier and status as they actually were at that moment, not as they
are today. That distinction, current state versus state-as-of, is exactly what a churn or funnel
analysis needs and an OLTP system, which typically only stores current state, cannot give
directly.
