# ADR 012: Bounded candidate Silver validation authority

**Status:** Accepted
**Date:** 2026-11-06

## Context

Milestone 6 must validate a real multi-symbol pipeline through Gold without adding
an unreviewed Silver publication to `config/silver_publications.toml`. The existing
Gold boundary intentionally accepts only `status = "approved"`; filesystem presence
or a complete manifest is not authority. Treating the new run as generally approved
would misrepresent its review state, while bypassing manifest and registry checks
would weaken the established contract.

## Decision

Add a separate, explicit `authorized_bounded_validation` input mode. It uses the
same fail-closed dataset/run selection, manifest SHA-256, source/request fingerprint,
artifact inventory, size and checksum verification as approved resolution. It is
selected only when a Gold request explicitly declares `silver_input_authority` and
the separately located registry has the matching status. The default remains
`approved`, and `resolve_approved_silver()` continues to reject validation-only
entries.

The Milestone 6 registry is under `config/milestone6/`; it does not modify the
approved Silver registry. Its decision is limited to the user-authorized bounded
pipeline validation. Resulting Gold runs remain unregistered review candidates.
Expected-Series configuration also records the exact acquisition-scope symbols so
the 25 intentionally unrequested cohort symbols are not interpreted as provider
coverage failures.

## Consequences

Candidate data can exercise the production transformations and immutable publishers
without acquiring general publication authority. Every output manifest records the
input authority mode; expected-series manifests additionally record acquisition
scope. No newest-directory discovery is introduced. Architecture review accepted
this boundary for bounded candidate validation. This acceptance approves the
implementation milestone, not any candidate dataset. The immutable evidence remains
outside approved registries until a separate publication decision follows the
documented migration procedure.
