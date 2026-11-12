# ADR 001: Independent modular Python project

Date: 2026-11-02. Status: accepted for Phase 0.

Context: the reference audit reports valuable historical evidence but incompatible
temporal/correction policies and operational coupling. The task requires an
independent greenfield foundation.

Decision: standard-library domain, pure analytics specifications, thin runners,
external adapters introduced only with concrete uses. Create only packages with
working code. Preserve the supplied audit byte-for-byte; no source/data import.
Use a modular monolith before considering services.

Consequences: smaller dependency surface and meaningful unit tests now; Spark
expressions may require distinct implementations later, governed by shared golden
fixtures and one versioned specification. No legacy result is silently recast as
a V2 metric. Microservices and wholesale legacy copy were rejected as premature.
