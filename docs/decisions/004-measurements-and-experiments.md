# ADR 004: Version estimands and freeze experiment stages

Date: 2026-11-02. Reviewed: 2026-11-03. Status: accepted specification;
experimental thresholds are versioned hypotheses, not validated detections.

Decision: distinguish intrabar returns, adjacent-close returns, sample rolling SD,
realized volatility, sector RMS intensity and covariance-inclusive portfolio RV.
Use complete same-session one-minute windows, variable expected membership, null
for incomplete windows and explicit coverage. One measurement specification drives
pure numerical fixtures and engine parity, with exact keys/counts/nulls and
`atol=1e-12, rtol=1e-10` for finite floating values.

Keep training/inference separate; chronological horizon-purged splits and train-only
calibration precede frozen selection and one held-out evaluation. Publish completion
last, verify artifact hashes on recovery, and preserve completed experiments.

Consequences: fewer superficially complete windows but defensible comparability.
No filling is hidden inside indicators. Neural tuning and deployment are deferred;
metrics and event thresholds are hypotheses requiring baseline validation.

Review correction: measurement specification v2.0.0 separates the former mean-log
return proxy from exact equal-weight rebalanced portfolio log returns. The latter
uses `log(mean(exp(r_i)))` each minute before summing squares. The original proxy
retains an explicit identifier rather than being silently renamed portfolio risk.
This is a meaning change, so the specification advances its major version; no
production V2 outputs or legacy experiments are rewritten. Bar schema stays 1.0.0.
Scalar arithmetic fixtures independently check the distinction. Measurement,
detector-rule and fitted-calibration versions have separate identities; refitting
calibration alone does not change the realized-volatility estimator.
