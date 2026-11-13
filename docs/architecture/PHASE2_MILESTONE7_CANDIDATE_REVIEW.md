# Milestone 7 candidate evidence review

Date: 2026-11-11. Result: passed, pending explicit activation authorization.

The review resolved identities from the immutable manifests, verified every declared
artifact size and SHA-256, rejected filesystem discovery, and checked Bronze-to-Silver-
to-Gold lineage against the inactive registry proposals. Active publication registries
were not modified. The machine-readable result is
`PHASE2_MILESTONE7_APPROVAL_REVIEW.json`.

## Candidate identities

- Bronze: `iex-1min-pilot-msft-nvda-jpm-xom-20260914-v1`, acquisition
  `ae7fecdc-d082-478d-b26d-b3e7e5db9600`, manifest SHA-256
  `7b1b3ce751ca50ded0fca940f029347f1a8f4bb80c5dc5212f2624a616e3f6a3`.
- Silver: `silver-pilot-five-symbol-20260914-v1`, run
  `f9d4bcc6-5a51-4c37-8d74-86cf9cee1ca2`, manifest SHA-256
  `f0ff790cfe0200e94c407010cfc4f2a8bfe93110d580f751b9f934095f9a15f8`.
- Individual Gold: `gold-pilot-five-symbol-20260914-individual-v1`, run
  `310941bd-7ff9-4e25-971a-febec753c9e5`, manifest SHA-256
  `c9ecea6009363f589bb57365acbccd6454fe8e122c3d737fbe3fd71363badad0`.
- Expected-Series Gold: `gold-study-cohort-20260914-multisymbol-expected-v1`, run
  `39ab367c-c6d2-480d-8c94-f35f767ed289`, manifest SHA-256
  `d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2`.

The Silver lineage reuses the separately preserved AAPL Bronze acquisition
`f2345e41-da10-420f-8b60-6079a9df2006` with manifest SHA-256
`048b6815a618b2779568ec6b10d15c0d71d4462f4d1282175508283765e8b9d0`.

## Scientific checks

Observed regular-session bars are AAPL 390, MSFT 390, NVDA 390, JPM 371 and XOM
320. Relative to the 390-minute XNYS grid, missing counts are 0, 0, 0, 19 and 70.
The 1,861 Silver canonical rows have 1,861 unique `(symbol, event_time)` keys. The
physical timestamp fields are Parquet `timestamp[us, tz=UTC]`; no missing observation
was filled or replaced with zero.

Independent scalar parity over all five symbols and horizons 5/15/30/60 passed:
1,950 return slots with 1,787 eligible; 7,780 rolling rows with 5,117 eligible; and
600 fixed windows with 468 eligible. Maximum absolute errors remained within the
approved tolerance; temporal/key mismatches were zero. Expected-Series Gold contains
30 symbols, 11,700 returns, 46,680 rolling rows and 3,600 fixed windows. Its 25
intentionally unrequested symbols account for 9,750 rows with zero observed bars and
remain distinct from JPM/XOM provider-response absence. Frozen AAPL parity passed.

## Limitations

This remains a one-session, five-symbol IEX pilot. IEX is venue-specific rather than
consolidated coverage. Provider correction finality is unproven. Historical listing,
entity identity and sector membership remain unverified research assumptions. Missing
JPM/XOM bars do not establish exchange-wide absence, and the 25 unrequested symbols
are not provider coverage failures. No sector metric was calculated.
