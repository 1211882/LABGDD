# IEEE report documentation guide

Documentation date: 2026-10-29. Training remains active; this documentation does not pause or alter it.

## Start here

| File | Purpose |
|---|---|
| [IEEE_STATE_OF_THE_ART.md](IEEE_STATE_OF_THE_ART.md) | Critical literature synthesis, comparison matrix, numbered references and search scope |
| [ieee/related_work.tex](ieee/related_work.tex) | LaTeX related-work section with BibTeX citation keys |
| [ieee/references.bib](ieee/references.bib) | Bibliography for `IEEEtran` |
| [IEEE_PROJECT_EVIDENCE.md](IEEE_PROJECT_EVIDENCE.md) | Research questions, methods, evidence map, operational history and limitations |
| [ieee/project_snapshot.json](ieee/project_snapshot.json) | Timestamped environment, source fingerprints and completed-result inventory |
| [PHASE14_V3_INTERIM_ANALYSIS.md](PHASE14_V3_INTERIM_ANALYSIS.md) | Fixed interim results analysis; not a live dashboard |
| [phase14_v3_interim_reference_metrics.csv](phase14_v3_interim_reference_metrics.csv) | All 93 reference combinations, parameters, fold scores, epochs and durations |

## Suggested manuscript structure

1. Introduction: financial monitoring problem, research questions and bounded contributions.
2. Related Work / State of the Art: streaming semantics, volatility measurement, econometric and ML forecasting, evaluation.
3. System Architecture: REST and WebSocket ingestion, Kafka, Spark Batch/Structured Streaming, persisted analytics, observability and presentation.
4. Data and Methodology: source, universe, sessions, missingness, target equations, temporal partitions, baselines, independent tuning and runtime measurement.
5. Experimental Results: completed processing experiments first; frozen Phase 10/11 results separately; final Phase 14 results only when validated. Label any interim subsection explicitly.
6. Discussion and Threats to Validity: computational constraints, one feed/year/universe, missing minutes, rare events, repeated validation selection and previously inspected TEST.
7. Conclusion and Future Work: conclusions supported by measured evidence, then pending live validation and stronger benchmarks.

The literature section describes other authors' findings. The methodology describes this implementation. The results section contains this project's measurements. Do not mix these categories.

## Using the LaTeX material

Copy `related_work.tex` and `references.bib` into the manuscript directory, then use:

```latex
\input{related_work}
% At the end of the manuscript:
\bibliographystyle{IEEEtran}
\bibliography{references}
```

IEEEtran/BibTeX assigns citation numbers by the manuscript's citation order; numbers in the Markdown reading copy are only local labels. Compile using the university or venue template. No LaTeX compilation is claimed for these snippets.

## Before final submission

- Replace interim model scores with the final validated output; record model-specific populations and common-key comparisons.
- Include exact selected parameters and best epochs per model/horizon in a table or appendix, with timing, holdout metrics and applicable financial/monitoring metrics.
- Distinguish observed results from planned seeds, ablations, exports and streaming ML integration.
- Resolve the missing HAR-RV/GARCH comparison or state it as a limitation. Do not call the current models globally state of the art.
- Read full texts for any detailed empirical claim currently verified only from a publisher abstract or author summary. The literature matrix records this boundary.
- Add event-episode counts and uncertainty analysis; overlapping windows are not independent events. Fold SD is not a confidence interval.
- Report acquisition timestamps and the environment used for final training. A copied dataset's date labels alone do not establish original collection provenance.
- Record the final source revision, final dependency environment and artifact checksums. The present source snapshot includes uncommitted files and is not a substitute for a future commit.

## Computer migration status

No migration package or commit was completed. The user requested that training continue. This documentation does not authorize stopping it. Git omits large inputs, NumPy arrays, predictions and model binaries; a future transfer needs a separately verified artifact copy. Current resume requires identical settings including absolute paths, prepared-cache fingerprints and the preserved-evidence inventory. Moving directories requires a reviewed portability change; the current command must not be presented as a portable one-click resume.
