# State of the art: reproducible financial streaming analytics and volatility forecasting

Review date: 2026-10-29. This is a targeted narrative review, not a systematic review or a claim of exhaustive coverage. It supports the project's IEEE report; it does not establish that the implemented models are state of the art.

## Review scope and verification

Searches covered event-time processing, Apache Spark Structured Streaming, realized volatility, HAR-RV, ARCH/GARCH, recurrent networks, tabular learning, volatility forecasting comparisons, foundation models, temporal validation and imbalanced evaluation. Sources were selected for direct relevance and identifiable primary publication or author records. Foundational work and selected 2022–2026 studies are included. Search phrases included “Structured Streaming declarative API”, “realized volatility linear models”, “HAR realized volatility”, and “realized volatility time series foundation models”.

Bibliographic details and the high-level claims below were checked against the linked publisher, author or proceedings records. Full-text URLs were located for several foundational papers, but this is not a page-by-page full-text review. Publisher abstracts/summaries underpin the recent financial comparison claims; no unavailable sample details, effect sizes or significance results are inferred. Brini [14] is cited as a 2026 arXiv preprint, without an asserted peer-reviewed venue. Re-run a broader literature search before submission if exhaustive or systematic coverage is required.

## A. Batch and streaming processing

Akidau et al. [1] frame streaming as a choice among correctness, latency and cost under unordered, unbounded input. Event time, windows and output timing therefore need explicit definitions. Armbrust et al. [2] describe Structured Streaming as incremental execution of declarative queries, supporting integration with batch analysis. Together these works motivate compatible analytical definitions and controlled replay comparisons, rather than treating “real time” as a property guaranteed by the engine name. [Dataflow paper](https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/), [Structured Streaming paper](https://cs.stanford.edu/~matei/papers/2018/sigmod_structured_streaming.pdf).

For this project, the resulting experimental questions concern equivalent logical outputs, event completeness, resource use and clearly defined latency. The existing producer-first replay includes queueing and startup; its latency cannot be substituted for steady-state exchange-to-alert delay. The project's Kafka/Spark implementation is an application and evaluation contribution, not a new streaming execution model.

## B. Volatility measurement and econometric baselines

ARCH [3] and GARCH [4] model time-varying conditional variance. They establish a forecasting tradition distinct from predicting the sign or conditional mean of returns. Andersen et al. [5] develop realized-volatility measurement and forecasting from high-frequency observations. Corsi's HAR-RV [6] uses volatility components over multiple time scales in a parsimonious forecasting model. These approaches motivate comparing complexity against meaningful volatility baselines. [ARCH](https://www.jstor.org/stable/1912773), [GARCH author record](https://scholars.duke.edu/publication/651658), [realized-volatility paper record](https://www.nber.org/papers/w8160), [HAR-RV author record](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1365738).

Phase 14 measures an H-minute square-root sum of squared log returns and then aggregates constituent variances within sectors. Applying the literature's daily or longer-horizon results to this intraday task requires new evaluation. Past-window persistence is implemented, but dedicated HAR-RV and GARCH benchmarks are not. The report must acknowledge this gap. The existing multiscale RV features and linear regression do not automatically constitute a validated HAR-RV implementation.

## C. Classical and recurrent learning

Random Forest combines randomized tree predictors [7], while LSTM addresses long-range recurrent learning [8] and Cho et al. introduce the gated recurrent unit in an encoder–decoder setting [9]. These are architectural foundations, not evidence that a particular architecture predicts financial volatility better. Grinsztajn et al. [10] find strong tree performance in their medium-sized tabular benchmark, supporting serious classical baselines; their setting differs from this project's sequential financial data. [Random Forest](https://doi.org/10.1023/A:1010933404324), [LSTM](https://www.bioinf.jku.at/publications/older/2604.pdf), [GRU origin](https://aclanthology.org/D14-1179/), [tabular comparison](https://arxiv.org/abs/2207.08815).

The methodological implication is to tune models separately by task and horizon, report training cost, and test whether recurrent information adds value beyond engineered history. This is an inference motivating the project design, not a finding reported by all four papers.

## D. Recent forecasting evidence and foundation models

Branco et al. [11] report that nonlinear methods do not systematically outperform simpler linear alternatives in their realized-volatility comparison. Díaz et al. [12] distinguish forecast accuracy from economic value, reporting horizon-dependent ML gains without corresponding benchmark-relative economic gains in their study. Their markets, inputs and forecast horizons differ from Phase 14; direct score comparisons would be invalid. [Branco et al.](https://doi.org/10.1016/j.jempfin.2024.101524), [Díaz et al.](https://doi.org/10.1016/j.irfa.2024.103286).

Chronos [13] studies pretrained probabilistic forecasting across diverse time-series datasets. This establishes a relevant future comparison family, not evidence of success for this sector-emergence target. Brini's 2026 preprint [14] compares foundation models with econometric volatility benchmarks and reports gains that vary substantially by asset and architecture. This supports retaining strong econometric comparators and disaggregated evaluation. Neither foundation models nor those econometric specifications have been evaluated in the current project. [Chronos](https://arxiv.org/abs/2403.07815), [Brini preprint](https://arxiv.org/abs/2607.05291).

## E. Evaluation and reproducibility

Bergstra and Bengio [15] motivate random search as a useful hyperparameter-optimization baseline. Rolling-origin evaluation [16] orders estimation before forecasting. Saito and Rehmsmeier [17] explain why precision–recall analysis is useful for imbalanced binary outcomes. These sources motivate explicit search budgets, chronological testing and event-sensitive metrics. They do not remove selection bias from repeatedly examining validation folds. [Random search](https://jmlr.org/papers/v13/bergstra12a.html), [temporal evaluation](https://otexts.com/fpp3/tscv.html), [imbalanced evaluation](https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0118432).

For the project, both prediction-window scores and episode-level detection are necessary. A mechanically H-minute future-window completion lead does not establish prediction of an independently dated economic onset. These last two points follow from the project's label construction and must be explained in methodology rather than attributed to the cited imbalanced-classification paper.

## Literature-to-project comparison matrix

| References | Evidence inspected | Contribution relevant here | Boundary / remaining project gap |
|---|---|---|---|
| [1]–[2] | Author/proceedings records and paper excerpts | Event-time semantics and incremental execution | Historical analytics equivalence does not validate live Phase 14 features |
| [3]–[4] | Publisher/author abstracts and metadata | Conditional variance models | Dedicated ARCH/GARCH comparison absent |
| [5]–[6] | NBER/author abstracts, publication metadata | Realized volatility and multiscale forecasting | Daily literature does not validate 5/15/30m sector targets; HAR-RV absent |
| [7]–[9] | Original publisher/proceedings records and paper excerpts | Forest and gated recurrent foundations | Architectural capacity is not a financial-performance result |
| [10] | Author abstract and proceedings paper excerpt | Competitive tree baselines | Medium-sized tabular benchmark differs from minute sequences |
| [11]–[12] | Publisher-indexed abstracts/highlights; full text not reviewed | Mixed accuracy gains and distinction from economic gains | No transfer of reported rankings to this experiment |
| [13] | Author abstract and metadata | Pretrained probabilistic forecasting | Not implemented; pretraining provenance matters in future tests |
| [14] | Author abstract and metadata | Recent benchmark-relative volatility comparison | Preprint; not an independently reproduced project result |
| [15]–[17] | Original journal/book content and metadata | Search, temporal validation and rare-event assessment | Current validation is reused for early stopping/selection; uncertainty remains |

## Defensible contribution statement

This project implements and evaluates a reproducible financial-data workflow combining historical and streaming analytics with independently configured multi-horizon volatility models. Its contribution is the integration and measured comparison of processing paths, explicit target contracts, temporal selection, resource accounting and auditable evaluation. It does not introduce a new recurrent architecture, prove a universal forecasting winner or establish profitable trading.

Sector emergence is an exploratory project-specific conjunction of elevated sector RV, constituent breadth, positive synchronization and bounded concentration. The reviewed sources do not validate this exact taxonomy. Novelty of that definition has not been established through an exhaustive domain review.

## Numbered reading references

The corresponding BibTeX keys are in `ieee/references.bib`; manuscript numbering will depend on citation order.

[1] T. Akidau et al., “The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing,” *Proc. VLDB Endow.*, vol. 8, no. 12, pp. 1792–1803, 2015, doi: 10.14778/2824032.2824076.

[2] M. Armbrust et al., “Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark,” in *Proc. SIGMOD*, 2018, doi: 10.1145/3183713.3190664.

[3] R. F. Engle, “Autoregressive Conditional Heteroscedasticity with Estimates of the Variance of United Kingdom Inflation,” *Econometrica*, vol. 50, no. 4, pp. 987–1007, 1982, doi: 10.2307/1912773.

[4] T. Bollerslev, “Generalized autoregressive conditional heteroskedasticity,” *J. Econometrics*, vol. 31, no. 3, pp. 307–327, 1986, doi: 10.1016/0304-4076(86)90063-1.

[5] T. G. Andersen, T. Bollerslev, F. X. Diebold, and P. Labys, “Modeling and Forecasting Realized Volatility,” *Econometrica*, vol. 71, no. 2, pp. 579–625, 2003. Author-access version: NBER Working Paper 8160, 2001, doi: 10.3386/w8160. The working-paper DOI is not represented as the journal DOI.

[6] F. Corsi, “A Simple Approximate Long-Memory Model of Realized Volatility,” *J. Financial Econometrics*, vol. 7, no. 2, pp. 174–196, 2009, doi: 10.1093/jjfinec/nbp001.

[7] L. Breiman, “Random Forests,” *Machine Learning*, vol. 45, pp. 5–32, 2001, doi: 10.1023/A:1010933404324.

[8] S. Hochreiter and J. Schmidhuber, “Long Short-Term Memory,” *Neural Computation*, vol. 9, no. 8, pp. 1735–1780, 1997.

[9] K. Cho et al., “Learning Phrase Representations using RNN Encoder–Decoder for Statistical Machine Translation,” in *Proc. EMNLP*, pp. 1724–1734, 2014, doi: 10.3115/v1/D14-1179.

[10] L. Grinsztajn, E. Oyallon, and G. Varoquaux, “Why do tree-based models still outperform deep learning on typical tabular data?,” in *Advances in Neural Information Processing Systems*, 2022. Author version: arXiv:2207.08815, whose title omits “typical”.

[11] R. R. Branco, A. Rubesam, and M. Zevallos, “Forecasting realized volatility: Does anything beat linear models?,” *J. Empirical Finance*, Art. no. 101524, 2024, doi: 10.1016/j.jempfin.2024.101524.

[12] J. D. Díaz, E. Hansen, and G. Cabrera, “Machine-learning stock market volatility: Predictability, drivers, and economic value,” *International Review of Financial Analysis*, vol. 94, Art. no. 103286, 2024, doi: 10.1016/j.irfa.2024.103286.

[13] A. F. Ansari et al., “Chronos: Learning the Language of Time Series,” arXiv:2403.07815, 2024.

[14] A. Brini, “Forecasting Realized Volatility with Time Series Foundation Models: A Comparison with Econometric Benchmarks,” arXiv:2607.05291, 2026. Preprint.

[15] J. Bergstra and Y. Bengio, “Random Search for Hyper-Parameter Optimization,” *J. Machine Learning Research*, vol. 13, pp. 281–305, 2012.

[16] R. J. Hyndman and G. Athanasopoulos, *Forecasting: Principles and Practice*, 3rd ed., OTexts, 2021, sec. 5.10. Online section accessed 2026-10-29.

[17] T. Saito and M. Rehmsmeier, “The Precision-Recall Plot Is More Informative than the ROC Plot When Evaluating Binary Classifiers on Imbalanced Datasets,” *PLOS ONE*, vol. 10, no. 3, Art. no. e0118432, 2015, doi: 10.1371/journal.pone.0118432.
