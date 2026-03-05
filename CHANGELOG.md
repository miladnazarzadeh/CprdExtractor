# Changelog

All notable changes to the CPRD Extractor are documented in this file.

## [1.0.0] — 2026-03-01

### Added

- **Code List Development** — Six-stage pipeline from clinical concept definition through CPRD EMIS Dictionary matching to clinician-reviewed code lists, with full audit trail and Excel export.
- **Drug Lookup** — Curated library of 315 cardiovascular and cardiometabolic drugs across 18 therapeutic classes, with automated CPRD Product Dictionary matching by generic name, brand name, and BNF code.
- **Disease Library** — Built-in SNOMED CT and ICD-10 code sets for 50 cardiovascular conditions spanning coronary heart disease, valvular heart disease, arrhythmias, cardiomyopathies, vascular disease, congenital heart disease, heart failure, and infectious/inflammatory conditions.
- **CPRD Aurum Extraction** — High-performance extraction from Observation, DrugIssue, Consultation, Patient, Practice, Problem, Referral, and Staff files using DuckDB, with parallel folder scanning and real-time progress tracking.
- **Linkage Data Extraction** — Support for HES Admitted Patient Care (diagnoses, procedures, episodes), HES Outpatient, HES A&E, ONS Mortality, and Index of Multiple Deprivation datasets.
- **Multi-Source Search** — Simultaneous extraction across CPRD primary care and all linked datasets with unified results.
- **Cohort Builder** — Interactive inclusion/exclusion criteria with attrition flowcharts, patient scope linking across extractions, and linkage eligibility filtering.
- **Demographics** — Automated demographic extraction including sex, age, deprivation (IMD quintile), ethnicity, and registration period.
- **Analytics** — Descriptive statistics, frequency distributions, temporal trends, and interactive Plotly visualisations.
- **Quick Extract (Newbie) mode** — Simplified one-click workflow for users new to CPRD data.
- **Slurm HPC support** — CLI mode with automatic Slurm job array script generation, shard-based parallel extraction, and one-command merge pipeline.
- **Cross-platform compatibility** — Tested on Oxford BMRC cluster, generic Linux servers, and Windows.
- **Output flexibility** — Parquet (default) and CSV export with automatic code description enrichment.
