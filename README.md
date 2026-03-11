# 🗂️ CPRD Extractor

**An interactive platform for Clinical Practice Research Datalink (CPRD) Aurum data extraction, code list development, and cohort assembly.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Built%20with-Streamlit-ff4b4b.svg)](https://streamlit.io/)
[![DOI](https://img.shields.io/badge/DOI-10.5281%2Fzenodo.placeholder-blue)](https://doi.org/)

> Developed by **Dr Milad Nazarzadeh**
> Nuffield Department of Women's & Reproductive Health, University of Oxford

---

## Overview

CPRD Extractor is an open-source, browser-based application that provides a complete workflow for researchers working with CPRD Aurum electronic health record data. It unifies clinical code list development, high-performance data extraction, linked dataset querying, and cohort assembly into a single interactive platform.

The tool is designed to run on institutional high-performance computing (HPC) clusters (tested on the Oxford BMRC environment), generic Linux servers, and Windows workstations. It includes a built-in mock data mode for testing and demonstration without requiring CPRD data access.

### Key Capabilities

- **Code List Development** — A structured six-stage pipeline for building, validating, and auditing clinical code lists (SNOMED CT, ICD-10, Read, medcodeid).
- **Drug Lookup** — A curated library of 315 cardiovascular and cardiometabolic drugs across 18 therapeutic classes with automated CPRD Product Dictionary matching.
- **Disease Library** — Pre-built SNOMED CT and ICD-10 code sets for 50+ cardiovascular conditions.
- **High-Performance Extraction** — DuckDB-powered parallel extraction from CPRD Aurum primary care files (Observation, DrugIssue, Patient, Consultation, Problem, Referral, Staff, Practice).
- **Linked Data Support** — Extraction from HES Admitted Patient Care, HES Outpatient, HES A&E, ONS Mortality, and Index of Multiple Deprivation.
- **Cohort Builder** — Interactive inclusion/exclusion criteria with attrition flowcharts and cross-extraction patient linking.
- **HPC Integration** — Automatic Slurm job array script generation for cluster-scale extraction with one-command launch.

---

## Screenshots

### Home Dashboard

The home screen provides an overview of the data environment, configured paths, and available modules.

![Home Dashboard](screenshots/home.png)

---

### Code List Development

A six-stage pipeline guides the user from defining a clinical feature of interest, through synonym generation and code browser searches, to CPRD EMIS Dictionary matching with full audit trail and Excel export.

![Code List Development](screenshots/codelist_development.png)

---

### Drug Lookup

Search and browse 315 cardiovascular/cardiometabolic drugs by therapeutic class, generic name, or brand name. Matched product codes (prodcodeids) are returned for direct use in CPRD DrugIssue extraction.

![Drug Lookup](screenshots/drug_lookup.png)

---

### CPRD Aurum Primary Care Extraction

Extract records from Observation, DrugIssue, Consultation, and other CPRD Aurum file types. Select conditions from the built-in disease library or enter custom SNOMED CT/medcode codes. Real-time progress tracking shows folder-level scanning status.

![Aurum Extraction](screenshots/aurum_extraction.png)

---

### Cohort Builder

Define inclusion and exclusion criteria interactively. The attrition flow visualises each filtering step with patient counts. Summary statistics (sex, age, follow-up, linkage eligibility) are computed automatically.

![Cohort Builder](screenshots/cohort_builder.png)

---

## Installation

### Prerequisites

- Python 3.9 or later
- pip (Python package manager)

### Setup

```bash
# Clone the repository
git clone https://github.com/miladnazarzadeh/CprdExtractor.git
cd CprdExtractor

# Install dependencies
pip install -r requirements.txt

# Launch the application
streamlit run app.py
```

The application opens in your default browser at `http://localhost:8501`.

### Running on an HPC Cluster (e.g. Oxford BMRC)

On clusters where internet access is restricted, install dependencies from a login node or pre-built environment:

```bash
module load Python/3.11.3-GCCcore-12.3.0
pip install --user -r requirements.txt

# Forward a port from the cluster to your local machine
ssh -L 8501:localhost:8501 username@bmrc-server

# On the cluster
streamlit run app.py --server.port 8501 --server.headless true
```

Then open `http://localhost:8501` in your local browser.

---

## Quick Start

### 1. Select a Data Mode

Use the sidebar to choose your environment:

| Mode | Description |
|------|-------------|
| 🧪 **Mock Data** | Synthetic data for testing and demonstration (default) |
| 🔬 **Live — BMRC** | Pre-configured paths for the Oxford BMRC cluster |
| 🖥️ **Live — Any Server** | Specify a custom CPRD data root directory |
| 💻 **Windows** | Windows-compatible mode with native path handling |

### 2. Develop a Code List (Optional)

Navigate to **📋 Code List Development** to build a clinical code list:

1. **Define** the clinical feature of interest
2. **Generate** synonyms and identify existing published code lists
3. **Search** code browsers (SNOMED CT, ICD-10)
4. **Review** and classify candidate codes
5. **Match** against the CPRD EMIS Medical Dictionary to obtain medcodeids
6. **Export** a clinician-review questionnaire with full audit trail

### 3. Extract Data

Navigate to **🧬 CPRD Aurum Extraction** or **🔗 Linkage Extraction**:

- Select a condition from the **Disease Library** (50+ cardiovascular conditions) or enter custom codes
- Optionally scope the extraction to patients from a previous extraction
- Click **Extract** and monitor real-time progress
- Results are displayed in-app and auto-saved to disk in Parquet or CSV format

### 4. Build a Cohort

Navigate to **👥👥 Cohort Builder** to apply eligibility criteria:

- Set age ranges, registration requirements, and linkage eligibility filters
- View the attrition flowchart with patient counts at each step
- Export the final cohort for downstream analysis

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Streamlit Web Interface                   │
├──────────┬──────────┬──────────┬──────────┬────────────────┤
│ Code List│  Drug    │  Aurum   │ Linkage  │    Cohort      │
│   Dev    │  Lookup  │Extraction│Extraction│    Builder     │
├──────────┴──────────┴──────────┴──────────┴────────────────┤
│                      CPRDEngine Core                        │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │  DuckDB SQL  │  │  Mock Data   │  │  Slurm CLI Mode   │  │
│  │  (parallel)  │  │  Generator   │  │  (task arrays)    │  │
│  └─────────────┘  └──────────────┘  └───────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                    Data Layer (Read-Only)                    │
│  CPRD Aurum │ HES APC/OP/A&E │ ONS Mortality │ IMD │ EMIS  │
└─────────────────────────────────────────────────────────────┘
```

### Core Components

| Component | Description |
|-----------|-------------|
| `CPRDEngine` | Central engine handling practice folder discovery, file scanning, DuckDB-based extraction, and mock data generation |
| `Code List Development` | Six-stage pipeline with EMIS Dictionary matching, expansion analysis, and clinical review questionnaire generation |
| `Drug Code Library` | 315 drugs with generic names, brand names, BNF codes, therapeutic classes, and CPRD Product Dictionary search terms |
| `Disease Library` | 50+ cardiovascular conditions with validated SNOMED CT and ICD-10 code sets |
| `Slurm Integration` | CLI mode for HPC job arrays with automatic script generation, shard-based parallelism, and merge pipeline |

---

## Modules

| Module | Navigation Label | Description |
|--------|-----------------|-------------|
| Home | 🏠 Home | Dashboard with data environment status and path verification |
| Code List Dev | 📋 Code List Development | Six-stage code list creation and EMIS Dictionary matching |
| Drug Lookup | 🧪 Drug Lookup | Search 315 drugs by class, name, or BNF code |
| Quick Extract | ⭐ Quick Extract (Newbie) | Simplified one-click extraction for new users |
| Demographics | 👤 Demographics | Sex, age, IMD, ethnicity, and registration period extraction |
| Aurum Extraction | 🧬 CPRD Aurum Extraction | Primary care data extraction by SNOMED, medcode, or prodcode |
| Linkage Extraction | 🔗 Linkage Extraction | HES APC, HES OP, HES A&E, ONS Death, and IMD extraction |
| Multi-Source | 🔍 Multi-Source Search | Simultaneous search across all CPRD and linked datasets |
| Cohort Builder | 👥👥 Cohort Builder | Inclusion/exclusion criteria with attrition flow |
| Analytics | 📊 Analytics | Descriptive statistics, temporal trends, and visualisations |
| Definitions | 📖 Definitions | Reference glossary for CPRD-specific terminology |
| Configuration | ⚙️ Configuration | Path management, output settings, and SSH connection panel |

---

## Slurm HPC Mode

For large-scale extractions on HPC clusters, the application generates Slurm job array scripts:

```bash
# Generate Slurm scripts
python app.py --generate_slurm \
    --extract_type snomed \
    --codes "60573004,86466006,83916000" \
    --total_tasks 50 \
    --output_dir /path/to/output

# One-command launch (submits extraction array + merge job)
cd /path/to/output
bash cprd_snomed_launch.sh

# Or step-by-step
sbatch cprd_snomed.sh                    # Submit array (50 parallel tasks)
sbatch --dependency=afterok:$JOB_ID cprd_snomed_merge.sh  # Merge shards
```

Each task processes a subset of practice folders in parallel. The merge job combines all Parquet shards into a single output file.

---

## Output Formats

| Format | Extension | Advantages |
|--------|-----------|------------|
| **Parquet** (default) | `.parquet` | ~5× smaller, ~10× faster to read, preserves data types |
| **CSV** | `.csv` | Universal compatibility, human-readable |
| **Both** | `.parquet` + `.csv` | Maximum flexibility |

All outputs can optionally include human-readable code descriptions merged from the EMIS Medical Dictionary.

---

## Disease Library Coverage

The built-in disease library includes validated SNOMED CT and ICD-10 code sets for:

- **Coronary Heart Disease** — Stable angina, unstable angina, NSTEMI, STEMI, chronic coronary syndrome
- **Valvular Heart Disease** — Aortic stenosis/regurgitation, mitral stenosis/regurgitation/prolapse, tricuspid and pulmonary valve disease, rheumatic heart disease
- **Arrhythmias** — Atrial fibrillation/flutter, SVT, VT/VF, bradycardia, heart block, long QT, Brugada, WPW
- **Cardiomyopathies** — Dilated, hypertrophic, restrictive, ARVC, Takotsubo, peripartum, amyloid, sarcoid
- **Vascular Disease** — PAD, carotid disease, aortic aneurysm/dissection, DVT, PE, renal artery stenosis
- **Congenital Heart Disease** — ASD, VSD, coarctation, Tetralogy of Fallot, TGA, HLHS, PDA
- **Heart Failure** — All subtypes with 50+ SNOMED codes
- **Infectious/Inflammatory** — Endocarditis, myocarditis, pericarditis, Kawasaki, Chagas

---

## Drug Library Coverage

315 cardiovascular and cardiometabolic drugs across 18 therapeutic classes:

Antiarrhythmics, Anticoagulants, Antihypertensives (ACE inhibitors, ARBs, CCBs, beta-blockers, diuretics, MRAs), Antiplatelets, Cardiac Amyloidosis agents, Critical Care & Vasoactive agents, Diabetes/Glucose-Lowering agents (SGLT2i, GLP-1 RA, DPP-4i, insulin), Heart Failure agents (ARNI, ivabradine, vericiguat), Lipid-Lowering agents (statins, PCSK9i, inclisiran, bempedoic acid), Nitrates & Antianginals, Obesity (CV-relevant), Pericarditis & Inflammatory, Peripheral Vascular Disease, Potassium Management, Pulmonary Hypertension, and Thrombolytics.

---

## Requirements

| Package | Minimum Version | Purpose |
|---------|----------------|---------|
| `streamlit` | 1.28.0 | Web interface |
| `pandas` | 1.5.0 | Data manipulation |
| `numpy` | 1.23.0 | Numerical operations |
| `duckdb` | 0.9.0 | High-performance SQL extraction |
| `pyarrow` | 12.0.0 | Parquet I/O |
| `openpyxl` | 3.1.0 | Excel export (code list questionnaires) |
| `plotly` | 5.15.0 | Interactive visualisations |

---

## Citation

If you use CPRD Extractor in your research, please cite:

```bibtex
@software{nazarzadeh2026cprdextractor,
  author    = {Nazarzadeh, Milad},
  title     = {{CPRD Extractor: An Interactive Platform for Clinical Practice
                Research Datalink Data Extraction and Cohort Assembly}},
  year      = {2026},
  url       = {https://github.com/miladnazarzadeh/CprdExtractor},
  version   = {1.0.0},
  institution = {Nuffield Department of Women's and Reproductive Health,
                 University of Oxford}
}
```

---

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## Licence

This project is licensed under the [MIT License](LICENSE).

**Note:** This software facilitates extraction from CPRD data. It does not distribute or contain any patient data. Users must hold a valid CPRD data licence and comply with all applicable data governance requirements.

---

## Acknowledgements

This tool was developed as part of the HEART-MIND Programme at the University of Oxford, supported by the Nuffield Department of Women's & Reproductive Health.
