# =============================================================================
# CPRD Extractor - app.py
# =============================================================================
# Version 1.0.0
# Developed by Dr Milad Nazarzadeh
# Nuffield Department of Women's & Reproductive Health, University of Oxford
# HEART-MIND Programme
#
# An interactive Streamlit application providing a complete workflow for
# researchers working with CPRD Aurum electronic health record data.
#
# Modules:
#   - Home Dashboard
#   - Code List Development (6-stage pipeline)
#   - Drug Lookup (315 cardiovascular/cardiometabolic drugs)
#   - Quick Extract (Newbie mode)
#   - Demographics
#   - CPRD Aurum Extraction
#   - Linkage Extraction (HES APC/OP/AE, ONS Death, IMD)
#   - Multi-Source Search
#   - Cohort Builder
#   - Analytics
#   - Definitions
#   - Configuration
#
# Usage:
#   streamlit run app.py
#
# HPC/Slurm CLI mode:
#   python app.py --generate-slurm --extract-type snomed \
#     --codes 60573004,86466006 --total-tasks 50 --output-dir /path/to/output
# =============================================================================

import streamlit as st
import pandas as pd
import numpy as np
import os
import re
import time
import argparse

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False

try:
    import pyarrow
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

try:
    import plotly.graph_objects as go
    import plotly.express as px
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

# =============================================================================
# CONFIGURATION & PATHS
# =============================================================================

DEFAULT_OUTPUT_DIR = os.path.expanduser("~/cprd_extractor_output")

PATHS = {
    "aurum_base": "/gpfs3/well/rahimi/projects/CPRD",
    "emis_dictionary": "/gpfs3/well/rahimi/projects/CPRD/202102_lookups/202102_EMISMedicalDictionary.txt",
    "linkage_eligibility": "/gpfs3/well/rahimi/projects/CPRD/linkage_eligibility_Set21",
    "linkage_eligibility_aurum": "/gpfs3/well/rahimi/projects/CPRD/linkage_eligibility_Set21_aurum",
    "linkage_base": [
        "Results-one/Aurum_linked/Final",
        "Results-two/Aurum_linked/Final",
        "Results-three/Aurum_linked/Final",
    ],
    # HES / Linkage paths
    "hes_diagnosis_hosp": "",
    "hes_diagnosis_epi": "",
    "hes_primary_diag": "",
    "hes_episodes": "",
    "hes_hospital": "",
    "hes_procedures_epi": "",
    "hes_op_clinical": "",
    "hes_op_patient": "",
    "hes_ae_attendance": "",
    "hes_ae_diagnosis": "",
    "death_patient": "",
    "hes_patient": "",
    "hes_maternity": "",
    "hes_ccare": "",
    "patient_imd": "",
    "practice_imd": "",
}

# =============================================================================
# NOTE: This is the application entry point.
# The full source code for CPRD Extractor v1.0.0 includes:
#
#  - CPRDEngine class: DuckDB-powered extraction engine with mock data support,
#    practice folder discovery, parallel scanning, and Slurm job generation.
#
#  - Disease Library: 50 cardiovascular conditions with SNOMED CT and ICD-10
#    code sets (coronary heart disease, valvular disease, arrhythmias,
#    cardiomyopathies, vascular disease, congenital heart disease, heart
#    failure, and infectious/inflammatory conditions).
#
#  - Drug Library: 315 cardiovascular/cardiometabolic drugs across 18
#    therapeutic classes (ACE inhibitors, ARBs, CCBs, beta-blockers,
#    diuretics, MRAs, antiplatelets, anticoagulants, antiarrhythmics,
#    statins, PCSK9 inhibitors, SGLT2i, GLP-1 RA, DPP-4i, ARNI,
#    pulmonary hypertension agents, and more).
#
#  - Page functions: page_home(), page_code_list_dev(), page_drug_lookup(),
#    page_newbie(), page_demographics(), page_aurum_extraction(),
#    page_linkage_extraction(), page_multi_source(), page_cohort_builder(),
#    page_analytics(), page_definitions(), page_config()
#
#  - Utility functions: autosave_to_disk(), enrich_with_code_details(),
#    classify_codes(), build_save_label(), apply_patient_scope(),
#    download_results(), get_output_settings()
#
#  - Streamlit sidebar navigation with Data Mode selector
#    (Mock Data, Live-BMRC, Live-Any Server, Windows)
# =============================================================================


def main():
    st.set_page_config(
        page_title="CPRD Extractor",
        page_icon="\U0001f5c2",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.sidebar.title("\U0001f5c2 CPRD Extractor")
    st.sidebar.caption("v1.0 \u00b7 University of Oxford")

    page = st.sidebar.radio(
        "Navigate",
        [
            "\U0001f3e0 Home",
            "\U0001f4cb Code List Development",
            "\U0001f9ea Drug Lookup",
            "\u2b50 Quick Extract (Newbie)",
            "\U0001f465 Demographics",
            "\U0001f9ec CPRD Aurum Extraction",
            "\U0001f517 Linkage Extraction",
            "\U0001f50d Multi-Source Search",
            "\U0001f465\U0001f465 Cohort Builder",
            "\U0001f4ca Analytics",
            "\U0001f4d6 Definitions",
            "\u2699\ufe0f Configuration",
        ],
    )

    st.info(
        "CPRD Extractor v1.0.0 \u2014 Full application source available at: "
        "https://github.com/miladnazarzadeh/CprdExtractor"
    )


if __name__ == "__main__":
    main()
