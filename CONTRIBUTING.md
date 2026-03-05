# Contributing to CPRD Extractor

Thank you for your interest in contributing to the CPRD Extractor. Contributions are welcome from the research community.

## How to Contribute

1. **Report bugs** — Open an issue describing the problem, the expected behaviour, and steps to reproduce.
2. **Suggest features** — Open an issue with the `enhancement` label describing your use case.
3. **Submit code** — Fork the repository, create a feature branch, and submit a pull request.

## Development Setup

```bash
git clone https://github.com/miladnazarzadeh/CprdExtractor.git
cd CprdExtractor
pip install -r requirements.txt
streamlit run app.py
```

The application will start in **Mock Data** mode by default, so no CPRD data access is required for development or testing.

## Code Style

- Follow PEP 8 conventions.
- Use descriptive variable names.
- Add docstrings to new functions.
- Test with Mock Data mode before submitting.

## Important Notes

- **Never commit patient data.** The `.gitignore` is configured to exclude data files, but please verify before pushing.
- CPRD licence terms apply. This tool facilitates data extraction — it does not distribute CPRD data.

## Contact

For questions, contact Milad Nazarzadeh at the University of Oxford.
