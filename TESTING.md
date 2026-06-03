# Testing Guide for bank_statement_parser

This guide explains how to run tests in bank_statement_parser using pytest markers for different testing scenarios.

## Quick Start

### Run all tests
```bash
pytest tests/
```

### Run only synthetic PDF tests (fast, no dependencies)
```bash
pytest tests/ -m synthetic
```

### Run only anonymised PDF tests (requires private repo access)
```bash
pytest tests/ -m anonymised
```

### Run specific test file
```bash
pytest tests/test_statements.py -v
```

---

## Available Pytest Markers

### `@pytest.mark.synthetic`
Tests using **synthetic (completely fake) PDFs** that are committed to this repo.

**When to use**:
- Local development (fast, no setup required)
- CI/CD pipelines (safe, no sensitive data)
- Quick validation of parsing logic

**Example**:
```bash
pytest -m synthetic -v
```

### `@pytest.mark.anonymised`
Tests using **anonymised real PDFs** from the private `bank-statement-data` repo.

**When to use**:
- Validation against real bank statement structures
- Integration testing with actual bank formats
- Pre-release testing

**Requires**:
- SSH access to private `bank-statement-data` repo
- `SSH_PRIVATE_KEY_TEST_DATA` secret configured in CI

**Example**:
```bash
pytest -m anonymised -v
```

---

## Test Categories & Recommended Usage

### 1. Development (Local Machine)

Use **synthetic PDFs only** for fast iteration:

```bash
# Run all synthetic tests
pytest tests/ -m synthetic -v

# Or run specific test file
pytest tests/test_statements.py::test_parse_hsbc -m synthetic -v

# Run with coverage
pytest tests/ -m synthetic --cov=bank_statement_parser --cov-report=html
```

**Why**: Synthetic tests are deterministic, fast, and need no special setup.

### 2. CI/CD (Pull Requests)

Uses **synthetic PDFs by default**, with optional anonymised PDFs:

```bash
# PR CI runs this (graceful fallback to synthetic if SSH unavailable)
pytest tests/ -v

# Both synthetic and anonymised if SSH secret available
pytest tests/ -m "synthetic or anonymised" -v
```

**Configuration**: See `.github/workflows/ci.yml`

### 3. Release Testing

Uses **both synthetic AND anonymised PDFs** for thorough validation:

```bash
# Run all tests (both synthetic and anonymised)
pytest tests/ -v

# Or explicitly require anonymised tests
pytest tests/ -m anonymised -v
```

**Requirement**: SSH secret `SSH_PRIVATE_KEY_TEST_DATA` must be available.

---

## Test Fixtures

### PDF Fixtures

All PDF fixtures are defined in `tests/conftest.py`:

#### `anonymised_pdf_dir()` (session scope)
Returns Path to anonymised PDFs from central `bank-statement-data` repo.

**Fallback behavior**: Uses bundled test PDFs if central repo unavailable.

**Usage in tests**:
```python
def test_parse_with_anonymised(anonymised_pdf_dir):
    good_pdf = list((anonymised_pdf_dir / "good").glob("*.pdf"))[0]
    result = parse_pdf(good_pdf)
    assert result is not None
```

#### `synthetic_pdf_dir()` (session scope)
Returns Path to synthetic PDFs committed to this repo.

**Usage in tests**:
```python
def test_parse_with_synthetic(synthetic_pdf_dir):
    pdf = list((synthetic_pdf_dir / "good").glob("*.pdf"))[0]
    result = parse_pdf(pdf)
    assert result is not None
```

#### `sample_good_pdf()` (function scope)
Returns a random good PDF from anonymised collection.

**Usage in tests**:
```python
def test_with_random_good_pdf(sample_good_pdf):
    result = parse_pdf(sample_good_pdf)
    assert result.success
```

#### `sample_bad_pdf()` (function scope)
Returns a random bad PDF (expected to fail parsing).

**Usage in tests**:
```python
def test_with_bad_pdf(sample_bad_pdf):
    with pytest.raises(ParseError):
        parse_pdf(sample_bad_pdf)
```

---

## Running Tests with Different Markers

### Combination Patterns

```bash
# Run synthetic OR anonymised (union)
pytest tests/ -m "synthetic or anonymised" -v

# Run synthetic AND NOT anonymised (intersection with negation)
pytest tests/ -m "synthetic and not anonymised" -v

# Run anonymised only (exclude synthetic)
pytest tests/ -m "anonymised and not synthetic" -v

# Run everything except a marker
pytest tests/ -m "not synthetic" -v
```

---

## Continuous Integration

### Development/PR CI (.github/workflows/ci.yml)

Runs tests with synthetic PDFs; gracefully falls back if anonymised unavailable:

```yaml
- name: Test (pytest)
  run: uv run pytest tests/ -v
  env:
    QT_QPA_PLATFORM: offscreen
```

**Result**: ✅ PR passes even without anonymised PDFs.

### Release CI

Enforces anonymised PDFs before release:

```bash
# Example (not yet implemented, for future reference)
pytest tests/ -m anonymised -v --strict-markers
```

---

## Troubleshooting

### No tests collected (marker mismatch)

**Problem**: `pytest -m unknown_marker` collects 0 items

**Solution**: Use `-v` to see which markers are available:
```bash
pytest --markers
```

Expected output should include:
```
@pytest.mark.synthetic: tests using synthetic PDFs (safe for public CI)
@pytest.mark.anonymised: tests using anonymised real PDFs (requires private repo access)
```

### Tests fail with "No good PDFs found"

**Problem**: `FileNotFoundError: No good PDFs found in ...`

**Solution**: Check that test PDFs exist in expected location:
```bash
ls -la tests/test_data/pdfs/good/
```

If empty, run fixture setup (or use synthetic PDFs instead).

### Import errors in tests

**Problem**: `ModuleNotFoundError: No module named 'bank_statement_parser'`

**Solution**: Install dependencies:
```bash
uv sync --group dev
uv run pytest tests/
```

---

## Test Development Workflow

When writing new tests:

1. **Use `@pytest.mark.synthetic` by default**:
   ```python
   @pytest.mark.synthetic
   def test_new_feature(synthetic_pdf_dir):
       # Test with synthetic PDFs
   ```

2. **Use `@pytest.mark.anonymised` for real-world edge cases**:
   ```python
   @pytest.mark.anonymised
   def test_edge_case_with_real_pdf(anonymised_pdf_dir):
       # Test with real bank statement structure
   ```

3. **Use both markers if test should pass in all scenarios**:
   ```python
   @pytest.mark.synthetic
   @pytest.mark.anonymised
   def test_core_parsing_logic(sample_good_pdf):
       # Works with any valid PDF
   ```

---

## Version Pinning

Bank_statement_parser does not have external version constraints (it's the reference implementation).

However, ensure tests pass with the pinned versions in dependent projects:
- **bank_statement_anonymiser**: Pinned to `bsp==0.2.1b7`
- **openstan**: Pinned to `bsp==0.2.1b7`

---

## Further Reading

- [pytest markers documentation](https://docs.pytest.org/en/stable/how-to.html#marking-whole-classes-or-modules)
- [Conftest.py guide](https://docs.pytest.org/en/stable/conftest.html)
- [Test fixtures documentation](https://docs.pytest.org/en/stable/fixture.html)
- [Central PDF strategy](../../bank-statement-data/README.md)
