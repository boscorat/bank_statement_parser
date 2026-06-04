# Testing Guide

## Test Data Strategy

This project uses a multi-tiered test data approach for balancing security, data integrity, and developer UX:

- **Private Repo** (`bank-statement-data`): Contains anonymised real bank statements
- **Public Repos**: Use symlinks (for developers with private repo access) or fallback to bundled PDFs
- **CI/CD**: Automatically fetches & cleans up after tests (security: prevent accidental leaks)

### Why Multiple Tiers?

1. **Security**: Never commit real (even anonymised) data to public repos
2. **Testing Completeness**: Developers with access can test with real data structures
3. **Accessibility**: Tests work for everyone without special setup
4. **Automation**: CI/CD handles PDF fetching & cleanup transparently

---

## Running Tests

### Option 1: Full Suite with Anonymised PDFs (Recommended for Core Developers)

Requires SSH access to private `bank-statement-data` repo.

**Step 1: Set up symlinks (one-time only)**

```bash
# See detailed setup guide in private repo:
# https://github.com/boscorat/bank-statement-data/blob/master/SYMLINK_SETUP.md

# Quick reference for Linux/macOS:
ln -s ~/repos/bank-statement-data/pdfs/good \
      ~/repos/bank_statement_parser/src/bank_statement_parser/test_data/pdfs/anonymised_good

ln -s ~/repos/bank-statement-data/pdfs/bad \
      ~/repos/bank_statement_parser/src/bank_statement_parser/test_data/pdfs/anonymised_bad
```

**Step 2: Verify symlinks**

```bash
ls -la src/bank_statement_parser/test_data/pdfs/

# You should see:
# anonymised_good -> ~/repos/bank-statement-data/pdfs/good
# anonymised_bad -> ~/repos/bank-statement-data/pdfs/bad
```

**Step 3: Run tests**

```bash
uv run pytest tests/ -v

# Output should show:
# [PDF_FIXTURES] Mode: ANONYMISED
# [PDF_FIXTURES] Using ANONYMISED PDFs (symlinks detected)
```

### Option 2: Standard Suite without Anonymised PDFs

No setup required. Uses bundled PDFs from the installed package.

```bash
uv run pytest tests/ -v

# Output should show:
# [PDF_FIXTURES] Mode: BUNDLED
# [PDF_FIXTURES] Using BUNDLED PDFs from installed package
```

---

## PDF Modes Explained

| Mode | Source | Setup Required | Data Type | Use Case |
|------|--------|-----------------|-----------|----------|
| **Anonymised** | Symlink to private repo | Manual (one-time) | Real bank statements (anonymised) | Core development, full test coverage |
| **Bundled** | Installed package | None | Pre-generated PDFs | CI/CD, public developers, quick tests |
| **None** | N/A | N/A | N/A | Tests skip gracefully |

---

## CI/CD Behavior

When tests run in CI:

1. **Fetch Phase**: If `SSH_PRIVATE_KEY_TEST_DATA` secret is available, fetches anonymised PDFs to `/tmp/`
2. **Test Phase**: Runs tests with anonymised PDFs if available, otherwise uses bundled PDFs
3. **Cleanup Phase**: Removes all fetched PDFs (security: prevent accidental commits)
4. **Fallback**: If SSH key unavailable, gracefully uses bundled PDFs

**Why temporary storage?**
- Security: PDFs never committed to git
- Isolation: Tests run in clean environment
- Cleanup: Guaranteed removal even if tests fail

---

## Troubleshooting

### Q: Some tests are being skipped?

**A:** Only certain tests require PDF fixtures. To see which are skipping:

```bash
uv run pytest tests/ -v 2>&1 | grep -i "skip\|pdf"
```

If fixtures are missing, set up symlinks per Option 1 above.

### Q: Can I commit PDFs to this repo?

**A:** No. `.gitignore` is configured to reject all PDFs:

```
*.pdf                                      # Block all PDFs
!src/bank_statement_parser/test_data/pdfs/**/synthetic_*.pdf  # Allow synthetic only
```

Anonymised PDFs must stay in the private `bank-statement-data` repo.

### Q: Where do I find the anonymised PDFs?

**A:** In the private `bank-statement-data` repo at `pdfs/good/` and `pdfs/bad/`.

You need SSH access. Ask the team for access if you don't have it.

### Q: What if I don't have access to the private repo?

**A:** You can still run tests! The suite falls back to bundled PDFs automatically. Some tests might skip (those requiring specific data structures), but core functionality tests will pass.

### Q: How do I check which PDFs are being used?

**A:** Look for the `[PDF_FIXTURES]` message when tests start:

```bash
uv run pytest tests/ -v 2>&1 | grep "PDF_FIXTURES"

# Example output:
# [PDF_FIXTURES] Mode: ANONYMISED
# [PDF_FIXTURES] Using ANONYMISED PDFs (symlinks detected)
# [PDF_FIXTURES] Location: /home/user/repos/bank-statement-data/pdfs/good
```

---

## Local Development Workflow

### Setup (First Time)

```bash
# 1. Clone the repo (you already have this)
cd ~/repos/bank_statement_parser

# 2. If you have access to private repo, set up symlinks
ln -s ~/repos/bank-statement-data/pdfs/good src/bank_statement_parser/test_data/pdfs/anonymised_good
ln -s ~/repos/bank-statement-data/pdfs/bad src/bank_statement_parser/test_data/pdfs/anonymised_bad

# 3. Verify symlinks work
ls -la src/bank_statement_parser/test_data/pdfs/anonymised_*
```

### Daily Testing

```bash
# Tests automatically detect and use available PDFs
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/test_cli.py -v

# Run tests matching a pattern
uv run pytest -k "test_import" -v
```

### If Symlinks Break

If symlinks are deleted or become invalid:

```bash
# Tests will gracefully fall back to bundled PDFs
# Just run tests again - no setup needed

uv run pytest tests/ -v

# Output will show:
# [PDF_FIXTURES] Mode: BUNDLED
```

---

## For Contributors

### Running Full Test Suite Before Submitting PR

```bash
# 1. Lint
uv run ruff check .

# 2. Format
uv run ruff format --check .

# 3. Tests
uv run pytest tests/ -v

# 4. If available, run with anonymised PDFs (see Option 1 above)
```

### Adding New Tests

New tests requiring PDF processing:

```python
def test_my_feature(good_project):
    """This test requires anonymised PDFs (will skip if unavailable)."""
    pdfs = good_project.pdfs
    batch = good_project.batch
    # ... test logic here
```

Tests not requiring PDFs:

```python
def test_cli_help():
    """This test doesn't need PDFs, so it runs everywhere."""
    # ... test logic here
```

---

## Related Documentation

- **Setup Guide**: [SYMLINK_SETUP.md](https://github.com/boscorat/bank-statement-data/blob/master/SYMLINK_SETUP.md) (in private repo)
- **Security**: [SECURITY.md](./SECURITY.md) (in this repo)
- **Contributing**: [CONTRIBUTING.md](./CONTRIBUTING.md) (if available)
- **Test Data Hub**: Private repo [bank-statement-data](https://github.com/boscorat/bank-statement-data)
