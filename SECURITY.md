# Security Policy

## Reporting Security Issues

**Do not open GitHub issues for security concerns.** Instead, please email [security contact] with details.

---

## Handling Sensitive Bank Statements

### The Problem

When reporting bugs or requesting new bank support, you may need to share your bank statement. However, **real (unanonymised) bank statements contain sensitive personal and financial information** that must never be stored in public repositories.

### The Solution

We ask users to **anonymise** statements before sharing them. An anonymised statement has:
- ✅ All readable text scrambled (names, descriptions)
- ✅ All numbers replaced (account numbers, sort codes, card numbers)
- ✅ Layout and structure preserved (so parser still works)
- ✅ No personal or financial information readable

### Where to Submit Anonymised Statements

You have two options:

#### Option 1: GitHub Issue (Easiest for Non-Technical Users)

1. **Anonymise your statement** using the anonymiser tool (see below)
2. **Open a GitHub issue** for your bug report or feature request
3. **Attach the anonymised PDF** directly in the issue form
4. **We'll review** and troubleshoot

**Note:** Anonymised PDFs attached to public issues are safe. The anonymiser ensures no sensitive data remains.

#### Option 2: Direct Submission to Private Test Data Repo (For Advanced Users)

If you want your anonymised statement to be included in our test suite for ongoing testing:

1. **Anonymise your statement** (see below)
2. **Create a fork** of the private test data repo: `github.com/boscorat/bank_statement_test_data`
3. **Add your anonymised PDF** to the appropriate folder
4. **Create a Pull Request** (don't worry if you're not familiar with PRs — see the guide below)
5. **We'll review** and merge if it's suitable for testing

---

## How to Anonymise Your Statement

### Using OpenStan (Easiest)

1. Open OpenStan
2. Go to the **Anonymise PDF** tool (Screens → Anonymise PDF)
3. Select your statement PDF
4. Click **Anonymise**
5. Review the output to ensure no personal info remains
6. Attach the anonymised version to your GitHub issue

### Using the CLI

```bash
bsp anonymise /path/to/your/statement.pdf -o anonymised_statement.pdf
```

### Using Python API

```python
from bank_statement_parser import anonymise_pdf

anonymise_pdf(
    input_path="my_statement.pdf",
    output_path="anonymised_statement.pdf"
)
```

---

## Verification Checklist

Before attaching **any** anonymised PDF, verify that:

- [ ] No readable names appear (all text should be scrambled)
- [ ] No readable account numbers (should be replaced with fake ones)
- [ ] No readable sort codes
- [ ] No readable card numbers
- [ ] No readable phone numbers or addresses
- [ ] Layout and page structure are intact
- [ ] All pages are processed (not blank or corrupted)

If you're unsure whether your anonymised PDF is safe to share, **do not attach it**. Contact the maintainers for guidance.

---

## What We Do With Submitted Statements

### GitHub Issue Attachments

- Used for **troubleshooting your specific issue only**
- Not automatically added to test suite
- Kept for **6 months** then deleted

### Test Data Repo Submissions

- Reviewed and verified by maintainers
- Used for **ongoing testing** of new features
- **Credited in documentation** (if you wish)
- Kept indefinitely (with your permission)

---

## Privacy & Security Guarantees

We take your privacy seriously:

1. **Anonymised PDFs are safe to share** — The anonymiser removes all sensitive data
2. **Private test data repo is private** — Not publicly accessible
3. **Audit trail** — All PDFs are tracked in git history with timestamps
4. **No redistribution** — We never share test data outside the core team
5. **Verification** — We manually verify each PDF before adding to test suite

---

## Questions?

If you have concerns about privacy or security:

- **General questions:** Open a GitHub Discussion
- **Security concerns:** Email [security contact]
- **Questions about your specific statement:** Comment on your GitHub issue

---

## Related Documentation

- [Anonymisation Guide](https://boscorat.github.io/bank_statement_parser/guides/anonymisation/)
- [Contributing Guide](CONTRIBUTING.md)
- [Bank Statement Test Data Repo](https://github.com/boscorat/bank_statement_test_data) (Private)
