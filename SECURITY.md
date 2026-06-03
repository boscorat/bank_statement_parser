# Security & Privacy Guidelines

This project uses test data from bank statements to validate parsing and anonymisation functionality. We maintain strict security practices to protect user data and prevent accidental commits of sensitive information.

## Test Data Security

### Synthetic vs. Anonymised Test PDFs

This repository uses two types of test PDFs:

1. **Synthetic PDFs** (safe to commit)
   - Generated programmatically with fake data
   - No real customer information
   - Located in `tests/fixtures/pdfs/synthetic/`
   - Used in public CI/CD workflows
   - Fast and deterministic

2. **Anonymised PDFs** (private, fetched during CI)
   - Real bank statements with sensitive information removed
   - Stored in private repo (`bank-statement-data`)
   - Fetched automatically during CI testing (if SSH key available)
   - Never committed to public repositories
   - Automatically cleaned up after tests
   - Provides real-world format validation

### PDF Security Rules

❌ **Never commit**:
- Unanonymised PDFs (with real customer names, account numbers, etc.)
- PDFs with identifiable information
- PDFs from `tests/fixtures/pdfs/good/` or `pdfs/bad/` in public repos
- Files from private `bank-statement-data` repository

✅ **Safe to commit**:
- Synthetic PDFs in `tests/fixtures/pdfs/synthetic/`
- Test code and configuration
- Documentation

### Preventing Accidental PDF Commits

This repository includes protective measures:

1. **`.gitignore` rules**:
   ```gitignore
   # Block all PDFs by default
   *.pdf
   
   # Exception: Allow synthetic PDFs only
   !tests/fixtures/pdfs/synthetic/
   !tests/fixtures/pdfs/synthetic/**/*.pdf
   ```

2. **CI Cleanup**:
   - Anonymised PDFs are fetched to temporary locations (`/tmp`)
   - Automatically cleaned up after tests complete
   - Never persisted in checked-out code

3. **Git Protection**:
   - Pre-commit hooks can be configured to block PDF commits
   - Repository administrators review all contributions

## Credential & Secret Handling

### GitHub Secrets Used

This project uses GitHub Secrets for sensitive configuration:

- `SSH_PRIVATE_KEY_TEST_DATA` - SSH key for cloning private `bank-statement-data` repo (CI only)
  - Auto-masked in logs
  - Automatically deleted after use
  - Only available to administrators

### Best Practices

✅ **Do**:
- Use GitHub Secrets for sensitive configuration
- Rotate credentials regularly
- Review CI/CD logs for accidental exposure

❌ **Never**:
- Hardcode credentials in source code
- Commit `.env` files with secrets
- Share SSH private keys
- Include tokens in repository URLs

## CI/CD Security

### Anonymised PDF Fetch (CI Only)

During CI testing, the workflow optionally fetches anonymised PDFs from the private repo:

1. **Conditional fetch**: Only runs if `SSH_PRIVATE_KEY_TEST_DATA` secret is configured
2. **Graceful fallback**: Tests continue with synthetic PDFs if fetch fails
3. **Automatic cleanup**: PDFs are deleted after tests complete
4. **Private logs**: GitHub Actions logs are visible only to administrators

Example workflow step:
```yaml
- name: Fetch anonymised PDFs for testing (graceful fallback)
  continue-on-error: true
  if: secrets.SSH_PRIVATE_KEY_TEST_DATA != ''
  run: |
    # Clone with temporary SSH key
    git clone --depth 1 --filter=blob:none --sparse \
      git@github.com:boscorat/bank-statement-data.git /tmp/test_pdfs
    
    # Copy to tests/fixtures/pdfs
    cp -r /tmp/test_pdfs/pdfs/* tests/fixtures/pdfs/
    
    # Cleanup
    rm -rf /tmp/test_pdfs /root/.ssh/bank_statement_deploy_key

- name: Clean up anonymised PDFs after tests (security)
  if: always()
  run: |
    # Remove anonymised PDFs to prevent accidental commits
    rm -rf tests/fixtures/pdfs/good tests/fixtures/pdfs/bad
```

### Log Review

GitHub Actions logs are private and visible only to repository administrators. To audit logs for security:

1. Go to Actions → Select workflow run → View logs
2. Check for:
   - ❌ Unmasked SSH keys (should see `***`)
   - ❌ Exposed file paths or repository URLs
   - ❌ Error messages revealing sensitive data

## User Data & Submissions

### Using Private Test Data

If you have access to the private `bank-statement-data` repository:

1. **Setup local testing** (optional, for contributors with access):
   ```bash
   git clone git@github.com:boscorat/bank-statement-data.git ../bank-statement-data
   ln -s ../bank-statement-data/pdfs tests/fixtures/pdfs/anonymised
   ```

2. **Run tests with anonymised PDFs**:
   ```bash
   # All tests (including anonymised)
   python test_runner.py all
   
   # Only anonymised PDFs (if available)
   python test_runner.py anonymised
   
   # Only synthetic PDFs (public/safe)
   python test_runner.py synthetic
   ```

3. **Results are private** - Don't commit the cloned PDFs

### Reporting Issues with Real Data

If you discover parsing issues with real bank statements:

1. **Anonymise the statement** using guidelines from private repo
2. **Submit via GitHub Issue** with anonymised attachment
3. **Or** create a PR to the private repo with anonymised PDF
4. Project maintainers will verify and add to test suite

See the private repo's [`ANONYMISATION_CHECKLIST.md`](https://github.com/boscorat/bank-statement-data/blob/master/ANONYMISATION_CHECKLIST.md) for anonymisation guidelines.

## Incident Response

### If Sensitive Data is Accidentally Committed

1. **Do not push** if you haven't pushed yet
2. **Amend the commit**:
   ```bash
   # Remove the sensitive file
   git rm --cached <sensitive_file>
   echo "<sensitive_file>" >> .gitignore
   git add .gitignore
   git commit --amend --no-edit
   ```

3. **If already pushed**:
   - Contact repository maintainers immediately
   - Use `BFG` or `git-filter-repo` to remove from history
   - Force push is required (uses `--force`)

4. **For leaked credentials**:
   - Revoke immediately
   - Rotate secrets
   - Notify all maintainers

## Questions?

- **Test data questions** → See [`TESTING.md`](TESTING.md)
- **PDF anonymisation** → See private repo: [`bank-statement-data/ANONYMISATION_CHECKLIST.md`](https://github.com/boscorat/bank-statement-data)
- **Security concerns** → Contact maintainers directly (not via Issues)
- **General contribution** → See [`CONTRIBUTING.md`](CONTRIBUTING.md)

## Summary

✅ **Safe practices**:
- Use synthetic PDFs for testing
- `.gitignore` protects against accidental commits
- CI automatically cleans up anonymised PDFs
- GitHub Secrets protect credentials

❌ **Never**:
- Commit unanonymised PDFs
- Hardcode credentials
- Skip anonymisation when submitting real data
- Share SSH keys

🛡️ **Security-first approach**:
- Multi-layer protection (`.gitignore` + CI cleanup + pre-commit checks)
- Audit trail and transparency
- Private repo for sensitive data
- Graceful fallback to synthetic PDFs
