# Release Process

This document describes the two-stage release process for bank-statement-parser.

## Overview

Releases follow a **draft-then-promote** workflow:

1. **Stage 1 (Automatic)**: Push a version tag → builds binaries → creates a **draft** GitHub Release
2. **Stage 2 (Manual)**: Review artifacts → promote to live → publish to PyPI → deploy docs

This allows maintainers to verify all artifacts are correct before making a release public.

---

## Stage 1: Create a Draft Release

### Prerequisites

- Version is bumped in `pyproject.toml`
- All changes are committed and pushed to a branch
- The branch is ready for release

### Steps

1. **Update version in `pyproject.toml`**:
   ```toml
   [project]
   version = "1.0.0"
   ```

2. **Create an annotated tag** and push it:
   ```bash
   git tag -a v1.0.0 -m "Release 1.0.0"
   git push origin v1.0.0
   ```

3. **GitHub Actions triggers automatically**:
   - `release.yml` workflow starts
   - Builds Python distributions (wheel + sdist)
   - Builds system packages (.deb + .rpm)
   - Creates a **draft** GitHub Release with all artifacts
   - Does **NOT** publish to PyPI
   - Does **NOT** deploy docs

4. **Monitor the workflow**:
   - Go to **Actions** tab in GitHub
   - Click **Release** workflow
   - Watch for job completion (~15-20 minutes)

### Verify the Draft Release

1. Go to **Releases** page on GitHub
2. Look for a **Draft** label on the new release (appears at top of page)
3. Download and inspect artifacts:
   - `bank_statement_parser-*.whl` (wheel distribution)
   - `bank_statement_parser-*.tar.gz` (source distribution)
   - `uk-bank-statement-parser_*.deb` (Debian package)
   - `uk-bank-statement-parser-*.rpm` (RPM package)

4. Verify release notes auto-generated from PR titles

5. **If something is wrong**:
   - Delete the draft release (GitHub UI)
   - Delete the local tag: `git tag -d v1.0.0`
   - Delete the remote tag: `git push origin --delete v1.0.0`
   - Fix the issue
   - Re-create and push the tag (workflow will re-trigger)

---

## Stage 2: Promote to Live

Once artifacts are verified, promote the release to live. This:

1. Publishes wheel + sdist to PyPI
2. Verifies package appears on PyPI
3. Creates a GitHub Deployment record
4. Un-drafts the GitHub Release (makes it visible to users)
5. Generates and deploys versioned docs to GitHub Pages

### Promote via GitHub CLI

```bash
gh workflow run promote-release.yml -f tag=v1.0.0
```

This triggers the `promote-release.yml` workflow for the specified tag.

### Promote via GitHub UI

1. Go to **Actions** → **Promote Release** workflow
2. Click **Run workflow**
3. Enter the tag (e.g., `v1.0.0`)
4. Click **Run workflow**

### Monitor Promotion

1. Go to **Actions** → **Promote Release**
2. Click the workflow run
3. Watch for job completion (~10-15 minutes)

### Verify Promotion

After promotion completes:

1. **GitHub Release**:
   - Navigate to **Releases** page
   - Draft label should be gone
   - Release is now **live** and visible to all users

2. **PyPI**:
   - Visit `https://pypi.org/project/bank-statement-parser/`
   - New version should appear in release history
   - Package can be installed: `pip install bank-statement-parser==1.0.0`

3. **GitHub Deployments** (tracking):
   - Go to **Deployments** page
   - New production deployment should show version
   - Status should be **success**

4. **Documentation**:
   - Visit GitHub Pages docs site
   - New version should be listed in version selector
   - Latest alias should point to new version

---

## Rollback After Promotion

If you promoted a release but need to undo:

1. **Undo PyPI publication** (contact PyPI support):
   - PyPI does not allow yanking recent packages via the UI normally
   - Use `pip-audit` or mark as yanked manually
   - See [PEP 592 - Yanked Releases](https://www.python.org/dev/peps/pep-0592/)

2. **Re-draft on GitHub**:
   ```bash
   gh release edit v1.0.0 --draft
   ```

3. **Delete the Git tag** (if you want to re-release):
   ```bash
   git tag -d v1.0.0
   git push origin --delete v1.0.0
   ```

---

## Troubleshooting

### Release workflow fails during build

**Problem**: `pypi-publish` job fails

**Solution**:
- Check job logs for error (usually version mismatch or build failure)
- Fix the issue in code
- Delete the tag and re-create it
- Push again

### Draft release exists but promote workflow won't start

**Problem**: `promote-release.yml` fails at "Check draft release exists"

**Solution**:
- Verify the tag exists: `git tag -l v1.0.0`
- Verify the GitHub Release exists on the Releases page
- If not found, delete everything and re-tag

### Promote workflow fails at PyPI verification

**Problem**: Package doesn't appear on PyPI after 30 seconds

**Solution**:
- PyPI indexing can take longer
- Check `https://pypi.org/project/bank-statement-parser/` manually
- If present, the promote workflow actually succeeded; re-run to verify
- If missing, check PyPI API: `curl https://pypi.org/pypi/bank-statement-parser/1.0.0/json`

### Docs deployment fails

**Problem**: `deploy-docs` job in promote workflow fails

**Solution**:
- This is non-blocking; release was already promoted to live
- Fix docs issues and re-run the promote workflow (or manually deploy)
- See mkdocs/mike documentation if needed

---

## One-time Setup

The following setup is required once per repository:

### PyPI Trusted Publisher

1. Go to **Settings** → **Environments** → **New environment**
2. Create environment named `pypi`
3. Register a Trusted Publisher on PyPI:
   - Log in to PyPI account
   - Go to Account → Publishing (or https://pypi.org/manage/account/publishing/)
   - Add a new pending publisher:
     - **GitHub Repository**: `boscorat/bank_statement_parser`
     - **Workflow**: `promote-release.yml` (used for promotion only)
     - **Environment**: `pypi`

### GitHub Pages

1. Go to **Settings** → **Pages**
2. Set **Source** to `gh-pages` branch
3. Ensure deploy from `gh-pages` is enabled

### Auto-generate Release Notes

1. Go to **Settings** → **General**
2. Under **Features**, enable **Auto-generate release notes**
3. (Optional) Customize release notes template in **Settings** → **Code & automation** → **Probot**

---

## Version Numbering

This project follows **Semantic Versioning** (SemVer): `MAJOR.MINOR.PATCH`

- **MAJOR**: Breaking API changes
- **MINOR**: New features (backward-compatible)
- **PATCH**: Bug fixes

Examples: `1.0.0`, `1.1.0`, `1.1.1`, `2.0.0-rc.1`

See [semver.org](https://semver.org/) for details.

---

## Release Checklist

Before tagging a release:

- [ ] All tests pass locally: `pytest`
- [ ] Linting passes: `ruff check .`
- [ ] Formatting is correct: `ruff format .`
- [ ] Version is updated in `pyproject.toml`
- [ ] No uncommitted changes: `git status`
- [ ] Main branch is up-to-date: `git pull origin main`
- [ ] CHANGELOG or release notes are up-to-date (if applicable)

After promotion:

- [ ] Release is live on GitHub
- [ ] Package is available on PyPI
- [ ] Docs are deployed to GitHub Pages
- [ ] Announcement posted (if desired)

---

## Related Files

- **`.github/workflows/release.yml`** — Triggered on tag push; builds binaries and creates draft
- **`.github/workflows/promote-release.yml`** — Manual trigger; promotes to live and publishes to PyPI
- **`pyproject.toml`** — Contains version number (must match tag)
- **`.github/workflows/ci.yml`** — Runs tests on every push (ensure passing before release)

---

## See Also

- [git-workflows skill](../AGENTS.md#git-workflows) — Release versioning and tagging patterns
- [Semantic Versioning](https://semver.org/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [PyPI Documentation](https://pypi.org/)
