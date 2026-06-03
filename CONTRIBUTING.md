# Contributing Guide

Thank you for your interest in contributing to bank_statement_parser!

## Ways to Contribute

### 1. Report Bugs

Found a parsing error? Report it:

1. **Open a GitHub issue** using the "Parsing issue" template
2. **Describe the error** with as much detail as possible
3. **(Optional) Attach an anonymised statement** to help us reproduce the issue

→ See [SECURITY.md](SECURITY.md) for how to anonymise and safely share statements.

### 2. Request New Bank Support

Want us to support your bank?

1. **Open a GitHub issue** using the "New bank request" template
2. **Provide bank details** (name, account types)
3. **(Recommended) Attach an anonymised statement sample** to speed up development

→ See [SECURITY.md](SECURITY.md) for how to anonymise and safely share statements.

### 3. Contribute Test Data

Help us test more bank statement formats!

If your anonymised statement might be useful for ongoing testing:

1. **Anonymise your statement** (see [SECURITY.md](SECURITY.md))
2. **Fork the test data repo:** [bank_statement_test_data](https://github.com/boscorat/bank_statement_test_data) (requires access request)
3. **Add your anonymised PDF** to the appropriate bank folder
4. **Create a Pull Request** with a clear description
5. **We'll review and merge** if it's suitable for testing

**Don't worry if you're not familiar with GitHub/Git.** See the **Non-Technical Guide** below.

### 4. Code Contributions

Interested in fixing bugs or adding features?

- Fork the repo
- Create a feature branch: `git checkout -b feature/my-feature`
- Make your changes with tests
- Submit a Pull Request with a clear description

We follow standard Git/GitHub workflows. See [Development Setup](docs/development.md) for details.

---

## Non-Technical Guide: How to Contribute an Anonymised Statement

**No experience with GitHub or Git?** Don't worry! Here's a step-by-step guide:

### Step 1: Get Access to the Test Data Repo

The test data repo is **private** for security reasons. To contribute test PDFs:

1. Email us at [maintainer email] with subject: "Request access to test data repo"
2. We'll send you an invitation
3. Click the invitation link to accept access

### Step 2: Create a Fork (Copy the Repo)

A "fork" is your own copy of the repo on GitHub:

1. Go to [bank_statement_test_data](https://github.com/boscorat/bank_statement_test_data)
2. Click the **Fork** button (top-right)
3. GitHub will create a copy under your account

### Step 3: Add Your Anonymised PDF

Using GitHub's web interface (no command line needed):

1. In your fork, navigate to `pdfs/good/[BANK]/` (create the folder if needed)
2. Click **Add file** → **Upload files**
3. Drag and drop your anonymised PDF
4. GitHub will show you a preview
5. Scroll down and click **Commit changes**
6. Add a message like: "Add anonymised HSBC UK current account from January 2024"

### Step 4: Create a Pull Request

A Pull Request asks the maintainers to review and merge your changes:

1. In your fork, click **Contribute** → **Open pull request**
2. Add a description:
   ```
   Title: Add anonymised [BANK] [TYPE] statement for testing
   
   Description:
   - Bank: [Bank name]
   - Account type: [e.g., Current, Savings, Credit Card]
   - Date: [Month/Year]
   - Notes: [Any relevant notes about this statement]
   ```
3. Click **Create pull request**
4. That's it! We'll review and merge

### Step 5: What Happens Next?

- **We review** your anonymised PDF
- **We verify** no sensitive data remains
- **We merge** if it looks good
- **We credit you** in documentation (if you want)
- Your statement becomes part of the test suite

---

## Code of Conduct

- Be respectful and helpful to others
- No harassment or discrimination
- Constructive feedback only

---

## Questions?

- **GitHub Discussions:** For general questions and ideas
- **GitHub Issues:** For bugs and feature requests
- **Email:** For security concerns (see [SECURITY.md](SECURITY.md))

---

## License

By contributing, you agree that your contribution will be licensed under the project's license (MIT).

---

Thank you for helping make bank_statement_parser better! 🙏
