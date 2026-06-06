# GitHub Discussions Setup Guide

GitHub Discussions has been enabled for bank_statement_parser to provide a searchable Q&A and ideas archive, reducing support load through an organized community forum.

## Steps to Enable GitHub Discussions

Follow these steps in your GitHub repository to set up the 4 recommended categories:

### Step 1: Enable Discussions on Your Repository

1. Go to **Settings** (top-right, under your profile dropdown)
2. In the left sidebar under "Code and automation," click **Discussions**
3. Toggle **"Enable discussions for this repository"** to ON
4. Click **Save** (if prompted)

### Step 2: Create Discussion Categories

Once Discussions are enabled, you'll see a "Categories" section. Click **"New category"** and create these 4 categories:

#### Category 1: **Questions**

- **Icon:** 💬 (Conversation)
- **Description:** "Get help using Bank Statement Parser — how-tos, troubleshooting, general guidance"
- **Is answerable:** YES (checkbox)
- **Reason:** Converts "How do I...?" GitHub Issues into a searchable archive. Users find answers without opening new issues.

**Example topics:**
- "How do I add a new bank?"
- "Why are my transactions missing?"
- "Can I use this with other PDF formats?"

---

#### Category 2: **Adding a Bank**

- **Icon:** 🏦 (Bank icon)
- **Description:** "Request support for a new bank or account type. Before posting, review CONTRIBUTING.md → Adding a New Bank"
- **Is answerable:** NO (checkbox unchecked)
- **Reason:** Banks requests get their own category so they're easily discoverable for contributors looking to help.

**Example topics:**
- "Interest in adding Santander support"
- "Would someone be able to support Nationwide savings accounts?"
- "Looking to contribute a Barclays credit card config"

---

#### Category 3: **Announcements**

- **Icon:** 📢 (Announcement)
- **Description:** "Releases, roadmap updates, and important notices from maintainers"
- **Is answerable:** NO (checkbox unchecked)
- **Reason:** One-way broadcast channel for release notes, security advisories, and planned downtime.

**Example topics:**
- "v0.3.0 released — new banks and performance improvements"
- "[SECURITY] Critical vulnerability fixed in v0.2.2"
- "Roadmap: Q3 2025 priorities"

---

#### Category 4: **Ideas**

- **Icon:** 💡 (Lightbulb)
- **Description:** "Feature requests and suggestions. Check VISION.md first to ensure your idea is in scope."
- **Is answerable:** NO (checkbox unchecked)
- **Reason:** Brainstorming and feature tracking separate from bugs. Helps identify out-of-scope requests early.

**Example topics:**
- "Should we add support for international banks?"
- "Idea: real-time balance monitoring"
- "Suggestion: desktop GUI application"

---

### Step 3: Pin an Announcement (Optional)

Once Discussions are set up, consider creating a pinned Announcement post:

1. Go to **Discussions** → **Announcements**
2. Click **"New discussion"**
3. **Title:** "Welcome to Bank Statement Parser Discussions!"
4. **Body:** Use the template below
5. Pin the discussion (⋮ menu → "Pin discussion")

**Template:**

```markdown
# Welcome to Bank Statement Parser Discussions! 👋

This is a community forum for questions, ideas, and bank requests.

## Where to Post

- **Questions?** 💬 Post in [Questions](/?category=questions) — "How do I...?"
- **Need a bank added?** 🏦 Post in [Adding a Bank](/?category=adding-a-bank) — Bank requests go here
- **Got an idea?** 💡 Post in [Ideas](/?category=ideas) — Feature suggestions and brainstorming
- **News?** 📢 [Announcements](/?category=announcements) — Only maintainers post here

## Before You Post

- ✅ Check [VISION.md](../VISION.md) — Scope boundaries (UK banks only, no crypto, no real-time feeds, etc.)
- ✅ Check existing discussions — Your question may already be answered
- ✅ For bugs, use [GitHub Issues](../issues) instead of Discussions

## Quick Links

- 📖 [Documentation](https://boscorat.github.io/bank_statement_parser/)
- 🐛 [Bug Reports](../issues)
- 🤝 [Contributing Guide](../CONTRIBUTING.md)
- 🔐 [Security Policy](../SECURITY.md)

Thank you for being part of the community! 🙌
```

---

## Post-Setup Checklist

Once you've created the categories:

- [ ] **Update README.md** to link to Discussions in the "Getting Help" or "Contributing" section (e.g., `- **Questions?** Open a [GitHub Discussion](https://github.com/boscorat/bank_statement_parser/discussions)`)
- [ ] **Create a pinned Announcement** with welcome message (optional but recommended)
- [ ] **Add Discussions link to CONTRIBUTING.md** in "Getting Help" section if not already present
- [ ] **Redirect issue reporters** — When issues arrive that are better suited to Discussions, politely redirect them:
  - "This looks like a how-to question. Please post in Discussions → Questions instead so others can find the answer!"
  - "Feature request? Please check VISION.md first, then post in Discussions → Ideas"

---

## What to Expect

Once Discussions are live:

**Week 1–2:** Quiet (you're still onboarding users)

**Month 1+:** You'll start seeing:
- Repeated questions → Answer once, link future askers to the archived discussion
- Bank requests → Easier to track and prioritize for community contributions
- Feature ideas → Early signal of what users actually want vs. what you anticipated

**Long-term benefit:** ~50% reduction in duplicate issues, searchable archive of solutions, natural place for asynchronous help.

---

## GitHub Discussions vs. GitHub Issues

| Need | Use |
|---|---|
| Report a bug | GitHub Issues |
| Ask how to do something | GitHub Discussions → Questions |
| Request a new bank | GitHub Discussions → Adding a Bank |
| Feature request | GitHub Discussions → Ideas |
| Security vulnerability | Email (see SECURITY.md) |
| Announce a release | GitHub Discussions → Announcements |

---

## Troubleshooting

**"Discussions option is grayed out"**
- Make sure you're a repository owner/admin
- Discussions are only available on public repos
- Try refreshing your browser

**"I can't see the Categories section"**
- You may need to create a Discussions post first (click "New discussion" at least once)
- Then go back to Settings → Discussions to see Categories

**"How do I enable notification for new Discussions?"**
- Users can watch the repo (Settings → Notifications) to get pinged on new Discussions
- You can set your own notification preferences in Settings

---

## Next Steps

Once Discussions are set up, consider:
1. **Announce on social media** that Q&A has moved from Issues to Discussions
2. **Close old Q&A issues** with a kind redirect: "Thanks for the question! I'm moving our Q&A to Discussions for better organization. Please repost your question [here](link) so others can find the answer."
3. **Monitor new Discussions** for the first month to establish culture and encourage participation
4. **Create wiki-style Discussions** for frequently asked questions (pin them in each category)

---

That's it! Let me know if you have any questions. 🚀
