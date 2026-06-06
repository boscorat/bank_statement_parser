# Burnout Prevention & Maintenance Metrics

Bank Statement Parser is a solo-maintained project. Tracking key metrics quarterly helps identify burnout signals early and maintain sustainable development.

---

## Monthly Tracking Sheet

Copy this template into a Google Sheet or Excel workbook and fill in the first of each month. Takes ~5 minutes.

| Metric | Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec |
|--------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|
| **Issues opened** | — | — | — | — | — | — | — | — | — | — | — | — |
| **Issues closed** | — | — | — | — | — | — | — | — | — | — | — | — |
| **PRs opened** | — | — | — | — | — | — | — | — | — | — | — | — |
| **PRs merged** | — | — | — | — | — | — | — | — | — | — | — | — |
| **Avg response time (days)** | — | — | — | — | — | — | — | — | — | — | — | — |
| **Maintenance hours (per week)** | — | — | — | — | — | — | — | — | — | — | — | — |
| **Vacation/off-season weeks** | — | — | — | — | — | — | — | — | — | — | — | — |
| **Notes** | | | | | | | | | | | | |

---

## Quarterly Review (Every 3 Months)

Set a recurring calendar reminder for the first day of every quarter (Jan 1, Apr 1, Jul 1, Oct 1).

Spend **15 minutes** reviewing the monthly metrics and filling in:

### Q[#] [YEAR] Review

**Period:** [Month 1–3]

**Summary Metrics:**
- Total issues opened: `___`
- Total issues closed: `___`
- Average response time: `___ days`
- Total maintenance hours (quarter): `___ hours` (~`___` hrs/week)
- Release cadence: `___ releases in quarter`
- Backlog size (open issues): `___ issues`

**Burnout Score** (0–100, where 100 = critical burnout)

| Factor | Score | Notes |
|--------|-------|-------|
| **Response time creep** | `__/20` | Are issues taking longer to triage? |
| **Backlog growth** | `__/20` | Is the backlog shrinking or growing? |
| **PR merge time** | `__/15` | Are PRs stuck in review? |
| **Feature fatigue** | `__/15` | Frustrated by scope creep or out-of-scope requests? |
| **Maintenance burden** | `__/15` | Hours per week sustainable? >12 hrs = 🚨 |
| **Release frequency** | `__/15` | Are releases on pace or falling behind? |

**Total Burnout Score:** `___/100`

---

## Burnout Score Interpretation

| Score | Status | Action |
|-------|--------|--------|
| 0–20 | ✅ Healthy | Continue current pace. Sustainable. |
| 21–40 | ⚠️ Caution | Minor stress signals. Slight increase in response times or backlog. Monitor closely next quarter. |
| 41–60 | 🟠 At Risk | Noticeable burnout signals. Consider: deprioritising features, closing out-of-scope issues, finding a co-maintainer. |
| 61–80 | 🔴 Critical | Serious burnout risk. Take immediate action: freeze new feature requests, focus on critical bugs only, recruit help. |
| 81–100 | 🚨 Emergency | Severe burnout. Consider: temporary hiatus, major scope reduction, or stepping back from solo maintenance. |

---

## Quarterly Action Plan

After calculating your burnout score, decide on actions for the next quarter:

### If Score < 40 (Healthy)
- ✅ Continue current practices
- ✅ Consider one new initiative (e.g., public launch, blog post)

### If Score 40–60 (At Risk)
- ⚠️ Reduce meeting/admin time
- ⚠️ Set stricter response SLAs (extend to 14 days if at 10 now)
- ⚠️ Close out-of-scope issues with a form response
- ⚠️ Deprioritise non-critical features
- ⚠️ Consider: Do you want a co-maintainer?

### If Score > 60 (Critical)
- 🔴 **Freeze new feature requests** — Only security fixes and critical bugs
- 🔴 **Go silent** — Take a 1-week complete break if possible
- 🔴 **Recruit help** — Find a co-maintainer or hand off to community
- 🔴 **Reduce scope** — Are there features you can officially deprecate?

---

## Example: Q1 2025 Review

**Period:** Jan 1 – Mar 31, 2025

**Summary Metrics:**
- Total issues opened: 12
- Total issues closed: 14
- Average response time: 6 days
- Total maintenance hours (quarter): 96 hours (~8 hrs/week)
- Release cadence: 2 releases (v0.2.1, v0.3.0)
- Backlog size (open issues): 3 issues

**Burnout Score Breakdown:**

| Factor | Score | Notes |
|--------|-------|-------|
| **Response time creep** | 2/20 | Avg 6 days, well under 10-day SLA ✅ |
| **Backlog growth** | 3/20 | Only 3 open issues; well managed ✅ |
| **PR merge time** | 2/15 | PRs merged within 2 weeks ✅ |
| **Feature fatigue** | 5/15 | One out-of-scope request (crypto); politely declined |
| **Maintenance burden** | 4/15 | 8 hrs/week is sustainable ✅ |
| **Release frequency** | 2/15 | 2 releases in 3 months is good pace ✅ |

**Total Burnout Score: 18/100** ✅ Healthy

**Action Plan for Q2:** Green light for public launch announcement.

---

## Quarterly Checklist

Before your Q[#] review, verify:

- [ ] Monthly tracking sheet is filled in (12 data points)
- [ ] All issues/PRs are labeled (bug, feature, question, enhancement, etc.)
- [ ] Backlog reviewed and cleaned up (close old, stale issues)
- [ ] Response time calculated (average days from issue open → first response)
- [ ] Maintenance hours self-reported (track in a simple log: "2h on issue #42, 1.5h on PR review")
- [ ] One release completed or in progress
- [ ] Vacation weeks noted

---

## Simple Maintenance Log Template

To make tracking maintenance hours easier, keep a simple log like this:

```
Daily Maintenance Log (copy into your notes app or GitHub project)

**Week of Jan 13, 2025:**
- Mon (1/13): 2h triaging issues #45-47; 1h on PR review
- Tue (1/14): 1h debugging issue #48
- Wed (1/15): Off (sick day)
- Thu (1/16): 3h implementing fix for issue #44
- Fri (1/17): 2h release prep for v0.3.0
- Weekend: Off
Total: 9 hours

**Week of Jan 20, 2025:**
[...]
```

At month-end, sum the hours and note them in your tracking sheet.

---

## When to Take Action

Don't wait for the quarterly review if you notice:

- **Response times consistently > 14 days** → Issue an urgent notice to users; temporarily close Issues
- **Maintenance > 15 hrs/week for 2+ weeks** → You're overextending; deprioritise next sprint
- **3+ months without a release** → Backlog is blocking; set a release date and stick to it
- **You're dreading GitHub notifications** → 🚨 Burnout signal; take a week off immediately
- **New issues trigger frustration before reading** → 🚨 Critical signal; step back and reassess

---

## Resources

- **Burnout case studies:** [Event-stream incident](https://github.com/dominictarr/event-stream/issues/116), [Babel deprecation notice](https://babeljs.io/docs/en/v7-migration.html)
- **Sustainable open-source:** [Burnout Prevention in Open Source](https://github.com/pul-ses/sustainability) (GitHub collection)
- **Finding co-maintainers:** [Succession Planning Guide](https://opensource.guide/leadership-and-governance/)

---

## Summary

A solo open-source maintainer is sustainable **only if you track metrics**. Burnout creeps in silently — by the time you notice, it's often too late.

**Quarterly burnout reviews take 15 minutes and can prevent months of suffering.**

Print this guide. Set calendar reminders. Review every 3 months.

You've got this. 🚀
