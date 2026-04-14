# Docs Writing Guide

## Branch

All documentation work happens on the **`docs`** branch.

For each new feature or integration, branch off `docs` using the feature name:

```bash
git checkout docs
git checkout -b docs/<feature-name>   # e.g. docs/semantic-layer
```

When the page is ready, open a PR from `docs/<feature-name>` back into `docs`.

Also update the integration page setup template — screenshots are only needed for UI-heavy steps, not for code or YAML configuration steps.

---

## Writing Style

- **Lead with impact, not process.** State what the feature does and why it matters before explaining how.
- **No storytelling.** Avoid preamble like "In today's data landscape..." or "As your team grows...". Get to the point.
- **Concise sentences.** One idea per sentence. If you're using "and" to join two actions, consider splitting into two bullets.
- **Active voice.** "Elementary sends an alert" not "An alert is sent by Elementary."
- **Consistent terminology.** Use the exact product terms: *monitors*, *incidents*, *[alert rules](https://docs.elementary-data.com/cloud/features/alerts-and-incidents/alert-rules)*, *environments*, *Ella*, *Elementary Cloud Platform*.
- **Steps use numbered lists.** Configuration and setup always uses numbered steps. Concept explanations use prose or bullets.
- **Headers are sentence case.** `## Enabling Slack alerts` not `## Enabling Slack Alerts`.
- **No internal content.** Do not reference or expose internal commands, macros, file paths, or tooling that users don't have access to.
- **No obvious sections.** Don't add sections that describe what users can already figure out themselves — e.g. "Navigating [feature]" that just restates the UI. If it's self-evident from the product, leave it out.
- **Links to other docs pages must be relative.** Use `/cloud/features/alerts-and-incidents/alert-rules`, not `https://docs.elementary-data.com/cloud/features/alerts-and-incidents/alert-rules`. Relative links work in local preview and won't break if the domain changes.

---

## Feature and Integration Page Structure

### Feature page

```
---
title: "Feature Name"
sidebarTitle: "Short name" # only if title is long
icon: "icon-name"          # Lucide/FontAwesome icon slug
---

One sentence: what this feature does and why it matters.

<Frame caption="Caption text">
  <img src="..." alt="..." />
</Frame>

## How it works   ← or "Overview" for simpler features

Explain the mechanism, not just what buttons to click.

## Key capabilities  ← use for feature-rich pages

### Capability name
...

## Setup / Enabling [Feature]   ← only if setup is required

Numbered steps.

## [Edge cases / advanced config]   ← as needed
```

### Integration page

```
---
title: "Tool Name"
---

One sentence: what this integration enables.

<Frame caption="Caption text">
  <img src="..." alt="..." />
</Frame>

## Enabling [Integration name]

Numbered setup steps. Include a screenshot after any step where the user must navigate a non-obvious UI flow or complete a form. No screenshots needed for code or YAML configuration steps. Cover every step — don't skip prerequisites or assume prior context.

## [Capabilities or options this integration unlocks]

## Troubleshooting   ← only if there are known gotchas
```

---

## Docs and AI Agents

Elementary's docs are consumed by two AI layers — make sure your content is agent-friendly:

### Kapa widget (in-docs support)
An AI support bot is embedded in the docs. It uses the full docs as its knowledge base.
- Write clear `<Note>` and `<Warning>` callouts — Kapa surfaces these in answers.
- Avoid burying important constraints in the middle of paragraphs. Put them in callouts.
- Each page should be self-contained enough that Kapa can answer questions from it without needing to follow links.

### Elementary MCP server
The [MCP server](https://docs.elementary-data.com/cloud/mcp/intro) exposes Elementary's data to AI agents (Claude, Cursor, etc.) via tools. Users will ask their AI assistants questions that the MCP tools answer, and the AI may reference docs to explain results.
- When documenting [MCP tools](https://docs.elementary-data.com/cloud/mcp/mcp-tools), describe each tool's purpose and expected output precisely.
- If a feature is accessible via MCP, note it on the feature page.

### AI agents
Elementary's [AI agents](https://docs.elementary-data.com/cloud/ai-agents/overview) ([governance](https://docs.elementary-data.com/cloud/ai-agents/governance-agent), [triage](https://docs.elementary-data.com/cloud/ai-agents/triage-resolution-agent), [test recommendation](https://docs.elementary-data.com/cloud/ai-agents/test-recommendation-agent), etc.) are documented individually. When writing or updating these pages:
- Describe what the agent *decides* and *acts on*, not just what it shows.
- Include the trigger / entry point (which screen, which action).
- Mention what the agent needs to work well (e.g. metadata coverage, policies set up).

---

## Adding a New Feature or Integration — All Places to Update

When shipping a new feature or integration, update **every** location below. Missing one breaks navigation or leaves the feature undiscoverable.

### 1. Create the page
Place it in the correct path:
- Feature: `docs/cloud/features/<group>/<feature-name>.mdx`
- Integration: `docs/cloud/integrations/<category>/<tool-name>.mdx`

### 2. `docs/docs.json` — navigation
Add the page path to the correct group in the `navigation.tabs` array. If the feature belongs to a new group, add the group too.

```json
{
  "group": "Group Name",
  "pages": [
    "cloud/features/group/existing-page",
    "cloud/features/group/your-new-page"
  ]
}
```

### 3. Features overview page
The [features hub](https://docs.elementary-data.com/cloud/features) (`cloud/features.mdx`) is powered by a snippet. Add a card for the new feature in `snippets/cloud/features.mdx`.

### 4. Integrations hub
The [integrations hub](https://docs.elementary-data.com/cloud/integrations/elementary-integrations) (`cloud/integrations/elementary-integrations.mdx`) is powered by a snippet. Add a card for the new integration in `snippets/cloud/integrations/cards-groups/cloud-integrations-cards.mdx`.

### 5. Integration surface list
If it's a new integration, also add it to the [integrations feature page](https://docs.elementary-data.com/cloud/features/integrations) (`cloud/features/integrations.mdx`).

### Checklist

- [ ] Page created at the right path
- [ ] Added to `docs.json` navigation
- [ ] Added to features or integrations hub snippet
- [ ] Screenshots included (see below)
- [ ] Mintlify checks pass (see below)

---

## Reusable Snippets

If the same content appears on more than one page — an intro paragraph, a card group, a callout about prerequisites — extract it into a snippet instead of duplicating it.

Snippets live in `docs/snippets/`. Mirror the structure of the page that uses them:
- `snippets/cloud/features/anomaly-detection/automated-monitors-intro.mdx`
- `snippets/cloud/integrations/cards-groups/cloud-integrations-cards.mdx`

**Creating a snippet**

Write it as a plain `.mdx` file with no frontmatter:

```mdx
Elementary automatically monitors freshness and volume for all sources in your dbt project.
No configuration required — monitors are created out of the box.
```

**Using a snippet**

Import it at the top of the page, then use it as a component:

```mdx
import AutomatedMonitorsIntro from '/snippets/cloud/features/anomaly-detection/automated-monitors-intro.mdx';

<AutomatedMonitorsIntro />
```

**When to use snippets**

| Use a snippet | Don't use a snippet |
|---|---|
| Same intro/description on multiple pages | Content that only exists in one place |
| Card groups that appear in both a hub page and a feature page | One-off callouts or notes |
| Repeated prerequisite or setup steps | Full page content |

**Updating shared content**

Edit the snippet file — all pages that import it update automatically. Never copy-paste snippet content directly into a page.

---

## Images and Screenshots

All images must be wrapped in a `<Frame>` with a caption. No exceptions.

```mdx
<Frame caption="Connect Tableau to Elementary environment">
  <img
    src="/pics/my-feature-screenshot.png"
    alt="Connect Tableau to Elementary environment"
  />
</Frame>
```

- **Store images as local files** under `docs/pics/`. Do not use external image hosting for new screenshots.
- The `caption` and `alt` text should be identical and describe what the image shows.
- Prefer PNG for UI screenshots. Use GIF only for interactions that require animation.
- Name files descriptively: `slack-alert-channels.png`, not `screenshot1.png`.

---

## Mintlify Design and Components

Use Mintlify's built-in components to improve scannability. Don't write walls of text where a component communicates better.

### [Callouts](https://mintlify.com/docs/content/components/callouts)

Use for information that would get missed in prose:

```mdx
<Note>Use this for non-obvious but non-critical context.</Note>
<Warning>Use this for actions that can't be undone or have side effects.</Warning>
<Info>Use this for prerequisites or environment-specific behavior.</Info>
<Tip>Use this for shortcuts or recommended practices.</Tip>
```

### [Cards](https://mintlify.com/docs/content/components/cards)

Use for feature/integration hubs and navigational pages:

```mdx
<CardGroup cols={2}>
  <Card title="Feature name" icon="icon-slug" href="/path/to/page">
    One sentence describing what it does.
  </Card>
</CardGroup>
```

### [Steps](https://mintlify.com/docs/content/components/steps)

Use instead of numbered lists for multi-step setup flows:

```mdx
<Steps>
  <Step title="Create a token">
    Go to Settings and generate a personal access token.
  </Step>
  <Step title="Connect to Elementary">
    Navigate to Environments and paste the token.
  </Step>
</Steps>
```

### [Tabs](https://mintlify.com/docs/content/components/tabs)

Use when the same workflow differs by platform (e.g. Cloud vs Server, Snowflake vs BigQuery):

```mdx
<Tabs>
  <Tab title="Snowflake">
    ...
  </Tab>
  <Tab title="BigQuery">
    ...
  </Tab>
</Tabs>
```

### [Accordions](https://mintlify.com/docs/content/components/accordions)

Use for optional/advanced config that would clutter the main flow:

```mdx
<AccordionGroup>
  <Accordion title="Advanced options">
    ...
  </Accordion>
</AccordionGroup>
```

### [Code blocks](https://mintlify.com/docs/content/components/code)

Always specify the language for syntax highlighting. Use the `filename` prop when showing file content:

```mdx
```yaml filename="profiles.yml"
elementary:
  target: dev
```
```

---

## Mintlify Checks

Before opening a PR, run these locally from the `docs/` directory:

```bash
# Install Mintlify CLI (once)
npm i -g mintlify

# Start local preview — catches broken links and MDX errors
mintlify dev

# Check for broken links explicitly
mintlify broken-links
```

**What to verify:**
- [ ] `mintlify dev` starts without errors
- [ ] No broken internal links (all paths in `docs.json` resolve to real files)
- [ ] No MDX syntax errors (unclosed tags, invalid JSX)
- [ ] Images render correctly in both light and dark mode
- [ ] Page appears in the correct sidebar group
- [ ] Redirects added in `docs.json` if a page was renamed or moved

If you renamed a page, add a redirect in `docs.json`:
```json
"redirects": [
  { "source": "/old/path", "destination": "/new/path" }
]
```

---

## Setup: Running Docs Locally

```bash
cd docs
npm i -g mintlify   # one-time install
mintlify dev        # starts at http://localhost:3000
```

The preview hot-reloads on save.
