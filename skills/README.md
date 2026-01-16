# AI Agent Skills

Skills are structured documentation that help AI coding assistants understand project-specific patterns and conventions. They follow the [Agent Skills open standard](https://agentskills.io).

## What Are Skills?

Skills are markdown files with YAML frontmatter that contain:
- **Metadata**: Scope, auto-invoke triggers, and descriptions
- **Critical Rules**: ALWAYS/NEVER guidelines for the AI
- **Patterns**: Code examples from the actual codebase
- **References**: Links to relevant documentation

## Setup

Run the setup script to configure your AI tools:

```bash
./skills/setup.sh
```

This creates symlinks for:
- Claude Code / Claude Desktop (`.claude/skills`)
- OpenCode (uses `.claude/skills` - it's a Claude Code fork)
- Gemini CLI (`.gemini/skills`)
- Codex (`.codex/skills`)
- Cursor IDE (`.cursor/skills`)
- GitHub Copilot (`.github/copilot-instructions.md`)

## How to Use Skills

### Auto-invoke

Skills define triggers in their frontmatter. When you perform a matching action, the AI should automatically apply that skill's rules.

### Manual Invoke

Reference a skill directly in your prompt:
```
Use the cloudflare-workers skill to implement this handler.
```

## Available Skills

| Skill | Description | Scope |
|-------|-------------|-------|
| cloudflare-workers | Cloudflare Workers patterns for this project | src |
| skill-sync | Auto-sync skill metadata to AGENTS.md | root |

## Directory Structure

```
skills/
├── README.md                    # This file
├── setup.sh                     # Multi-tool setup script
├── cloudflare-workers/
│   └── SKILL.md                # Worker patterns skill
└── skill-sync/
    ├── SKILL.md                # Meta skill for syncing
    └── assets/
        └── sync.sh             # AGENTS.md auto-updater
```

## Design Principles

1. **Concise**: Keep skills focused on critical patterns only
2. **Progressive Disclosure**: Most important rules first
3. **Project-Specific**: Use actual code from this codebase, not generic examples
4. **Maintainable**: Skills auto-sync to AGENTS.md via sync.sh

## Creating New Skills

1. Create a directory: `skills/{skill-name}/`
2. Add `SKILL.md` with YAML frontmatter:

```yaml
---
name: skill-name
description: Brief description
license: MIT
metadata:
  scope:
    - src
  auto_invoke:
    - "Trigger phrase 1"
    - "Trigger phrase 2"
---
```

3. Run `./skills/skill-sync/assets/sync.sh` to update AGENTS.md files

## References

- [Agent Skills Standard](https://agentskills.io)
- [Project AGENTS.md](../AGENTS.md)
