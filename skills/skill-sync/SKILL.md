---
name: skill-sync
description: Auto-sync skill metadata to AGENTS.md files
license: MIT
metadata:
  scope:
    - root
  auto_invoke:
    - "Updating AGENTS.md Auto-invoke tables"
    - "Adding new skills"
---

# Skill Sync

Meta skill for automatically updating AGENTS.md files with skill metadata.

## Usage

```bash
./skills/skill-sync/assets/sync.sh
```

## What It Does

1. Reads all `skills/*/SKILL.md` files
2. Extracts `metadata.scope` and `metadata.auto_invoke` from YAML frontmatter
3. Updates `<!-- AUTO_INVOKE_START -->` sections in target AGENTS.md files
4. Updates `<!-- SKILLS_TABLE_START -->` section in root AGENTS.md

## Scope Mappings

| Scope | Target File |
|-------|-------------|
| root | AGENTS.md |
| src | src/AGENTS.md |
| ui | ui/AGENTS.md |
