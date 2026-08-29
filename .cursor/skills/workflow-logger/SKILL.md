---
name: log-ai-workflow
description: >-
  Documents architectural shifts, corrections to AI output, or workflow speed
  wins in README "AI in My Workflow". Use when logging AI decisions, README AI
  LOG entries, assessment AI workflow evidence, or after major design pivots.
paths:
  - "README.md"
---

# Skill: Log AI Workflow

Help append a structured entry to the "AI in My Workflow" section of `README.md`.

## Expected Protocol

1. Read the current `README.md` to see the structure of the "AI in My Workflow" section.
2. Prompt with these exact questions if they aren't provided in the request:
   - What did we just change or correct?
   - Why did the initial AI approach fall short?
   - How did this impact our development speed or design quality?
3. Format the output cleanly as a bullet point or sub-section under the appropriate README header.

## Companion

When an architectural pivot happens mid-chat, also emit a brief `📝 README AI LOG` snippet (per `.cursorrules`) even before the README edit, so evidence is not lost.
