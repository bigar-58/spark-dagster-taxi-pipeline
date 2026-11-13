
# Engineering Guidelines

## Project Context
- This repository contains a data engineering pipeline currently using
  Dagster, PySpark, PostgreSQL, and Docker.
- Consult the README and relevant project documentation to understand
  the current architecture before proposing changes.
- Treat existing code and tests as the source of truth for implemented
  behavior, and flag discrepancies with documentation.
- Preserve established architectural boundaries and data contracts
  unless a change has been explicitly approved.
- Update relevant documentation when architectural decisions change.

## Design and Decision Making
- For nontrivial changes, propose a design before implementing.
- Explain relevant patterns, alternatives, and tradeoffs concisely.
- Prefer simple, cohesive modules with clearly defined responsibilities.
- Avoid premature abstractions and speculative features.
- Request human approval before significant architectural decisions.

## Implementation Discipline
- Implement one approved, narrowly scoped change at a time.
- Avoid unrelated refactoring or unnecessary new files.
- If the implementation deviates significantly from the approved design,
  stop and explain the reason before continuing.
- If a small change unexpectedly touches more than ~8 substantive files,
  pause for approval.

## Testing and Review
- Add or update tests for behavioral changes.
- Run relevant tests where possible and report actual results.
- Review changes for correctness, maintainability, and scope.
- Never claim tests passed unless they were executed successfully.

## Git and GitHub
- Never commit, push, create PRs, or merge without explicit approval.
- Never modify secrets or commit credentials.
- Keep changes focused and PR descriptions concise.

## Communication
- Explain important engineering decisions and their rationale.
- Highlight unfamiliar design patterns and their practical tradeoffs.
- Prefer concise summaries over file-by-file narration.
- Ask for clarification when requirements materially affect design.

## Agent skills

### Issue tracker

Issues and specs are tracked as local Markdown files under `.scratch/`.
See `docs/agents/issue-tracker.md`.
The user manages GitHub interactions manually, including issues, commits,
pushes, and pull requests. Skills must not perform these actions.

### Domain docs

Single-context layout: root `GLOSSARY.md` and `docs/adr/`.
See `docs/agents/domain.md`.
