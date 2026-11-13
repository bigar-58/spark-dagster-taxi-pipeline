# Domain docs

This repository uses a single-context domain documentation layout.

## Before exploring

- Read `GLOSSARY.md` at the repository root.
- Read ADRs in `docs/adr/` that relate to the area being explored.

If these files or directories do not exist, proceed silently.
Do not flag their absence or suggest creating them upfront.

The domain-modeling skill creates domain documentation when terms
or decisions are resolved.

## File structure

    /
    ├── GLOSSARY.md
    └── docs/
        └── adr/
            └── <number>-<decision-title>.md

`GLOSSARY.md` holds the shared vocabulary for the pipeline.
`docs/adr/` holds architectural decision records.

## Use the glossary's vocabulary

Use defined domain terms consistently in issue titles, proposals,
code, and tests. Avoid synonyms the glossary explicitly excludes.

If a needed concept is missing, reconsider whether it belongs in
the domain model or note the gap for domain-modeling.

## Flag ADR conflicts

If a proposal contradicts an existing ADR, identify the ADR and
explain why its decision should be reconsidered. Do not silently
override it.
