# Issue tracker: Local Markdown

Issues and specs for this repo live as Markdown files in `.scratch/`.

## Human approval

- Issues and specifications must be reviewed by the user before
  publishing to the local tracker. Present drafts before creating
  tracker files.
- Creating, modifying, changing the status of, or closing local
  issues requires the user's explicit approval.
- Creating an issue must not automatically trigger implementation.
- The user manages GitHub interactions manually, including issues,
  commits, pushes, and pull requests. Skills must not perform these
  actions.

## Conventions

- One feature per directory: `.scratch/<feature-slug>/`.
- The spec is `.scratch/<feature-slug>/spec.md`.
- Implementation issues are one file per ticket at
  `.scratch/<feature-slug>/issues/<NN>-<slug>.md`, numbered from `01`
  in dependency order (blockers first). Never use a combined tickets file.
- Record issue state as a `Status:` line near the top of each issue file.
- Record dependencies as an optional `Blocked by:` line listing ticket
  numbers and titles within the same feature directory.
- A ticket is unblocked when every blocker is closed or resolved.
- Append comments and conversation history under a `## Comments` heading.
- Create directories and tracker files only when needed and approved.

## Skill terminology

- "Publish to the issue tracker" means create the approved Markdown file
  under `.scratch/<feature-slug>/`.
- "Fetch the relevant ticket" means read the file at the referenced path.
  If only a ticket number is supplied, resolve it within the relevant
  feature directory; ask if the feature is ambiguous.
