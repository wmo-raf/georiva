# Contributing to GeoRiva

Thanks for your interest in contributing to GeoRiva. This guide covers how to set up a development environment, the
conventions we follow, and how to submit your work.

---

## Getting Oriented

Before diving into code, it's worth reading the [Architecture Design Document](architecture/README.md) to understand how
the system is structured. The [Open Questions](architecture/README.md#9-open-questions--discussion-points) section is a
good starting point if you're looking for areas where input is especially valuable.

---

## Development Setup

### Prerequisites

- Docker and Docker Compose
- Python 3.12+
- Node.js 20+ (for frontend tooling, if applicable)
- Git

### Running the Stack

TODO

### Running Outside Docker (for faster iteration)

TODO

### Running Tests

TODO

---

## Ways to Contribute

### Architecture Feedback

The system is still in its early stages. Opening an issue or discussion to challenge a design decision is just as
valuable as writing code.

### Source Plugins

If you work with a geospatial data provider (weather models, satellite products, reanalysis datasets), building a source
plugin is one of the most impactful contributions. A source plugin is a Wagtail app that implements the source plugin
contract — see the architecture doc's [Source Plugins section](architecture/README.md#31-path-a-source-plugins) for the
design, and look at an existing plugins in `ecmwf_aifs_source/` for reference.

### Analysis Modules

Analysis modules follow a similar plugin pattern. If you have domain expertise and know which Xarray-compatible
libraries would be useful, an analysis module is a great contribution. See
the [Analysis Layer section](architecture/README.md#6-analysis-layer) for context.

### Bug Fixes and Improvements

Check the issue tracker for bugs labeled `good first issue` or `help wanted`. These are scoped to be approachable
without deep knowledge of the full system.

### Documentation

Improvements to documentation are always welcome — whether that's fixing typos, clarifying explanations, or adding
examples.

---

## Workflow

### Branching

We use a simple branching model:

- `main` — stable, deployable state
- `dev` — integration branch for ongoing work
- Feature branches — branched from `dev`, named descriptively

```
dev
  └── feature/gfs-source-plugin
  └── fix/item-datetime-indexing
  └── docs/plugin-authoring-guide
```

### Submitting Changes

1. Fork the repository and create a branch from `dev`
2. Make your changes, keeping commits focused and descriptive
3. Add or update tests for any new functionality
4. Make sure the test suite passes
5. Open a pull request against `dev` with a clear description of what the change does and why

### Pull Request Guidelines

- Keep PRs focused on a single concern. If you find something unrelated that needs fixing, open a separate PR.
- Include context in the PR description. Link to relevant issues or architecture doc sections.
- If the PR changes the architecture or introduces a new pattern, update the relevant documentation.
- For new plugins (source or analysis), include a brief README in the plugin directory explaining what it does and how
  to configure it.

---

## Code Conventions

### Commit Messages

Write clear, descriptive commit messages. Use the imperative mood ("Add GFS source plugin" not "Added GFS source
plugin").

For non-trivial changes, include a brief body explaining the reasoning:

```
Add TimescaleDB hypertable for Item model

The Item table will be the most heavily queried model, primarily by
time range. Converting it to a TimescaleDB hypertable enables automatic
partitioning and optimized time-range queries.
```

### Django / Wagtail

- Models go in the appropriate app's `models.py` (or `models/` package for larger apps).
- Keep business logic out of views — use service functions or model methods.
- Use Django's migration system. Don't modify migrations by hand unless you know what you're doing.
- Plugin apps should be self-contained and not import from other plugins.

---

## Getting Help

If you're unsure about anything — whether it's how something works, where to put your code, or whether an idea fits the
project — just open an issue or start a discussion. There are no bad questions when a project is this early.
