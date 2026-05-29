# Contributing to Veriflow

Thanks for your interest in contributing! Veriflow is an early-stage project and all contributions are welcome — bug reports, feature ideas, documentation improvements, and code.

## Getting Started

### Prerequisites
- Go 1.22+
- Docker + Kubernetes (or kind/minikube for local dev)
- PostgreSQL

### Local Setup
```bash
git clone https://github.com/NasitSony/veriflow-control-plane.git
cd veriflow-control-plane
make up      # starts Postgres via Docker
make api     # starts the Job API
make sched   # starts the Scheduler
make demo-success  # runs a demo job end-to-end
```

## How to Contribute

### Reporting Bugs
Open an issue at [GitHub Issues](https://github.com/NasitSony/veriflow-control-plane/issues) and include:
- What you expected to happen
- What actually happened
- Steps to reproduce
- Relevant logs or error messages

### Suggesting Features
Open an issue with the `enhancement` label. Good feature requests explain:
- The problem you're trying to solve
- Why it belongs in Veriflow's core (not a wrapper on top)

### Submitting a Pull Request
1. Fork the repo
2. Create a branch: `git checkout -b your-feature-name`
3. Make your changes
4. Make sure the build passes: `go build ./...`
5. Run tests: `go test ./...`
6. Commit with a clear message: `git commit -m "feat: add X"`
7. Push and open a PR against `main`

## Good First Issues
Look for issues tagged `good first issue` — these are small, well-scoped tasks
that are a great way to get familiar with the codebase.

Areas that always welcome contributions:
- Additional GPU placement strategies
- Improved observability and metrics
- Documentation and runbook improvements
- Test coverage

## Code Style
- Follow standard Go conventions (`gofmt`, `go vet`)
- Keep functions focused and small
- Add comments for non-obvious logic
- Match the existing patterns in the codebase

## Commit Message Format
Use conventional commits where possible:
- `feat:` — new feature
- `fix:` — bug fix
- `docs:` — documentation only
- `refactor:` — code change that neither fixes a bug nor adds a feature
- `test:` — adding or updating tests

## Questions?
Open an issue or start a discussion — happy to help you get oriented.
