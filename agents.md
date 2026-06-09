# DATASTORE-API

This application serves as a datastore for the microdata.no plattform. It is a FastAPI application that persist data as
parquet files, metadata in json files or sqlite tables, and job information in sqlite tables.

## Modules
The application uses a layered architecture inspired by hexagonal architecture:
- **datastore_api/** directory containing the source code
    - **api/**: definitions for the restful api
    - **domain/**: all core domain logic for the application
    - **adapter/**: adapters for databases, filesystem and external services
    - **common/**: common modules used by the whole stack
    - **config/**: configuration for application and logging
    - **main.py**: the entrypoint for application startup

The application flows from api, to  domain, to adapters. No imports in the wrong direction. api can import from domain
or adapters, but adapters can never import from domain or api.
When developing a feature that cuts through every layer of the application, the implementation should start from the
bottom to the top, from adapters to api.

## Development Workflow (uv)
- Use `uv` for Python package and environment management.
- Add dependencies with `uv add <package>` (and dev dependencies with `uv add --dev <package>`).
- Format code with `uv run ruff format`.
- Run autofixes with `uv run ruff check --fix`.
- Sort imports specifically with `uv run ruff check --fix --select I`.
- Run tests with `uv run pytest`

## RULES

### 1. Scope Before You Build 

**Move fast on small tasks. Plan deliberately on larger ones.**

When scoping work:
- If the user request is small, do it directly without creating a todo list.
- If the request is larger (multi-step or non-trivial), always create a todo list.
- Iterate on the todo list together with the developer until both scope and implementation are satisfactory.
- Prefer small pull requests and manageable chunks of work.
- If a request can be split into a smaller, safer increment, suggest that alternative before creating the todo list.

The test: Work should stay focused, trackable, and easy to review in small increments.

### 2. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 3. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 4. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

### 5. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

