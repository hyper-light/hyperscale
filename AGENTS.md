# Development Guidelines

This document contains critical information about working with this codebase. Follow these guidelines precisely.

## Core Development Rules

1. Package Management
  - ONLY use uv, NEVER pip
  - Installation: `uv add package`
  - Running tools: `uv run tool`
  - Upgrading: `uv add --dev package --upgrade-package package`
  - FORBIDDEN: `uv pip install`, `@latest` syntax
  - Use internal modules like taskex/ for running things in the background or our own async logging class at hypercale/logging

2. Code Quality
  - Type hints required, but we prefer to infer return types.
  - For test workflow classes, type hints and return type hints are REQUIRED.
  - Public APIs must have docstrings
  - Functions may be larger but not greater than a hundred or so lines.
  - If we do something more that three times, it becomes a function
  - Follow existing patterns exactly
  - Line length: 120 chars maximum
  - We prefer creating composed smaller classes to large monolithc ones
  - Avoid writing functions or logic with large cyclomatic complexity
  - We *do not* EVER swallow errors
  - We *never* create asyncio orphaned tasks or futures. Use the TaskRunner instead
  - We *always* use the Logger in hyperscale/Logger. If you need to create new logger models, they go in hyperscale_logging_models.py. Follow the patterns and conventions there.
  - When creating a class we try to use init state as confiugration and avoid mutating it in method calls.
  - We always cleanup - if we store long running task data, we clean it up.
  - Memory leaks are *unnacceptable* period.
  - For an architectural or implementation decision, we ALWAYS take the most robust approach
  - One class per file. Period.
  - Files in a given folder should be similar - nodes contains node implmentations, swim our core swim logic, models our data models, etc.
  - We *ALWAYS* use absolute imports for external imports unless the import comes from a child module of our current module, then we use relative.
  - The logger is async and you need to await .log(), don't add it to the task runner
  - If a function is async and returns quickly, you do not need to submit it to the task runner, we submit things like polling jobs, log-running tasks, synchronous calls to the task runner.
  - If you can use generics, do so. Avoid using Any for typehints.
  - Read Architecture.md any time you need more context about what something does. This will save you LOTS of time.
  - Again, if there is a way to implement something that is more correct and robust, we do it.
  - Treat *everything* as if it must be compatible with asyncio
  - You need to pay particular attentionto detail with providing correct attribues to classes and accessing them correctly.
  - Use long variable names and avoid abbreviations (like i for index as opposed to idx) or "shortnames" for variables. Maximize readability.
  

3. Testing Requirements
  - Write integration style tests as in tests/integration but do not run them
  - DO NOT RUN THE INTEGRATION TESTS YOURSELF. Ask me to.


4. Code Style
    - PEP 8 naming (snake_case for functions/variables)
    - Class names in PascalCase
    - Constants in UPPER_SNAKE_CASE
    - Document with docstrings
    - Use f-strings for formatting
    - Avoid cyclomatic complexity beyond three
    - Use python 3.12+ Walrus operators and other modenr Python syntax
    - Use list and dic comprehensions for filtering, flattening, or mapping
    - Use .update() for merging dicts when possible to avoid unneccessary re-allocations
    - sorted and map are fine when needed


- After any fix or implementation of a todo, we generate a fresh commit. Do NOT run the tests. A user will run them and confirm.
- Always commit everything - i.e. `git add -A && git commit -m "<MESSAGE_HERE>"