COPILOT GUIDELINES:


# Project Python Style Guide

## 1. Structural Constraints
* **PEP 8**: Strict adherence required with 4-space indentation.
* **Line Length**: Hard limit of 40 characters per line.
* **Function Scope**: Maximum 20 lines per function; focus on a single task.
* **No Nesting**: Never define functions inside other functions.
* **Returns**: Functions must return exactly one data type.
* **Modern Python**: Use version 3.13+ features (dataclasses, pattern matching, type hints).

## 2. Documentation & Naming
* **Variable Names**: Must be descriptive enough to make comments unnecessary.
* **Docstrings**: Required for all functions/classes; maximum 5 lines.
    * Include: Purpose and edge cases.
    * Exclude: Parameter descriptions for simple unit functions.
* **Comments**: Bias against them; use only if code cannot be made self-explanatory.

## 3. Logic & Flow Control
* **Error Handling**: Bias against `try/except`. Use `validate_` functions instead.
    * If `try` is required, it must contain only one operation.
    * Never use exceptions for program flow.
* **Guard Clauses**: Use to minimize nesting and improve readability.
* **Conditionals**: Use `if` only for "expected" logic (e.g., data validation), not for configuration errors.
* **Iteration**: Prefer standard loops over list comprehensions or lambdas to maintain clarity.
* **Constants**: No "magic numbers"; assign all values to descriptive variables.

## 4. Quality Standards
* **DRY & Concise**: Avoid repetition, but never sacrifice readability for brevity.
* **OOP**: Use Object-Oriented Programming where possible for modularity.
* **Formatting**: Use referencing instead of f strings for logging and`round()` for all numerical output.
* **Types**: Avoid `object` type; use specific, inbuilt Python types.
