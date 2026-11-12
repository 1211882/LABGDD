# LABGDD V2 – Development Workflow and Mandatory Handoff

These instructions apply to every future development task, implementation phase, refactoring, bug fix, and architecture change in this repository.

## 1. Development Workflow

Work incrementally and preserve the established architecture.

For every task:

1. Inspect the relevant existing implementation.
2. Identify the objective and scope.
3. Explain significant architectural decisions.
4. Implement only the requested changes.
5. Add or update appropriate tests.
6. Execute relevant validation checks.
7. Review the final diff for unintended changes.
8. Produce a complete handoff report.

Do not automatically proceed to the next phase.

Do not introduce unnecessary abstractions, dependencies, services, or architectural complexity.

Preserve data contracts, scientific reproducibility, and existing validated behavior.

## 2. Mandatory End-of-Task Handoff

At the end of **every execution**, provide a self-contained technical report directly in your final response.

The report must be written in English and formatted as Markdown.

It must be possible to copy the entire report and paste it into another AI assistant without requiring access to this repository.

Use the following structure.

### LABGDD V2 – Development Handoff

**Phase:**  
**Task:**  
**Status:** Completed / Partial / Blocked  
**Date:**  
**Git branch and commit:**  

#### 1. Objective

Explain what this task was intended to accomplish.

#### 2. Changes Implemented

Describe the actual implementation.

Include important modules, classes, functions, configurations, and architectural changes.

Do not simply list filenames.

#### 3. Files Created or Modified

List the relevant files and explain their purpose.

#### 4. Architecture and Design Decisions

Explain significant decisions and trade-offs.

Identify any changes to data contracts, dependency boundaries, persistence, financial calculations, or execution behavior.

#### 5. Tests and Validation

Provide the actual executed commands and their results.

Include:

- Unit tests
- Integration tests, when applicable
- Formatting
- Linting
- Type checking
- Build or installation checks, when relevant
- Domain-specific validation

Clearly distinguish:

- Passed
- Failed
- Skipped
- Not executed

Never claim a test passed if it was not executed.

#### 6. Issues and Limitations

Identify:

- Known bugs
- Unverified assumptions
- Technical debt
- Missing tests
- Architecture concerns
- Environment limitations
- Potential regressions

#### 7. Scientific and Data Integrity

When relevant, explain:

- Changes to financial formulas
- Changes to temporal semantics
- Data-quality policies
- Batch/streaming consistency
- Dataset or model versioning
- Reproducibility
- Implications for previous experiments

If no scientific or data semantics changed, explicitly state that.

#### 8. Current Project Status

Summarize which components are operational, which remain incomplete, and what the project can currently do.

Do not confuse implemented code with verified runtime behavior.

#### 9. Recommended Next Step

Identify the next logical development milestone.

Do not implement it automatically.

#### 10. Review Questions

List only significant decisions that require architecture review or user approval.

If none exist, state "None".

## 3. Copy-Paste Requirements

The final handoff must:

- Be self-contained.
- Contain enough technical detail for an external reviewer.
- Use exact repository-relative file paths.
- Identify relevant interfaces and function names.
- Include actual validation results.
- Distinguish verified facts from assumptions.
- Never contain credentials or secret values.
- Avoid unnecessary terminal output.
- Avoid generic success claims.

Aim for approximately 800–1,500 words for substantial development phases, but use less for small changes.

Do not omit critical information to satisfy a word limit.

## 4. Persistent Handoff Documentation

For each substantial phase, also create or update:

`docs/handoffs/PHASE_<NUMBER>_HANDOFF.md`

For smaller tasks, use:

`docs/handoffs/TASK_<SHORT_NAME>_HANDOFF.md`

The document should contain the same technical information as the final response.

The final response must still contain the complete copy-paste report, not merely a link to the document.

Do not overwrite previous handoffs.

## 5. Final Review Gate

Before finishing any task, verify:

- Was the requested scope respected?
- Were all relevant checks executed?
- Were any existing contracts changed?
- Were new dependencies justified?
- Are there remaining risks?
- Is the implementation ready for review?
- Is the handoff understandable without repository access?

End every report with:

**Review status:** READY FOR REVIEW / NEEDS ATTENTION / BLOCKED

**Next phase:** NOT STARTED

Never proceed automatically to the next phase without explicit instruction.
