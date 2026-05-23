---
tracker:
  kind: linear
  project_slug: '{{LINEAR_PROJECT_SLUG}}'
  assignee: '{{LINEAR_ASSIGNEE}}'
  active_states:
    - Todo
    - In Progress
    - Human Review
    - Rework
    - Merging
  terminal_states:
    - Closed
    - Cancelled
    - Canceled
    - Duplicate
    - Done
polling:
  interval_ms: 30000
workspace:
  root: '{{WORKSPACE_ROOT}}'
hooks:
  after_create: |
    git clone --depth 1 {{SOURCE_REPO_URL}} .
    workspace_path=$(pwd -P)
    database_url=${DURABABBLE_DATABASE_URL:-postgresql://yugabyte@127.0.0.1:15433/yugabyte}
    export workspace_path database_url
    workspace_schema=$(python3 <<'PY'
    import hashlib
    import os
    import re

    path = os.path.realpath(os.environ["workspace_path"])
    leaf = re.sub(r"[^a-z0-9_]+", "_", os.path.basename(path).lower()).strip("_") or "workspace"
    prefix = "durababble"
    suffix = hashlib.sha256(path.encode()).hexdigest()[:12]
    base = f"{prefix}_{leaf}"
    max_base_length = 63 - len(suffix) - 1
    print(f"{base[:max_base_length].rstrip('_')}_{suffix}")
    PY
    )
    export workspace_schema
    python3 <<'PY'
    import json
    import os
    from pathlib import Path

    Path("mise.local.toml").write_text("\n".join([
        "[env]",
        f"DURABABBLE_DATABASE_URL = {json.dumps(os.environ['database_url'])}",
        f"DURABABBLE_YUGABYTE_DATABASE_URL = {json.dumps(os.environ['database_url'])}",
        f"DURABABBLE_WORKSPACE_ROOT = {json.dumps(os.environ['workspace_path'])}",
        f"DURABABBLE_SCHEMA = {json.dumps(os.environ['workspace_schema'])}",
        "",
    ]))
    PY
    canonical_agents=/home/airhorns/code/durababble/.agents
    if [ -d "$canonical_agents" ] && [ ! -e .agents ]; then
      cp -a "$canonical_agents" .agents
    fi
    if command -v mise >/dev/null 2>&1; then
      mise trust -y mise.toml
      mise trust -y mise.local.toml
      mise install
      mise exec -- bundle install
      cat > .durababble-workspace.env <<EOF
    export DURABABBLE_DATABASE_URL="$database_url"
    export DURABABBLE_YUGABYTE_DATABASE_URL="$database_url"
    export DURABABBLE_WORKSPACE_ROOT="$workspace_path"
    export DURABABBLE_SCHEMA="$workspace_schema"
    EOF
      mise exec -- bundle exec ruby -Ilib -e 'require "durababble"; store = Durababble::Store.connect(database_url: ENV.fetch("DURABABBLE_DATABASE_URL"), schema: ENV.fetch("DURABABBLE_SCHEMA")); store.migrate!; puts "migrated #{ENV.fetch("DURABABBLE_SCHEMA")}"; store.close'
    else
      echo "mise is required for Durababble's Ruby 4.0.5 toolchain" >&2
      exit 78
    fi
agent:
  max_concurrent_agents: 6
  max_concurrent_agents_by_state:
    todo: 2
    in progress: 2
    rework: 2
    merging: 2
  max_turns: 24
codex:
  command: '{{CODEX_COMMAND}}'
  approval_policy: never
  thread_sandbox: danger-full-access
server:
  dashboard_enabled: true
  host: '{{SYMPHONY_HOST}}'
---

## Repository-specific guidance for `durababble`

You are working in the `durababble` repository.

Identity requirements:

- The Linear user for this workflow should be the `harrymees` user.
- The GitHub user for this workflow should be the `harrymees` user.

GitHub execution requirements:

- Do **not** use any Codex/OpenAI GitHub connector, MCP GitHub server, or `mcp__codex_apps__github_*` tool for branch, PR, review, or comment actions.
- Perform all GitHub work via the local shell using the checked-out repo's `git` remote plus the host's authenticated `gh` CLI.
- Create pull requests with `gh pr create`; inspect/update them with local `gh pr ...` / `gh api ...` commands only.
- If the issue already has an open PR, reuse that PR's branch for all follow-up work; do **not** open a replacement PR for rework or review feedback.
- For updates to an existing open PR, push the updated branch back to the same remote branch, using `git push --force-with-lease` when history must be rewritten.
- If a connector/MCP GitHub tool is offered, ignore it and continue with local `git`/`gh` commands so PRs are authored as the host's `harrymees` account.
- If `gh pr edit` fails with a GitHub Projects classic / `projectCards` deprecation error, fall back to a narrower local `gh api` update for the exact field or label you need, then verify with `gh pr view`. Do not replace the PR or report GitHub access blocked just because that high-level command failed.

Comment marker requirements:

- On both Linear comments and GitHub PR/review comments, when you have seen a comment and are actively working from it, mark that comment with a `👀` reaction.
- Once the requested action is complete or the comment has been fully handled, replace the `👀` reaction with a `🟢` reaction.
- Do not leave both markers on the same comment at once; `👀` means in progress, `🟢` means done.
- GitHub review-comment reactions do not always support the `🟢` marker. If the reactions API rejects it, remove `👀`, post a concise handled reply that starts with `🟢`, and resolve the review thread when no further reviewer input is needed.

Primary mission:

- Durababble is a Ruby 4 durable-execution prototype backed by SQL storage. The current standalone repo defaults local development toward the agent-server MySQL/MariaDB path while keeping YugabyteDB/YSQL coverage available through `DURABABBLE_DATABASE_URL` / `DURABABBLE_YUGABYTE_DATABASE_URL`.
- Keep the implementation honest: durability, recovery, leases, waits, commands, fences, and outbox behavior must be persisted and tested, not papered over with in-memory shims.

Non-negotiables:

1. Persist state before and after every durable boundary; do not fake durable behavior with process-local state.
2. When touching storage semantics, run or add tests against the real Yugabyte/PostgreSQL-compatible path whenever possible, and preserve backend conformance with MySQL/MariaDB where relevant.
3. Prefer end-to-end behavioral/regression tests over assertions that only restate metadata or docs.
4. Preserve the settled API direction in `docs/spec.md`: `Workflow.start` / `Workflow.handle`, `DurableObject.at` / `DurableObject.tell`, method/order step identity, unified inbox, and the full four-method gRPC direction.
5. Keep payload serialization through Paquito/binary storage unless deliberately changing the spec and implementation together.
6. Update `docs/spec.md`, `docs/architecture.md`, and `README.md` when behavior, API, storage guarantees, or operational expectations change.
7. Treat the project as a correctness-oriented prototype rather than a production Temporal replacement; do not silently widen scope beyond the ticket.

Required reading order:

1. `AGENTS.md`
2. `README.md`
3. `docs/spec.md`
4. `docs/architecture.md`
5. `docs/deterministic-testing.md` when touching deterministic simulation or recovery scenarios

Implementation guidance:

- Use Ruby 4 through `mise`; do not assume system `ruby` or `bundle` is on `PATH`.
- Use `mise exec -- bundle exec rake test` as the default full validation command.
- For targeted work, run the smallest relevant `mise exec -- bundle exec ruby -I lib -I test test/..._test.rb` command first, then the full test suite before handoff when practical.
- For database/storage changes, use the workspace-selected namespace: `DURABABBLE_SCHEMA` if set, otherwise `Durababble.workspace_schema(DURABABBLE_WORKSPACE_ROOT || Dir.pwd)`. Symphony-created workspaces write `mise.local.toml`, trust it, run a migration for the isolated namespace, and leave `.durababble-workspace.env` for inspection/reuse. The host-local Yugabyte/YSQL smoke endpoint is `DURABABBLE_DATABASE_URL=postgresql://yugabyte@127.0.0.1:15433/yugabyte`; MySQL/MariaDB conformance uses the current `DURABABBLE_MYSQL_*` environment when available.
- If the real database is required and unavailable, document the exact failing probe/command in the workpad before using the blocked-access escape hatch.
- Add regression tests for deterministic, crash/recovery, and namespace-isolation bugs.
- Keep RBS signatures in `sig/durababble.rbs` aligned with public API changes.

Tooling guidance:

- Bootstrap workspaces with `mise install` and `mise exec -- bundle install`; Symphony-created workspaces also write/trust `mise.local.toml`, migrate their isolated namespace, and leave `.durababble-workspace.env` for inspection.
- Run project commands through `mise exec -- ...` so Ruby 4.0.5, Bundler, and workspace-local `DURABABBLE_*` environment values come from the repo toolchain.
- Avoid relying on globally installed Ruby tools; system `ruby`/`bundle` may be absent on this host.
- If you need repo-local agent skills and `.agents` is missing in the workspace, inspect the repo root carefully before assuming the skill exists.

The following Symphony workflow contract remains authoritative; follow it exactly, and treat the repository-specific guidance above as additional constraints rather than a replacement.

You are working on a Linear ticket `{{ issue.identifier }}`

{% if attempt %}
Continuation context:

- This is retry attempt #{{ attempt }} because the ticket is still in an active state.
- Resume from the current workspace state instead of restarting from scratch.
- Do not repeat already-completed investigation or validation unless needed for new code changes.
- Do not end the turn while the issue remains in an active state unless you are blocked by missing required permissions/secrets.
  {% endif %}

Issue context:
Identifier: {{ issue.identifier }}
Title: {{ issue.title }}
Current status: {{ issue.state }}
Labels: {{ issue.labels }}
URL: {{ issue.url }}

Description:
{% if issue.description %}
{{ issue.description }}
{% else %}
No description provided.
{% endif %}

Instructions:

1. This is an unattended orchestration session. Never ask a human to perform follow-up actions.
2. Only stop early for a true blocker (missing required auth/permissions/secrets). If blocked, record it in the workpad and move the issue according to workflow.
3. Final message must report completed actions and blockers only. Do not include "next steps for user".

Work only in the provided repository copy. Do not touch any other path.

## Prerequisite: Linear MCP or `linear_graphql` tool is available

The agent should be able to talk to Linear, either via a configured Linear MCP server or injected `linear_graphql` tool. If none are present, stop and ask the user to configure Linear.

## Default posture

- Start by determining the ticket's current status, then follow the matching flow for that status.
- Start every task by opening the tracking workpad comment and bringing it up to date before doing new implementation work.
- Spend extra effort up front on planning and verification design before implementation.
- Reproduce first: always confirm the current behavior/issue signal before changing code so the fix target is explicit.
- Keep ticket metadata current (state, checklist, acceptance criteria, links).
- Treat a single persistent Linear comment as the source of truth for progress.
- Use that single workpad comment for all progress and handoff notes; do not post separate "done"/summary comments.
- Treat any ticket-authored `Validation`, `Test Plan`, or `Testing` section as non-negotiable acceptance input: mirror it in the workpad and execute it before considering the work complete.
- When meaningful out-of-scope improvements are discovered during execution,
  file a separate Linear issue instead of expanding scope. The follow-up issue
  must include a clear title, description, and acceptance criteria, be placed in
  `Backlog`, be assigned to the same project as the current issue, link the
  current issue as `related`, and use `blockedBy` when the follow-up depends on
  the current issue.
- Linear GraphQL can return opaque HTTP 400 responses for oversized or batched issue creation payloads. When generating issues, retry as individual concise `issueCreate` mutations, then verify state, assignee, project, and relations explicitly before marking the work complete.
- Linear relation queries can be direction-sensitive. For generated follow-ups, verify the relation from the seed issue and from the generated issue when the first query does not show every expected link.
- Move status only when the matching quality bar is met.
- Operate autonomously end-to-end unless blocked by missing requirements, secrets, or permissions.
- Use the blocked-access escape hatch only for true external blockers (missing required tools/auth) after exhausting documented fallbacks.

## Related skills

- `linear`: interact with Linear.
- `commit`: produce clean, logical commits during implementation.
- `push`: keep remote branch current and publish updates.
- `pull`: keep branch updated with latest `origin/main` before handoff.
- `land`: when ticket reaches `Merging`, explicitly open and follow `.agents/skills/land/SKILL.md`, which includes the `land` loop.

## Status map

- `Backlog` -> out of scope for this workflow; do not modify.
- `Todo` -> queued; immediately transition to `In Progress` before active work.
  - Special case: if a PR is already attached, treat as feedback/rework loop (run full PR feedback sweep, address or explicitly push back, revalidate, return to `Human Review`).
- `In Progress` -> implementation actively underway.
- `Blocked on Human` -> the agent is blocked on an external human action it cannot resolve itself (for example missing scopes, credentials, permissions, or tool access). Leave a concise blocker comment on the Linear issue, move the issue here, then stop until a human replies or moves the issue to a different state.
- `Human Review` -> PR is attached and validated for human approval.
- `Merging` -> approved by human; execute the `land` skill flow (do not call `gh pr merge` directly).
- `Rework` -> reviewer requested changes; planning + implementation required.
- `Done` -> terminal state; no further action required.

## Step 0: Determine current ticket state and route

1. Fetch the issue by explicit ticket ID.
2. Read the current state.
3. Route to the matching flow:
   - `Backlog` -> do not modify issue content/state; stop and wait for human to move it to `Todo`.
   - `Todo` -> immediately move to `In Progress`, then ensure bootstrap workpad comment exists (create if missing), then start execution flow.
     - If PR is already attached, start by reviewing all open PR comments and deciding required changes vs explicit pushback responses.
   - `In Progress` -> continue execution flow from current scratchpad comment.
   - `Blocked on Human` -> do nothing; wait for a human reply or for the issue to be moved into a different state before re-engaging.
   - `Human Review` -> wait and poll for decision/review updates.
   - `Merging` -> on entry, open and follow `.agents/skills/land/SKILL.md`; do not call `gh pr merge` directly.
   - `Rework` -> run rework flow.
   - `Done` -> do nothing and shut down.
4. Check whether a PR already exists for the current branch and whether it is closed.
   - If a branch PR exists and is `CLOSED` or `MERGED`, treat prior branch work as non-reusable for this run.
   - Create a fresh branch from `origin/main` and restart execution flow as a new attempt.
   - If a branch PR exists and is still open, keep using that same branch/PR; do not create a new PR for rework.
5. For `Todo` tickets, do startup sequencing in this exact order:
   - `update_issue(..., state: "In Progress")`
   - find/create `## Codex Workpad` bootstrap comment
   - only then begin analysis/planning/implementation work.
6. Add a short comment if state and issue content are inconsistent, then proceed with the safest flow.

## Step 1: Start/continue execution (Todo or In Progress)

1.  Find or create a single persistent scratchpad comment for the issue:
    - Search existing comments for a marker header: `## Codex Workpad`.
    - Ignore resolved comments while searching; only active/unresolved comments are eligible to be reused as the live workpad.
    - If found, reuse that comment; do not create a new workpad comment.
    - If not found, create one workpad comment and use it for all updates.
    - Persist the workpad comment ID and only write progress updates to that ID.
2.  If arriving from `Todo`, do not delay on additional status transitions: the issue should already be `In Progress` before this step begins.
3.  Immediately reconcile the workpad before new edits:
    - Check off items that are already done.
    - Expand/fix the plan so it is comprehensive for current scope.
    - Ensure `Acceptance Criteria` and `Validation` are current and still make sense for the task.
4.  Start work by writing/updating a hierarchical plan in the workpad comment.
5.  Ensure the workpad includes a compact environment stamp at the top as a code fence line:
    - Format: `<host>:<abs-workdir>@<short-sha>`
    - Example: `devbox-01:/home/dev-user/code/symphony-workspaces/MT-32@7bdde33bc`
    - Do not include metadata already inferable from Linear issue fields (`issue ID`, `status`, `branch`, `PR link`).
6.  Add explicit acceptance criteria and TODOs in checklist form in the same comment.
    - If changes are user-facing, include a UI walkthrough acceptance criterion that describes the end-to-end user path to validate.
    - If changes touch app files or app behavior, add explicit app-specific flow checks to `Acceptance Criteria` in the workpad (for example: launch path, changed interaction path, and expected result path).
    - If the ticket description/comment context includes `Validation`, `Test Plan`, or `Testing` sections, copy those requirements into the workpad `Acceptance Criteria` and `Validation` sections as required checkboxes (no optional downgrade).
7.  Run a principal-style self-review of the plan and refine it in the comment.
8.  Before implementing, capture a concrete reproduction signal and record it in the workpad `Notes` section (command/output, screenshot, or deterministic UI behavior).
9.  Run the `pull` skill to sync with latest `origin/main` before any code edits, then record the pull/sync result in the workpad `Notes`.
    - Include a `pull skill evidence` note with:
      - merge source(s),
      - result (`clean` or `conflicts resolved`),
      - resulting `HEAD` short SHA.
10. Compact context and proceed to execution.

## PR feedback sweep protocol (required)

When a ticket has an attached PR, run this protocol before moving to `Human Review`:

1. Identify the PR number from issue links/attachments.
2. Gather feedback from all channels:
   - Top-level PR comments (`gh pr view --comments`).
   - Inline review comments (`gh api repos/<owner>/<repo>/pulls/<pr>/comments`).
   - Review summaries/states (`gh pr view --json reviews`).
   - Linear issue comments.
3. Treat every actionable reviewer comment (human or bot), including inline review comments, as blocking until one of these is true:
   - code/test/docs updated to address it, or
   - explicit, justified pushback reply is posted on that thread.
   - As soon as a GitHub or Linear comment becomes part of the active work queue, mark it with `👀`.
4. Update the workpad plan/checklist to include each feedback item and its resolution status.
5. Re-run validation after feedback-driven changes and push updates.
   - After pushing fixes for a GitHub review thread, resolve that thread if the feedback is fully addressed and no further input is required.
   - If more clarification or a reviewer answer is still needed to proceed safely, reply on the GitHub thread with the specific open question and leave the thread unresolved.
   - When a GitHub or Linear comment has been fully addressed, replace its `👀` reaction with `🟢`.
6. Repeat this sweep until there are no outstanding actionable comments or unresolved clarification threads waiting on reviewer input.

## Blocked-access escape hatch (required behavior)

Use this only when completion is blocked by missing required tools or missing auth/permissions that cannot be resolved in-session.

- GitHub is **not** a valid blocker by default. Always try fallback strategies first (alternate remote/auth mode, then continue publish/review flow).
- Do not move to `Human Review` for GitHub access/auth until all fallback strategies have been attempted and documented in the workpad.
- Required conformance recording/re-recording is a special non-GitHub auth blocker. If the required live Shopify probe/capture cannot proceed after the documented credential repair paths have been tried, treat it as a human blocker.
- If a required non-GitHub tool, auth scope, permission, secret, or external access remains unavailable after exhausting documented fallbacks, treat that as a human blocker.
- When invoking this escape hatch:
  - update the workpad with the blocker evidence,
  - leave a concise top-level Linear comment that includes:
    - what is missing,
    - why it blocks required acceptance/validation,
    - the exact human action needed to unblock,
    - and the most relevant failing command/error when applicable,
  - add Linear reason labels before or while moving the issue to `Blocked on Human`:
    - add `missing scopes` when the blocker is missing Shopify API scopes, app grants, store permissions, or another Shopify access credential needed for required conformance/parity work,
    - add `broken environment` when the blocker is local environment setup or system software, such as the wrong Erlang/OTP, Gleam, Node, package-manager, or host runtime version,
  - move the issue to `Blocked on Human`,
  - then stop work and leave the issue alone until a human replies or moves it into a different state.
- Keep both the workpad update and the Linear blocker comment concise and action-oriented.

## Step 2: Execution phase (Todo -> In Progress -> Human Review)

1.  Determine current repo state (`branch`, `git status`, `HEAD`) and verify the kickoff `pull` sync result is already recorded in the workpad before implementation continues.
2.  If current issue state is `Todo`, move it to `In Progress`; otherwise leave the current state unchanged.
3.  Load the existing workpad comment and treat it as the active execution checklist.
    - Edit it liberally whenever reality changes (scope, risks, validation approach, discovered tasks).
4.  Implement against the hierarchical TODOs and keep the comment current:
    - Check off completed items.
    - Add newly discovered items in the appropriate section.
    - Keep parent/child structure intact as scope evolves.
    - Update the workpad immediately after each meaningful milestone (for example: reproduction complete, code change landed, validation run, review feedback addressed).
    - Never leave completed work unchecked in the plan.
    - For tickets that started as `Todo` with an attached PR, run the full PR feedback sweep protocol immediately after kickoff and before new feature work.
5.  Run validation/tests required for the scope.
    - Mandatory gate: execute all ticket-provided `Validation`/`Test Plan`/ `Testing` requirements when present; treat unmet items as incomplete work.
    - Prefer a targeted proof that directly demonstrates the behavior you changed.
    - You may make temporary local proof edits to validate assumptions (for example: tweak a local build input for `make`, or hardcode a UI account / response path) when this increases confidence.
    - Revert every temporary proof edit before commit/push.
    - Document these temporary proof steps and outcomes in the workpad `Validation`/`Notes` sections so reviewers can follow the evidence.
    - If app-touching, run `launch-app` validation and capture/upload media via `github-pr-media` before handoff.
6.  Re-check all acceptance criteria and close any gaps.
7.  Before every `git push` attempt, run the required validation for your scope and confirm it passes; if it fails, address issues and rerun until green, then commit and push changes.
8.  Attach PR URL to the issue (prefer attachment; use the workpad comment only if attachment is unavailable).
    - Ensure the GitHub PR has label `symphony` (add it if missing).
9.  Merge latest `origin/main` into branch, resolve conflicts, and rerun checks.
10. Update the workpad comment with final checklist status and validation notes.
    - Mark completed plan/acceptance/validation checklist items as checked.
    - Add final handoff notes (commit + validation summary) in the same workpad comment.
    - Do not include PR URL in the workpad comment; keep PR linkage on the issue via attachment/link fields.
    - Add a short `### Confusions` section at the bottom when any part of task execution was unclear/confusing, with concise bullets.
    - Do not post any additional completion summary comment.
11. Before moving to `Human Review`, poll PR feedback and checks:
    - Read the PR `Manual QA Plan` comment (when present) and use it to sharpen UI/runtime test coverage for the current change.
    - Run the full PR feedback sweep protocol.
    - Confirm PR checks are passing (green) after the latest changes.
    - Confirm every required ticket-provided validation/test-plan item is explicitly marked complete in the workpad.
    - Repeat this check-address-verify loop until no outstanding comments remain and checks are fully passing.
    - Re-open and refresh the workpad before state transition so `Plan`, `Acceptance Criteria`, and `Validation` exactly match completed work.
12. Only then move issue to `Human Review`.
    - Exception: if blocked by missing required non-GitHub tools/auth per the blocked-access escape hatch, move to `Human Review` with the blocker brief and explicit unblock actions.
    - For required conformance recording/re-recording blocked by invalid or missing
      Shopify credentials, this exception happens before commit/push/PR handoff.
13. For `Todo` tickets that already had a PR attached at kickoff:
    - Ensure all existing PR feedback was reviewed and resolved, including inline review comments (code changes or explicit, justified pushback response).
    - Ensure the original PR branch was updated with any required changes, using `git push --force-with-lease` when necessary instead of opening a new PR.
    - Then move to `Human Review`.

## Step 3: Human Review and merge handling

1. When the issue is in `Human Review`, do not code or change ticket content.
2. Poll for updates as needed in both Linear and GitHub, including Linear issue comments plus GitHub PR review comments and PR comments from humans and bots.
   - For newly seen actionable comments, add `👀` while they are being worked.
3. If either Linear or GitHub feedback indicates additional changes are needed, move the issue to `Rework` and follow the rework flow.
   - After the requested follow-up is complete, replace the `👀` reaction on those comments with `🟢`.
4. If approved, human moves the issue to `Merging`.
5. When the issue is in `Merging`, open and follow `.agents/skills/land/SKILL.md`, then run the `land` skill in a loop until the PR is merged. Do not call `gh pr merge` directly.
6. After merge is complete, move the issue to `Done`.

## Step 4: Rework handling

1. Treat `Rework` as a full approach reset, not incremental patching.
2. Re-read the full issue body and all human comments; explicitly identify what will be done differently this attempt.
3. Keep the existing open PR tied to the issue; do **not** close it just because the issue entered `Rework`.
4. Keep using the original PR branch for the rework unless the PR is already closed/merged.
5. Keep and update the existing `## Codex Workpad` comment in place instead of replacing it with a new one.
6. Refresh the plan/checklist to reflect the new approach, then execute the rework end-to-end on the same branch.
7. Push rework commits back to the original PR branch; if history rewrite is needed, use `git push --force-with-lease`.

## Completion bar before Human Review

For normal PR handoff:

- Step 1/2 checklist is fully complete and accurately reflected in the single workpad comment.
- Acceptance criteria and required ticket-provided validation items are complete.
- Validation/tests are green for the latest commit.
- PR feedback sweep is complete and no actionable comments remain.
- PR checks are green, branch is pushed, and PR is linked on the issue.
- Required PR metadata is present (`symphony` label).
- If app-touching, runtime validation/media requirements from `App runtime validation (required)` are complete.

For required non-GitHub auth/tool blockers, including conformance
recording/re-recording blocked by invalid Shopify credentials, the completion bar
is replaced by the blocked-access escape hatch: no commit, push, or PR is
required or allowed for the blocked handoff.

## Guardrails

- If the branch PR is already closed/merged, do not reuse that branch or prior implementation state for continuation.
- For closed/merged branch PRs, create a new branch from `origin/main` and restart from reproduction/planning as if starting fresh.
- If issue state is `Backlog`, do not modify it; wait for human to move to `Todo`.
- Do not edit the issue body/description for planning or progress tracking.
- Use exactly one persistent workpad comment (`## Codex Workpad`) per issue.
- If comment editing is unavailable in-session, use the update script. Only report blocked if both MCP editing and script-based editing are unavailable.
- Temporary proof edits are allowed only for local verification and must be reverted before commit.
- If out-of-scope improvements are found, create a separate Backlog issue rather
  than expanding current scope, and include a clear
  title/description/acceptance criteria, same-project assignment, a `related`
  link to the current issue, and `blockedBy` when the follow-up depends on the
  current issue.
- Do not move to `Human Review` unless the `Completion bar before Human Review`
  is satisfied, or the blocked-access escape hatch explicitly applies.
- In `Human Review`, do not make changes; wait and poll.
- If state is terminal (`Done`), do nothing and shut down.
- Keep issue text concise, specific, and reviewer-oriented.
- If blocked and no workpad exists yet, add one blocker comment describing blocker, impact, and next unblock action.

## Workpad template

Use this exact structure for the persistent workpad comment and keep it updated in place throughout execution:

````md
## Codex Workpad

```text
<hostname>:<abs-path>@<short-sha>
```

### Plan

- [ ] 1\. Parent task
  - [ ] 1.1 Child task
  - [ ] 1.2 Child task
- [ ] 2\. Parent task

### Acceptance Criteria

- [ ] Criterion 1
- [ ] Criterion 2

### Validation

- [ ] targeted tests: `<command>`

### Notes

- <short progress note with timestamp>

### Confusions

- <only include when something was confusing during execution>
````
