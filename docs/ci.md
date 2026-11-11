# Continuous integration and the demo gate

`EventGate quality and integration` runs on GitHub's Ubuntu 24.04 x64 runner on
pushes to `main` and on pull requests. It checks out the pushed commit or the pull
request head, records the tested SHA, and verifies that the runner and Docker
server both use Linux x86-64. It installs from the locks, runs Ruff, mypy, and
unit tests, then builds the application and runs integration and CLI tests.
The suite tests Stable and Buggy across all five scenarios. It passes when it
correctly detects the intentional defects in Buggy.

`EventGate demo gate` tests the Candidate application with the runner, Compose resource,
release config, and Baseline from the exact pull request base SHA. It checks out the
exact head SHA into a different directory. It builds both application images in Stable
mode, records the checkout SHAs, and runs the installed trusted CLI. The Candidate
application runs in its container. A nonzero verify status fails the job; the artifact
upload does not change it. The demo gate is independent of the Candidate quality job.

On pull requests, both workflows use the ordinary `pull_request` event, read-only contents permission,
and no production credentials. Separate checkouts do not protect against a pull
request that modifies the workflow itself; review changes to the gate. See [GitHub's
pull request workflow reference](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request).

## Bootstrap and required check

1. Publish the initial commit on `main` to start the Linux acceptance job. Once the
   job completes, record its URL and download `eventgate-test-evidence` from the
   run page. The artifact contains the tested SHA, platform details, JUnit summaries,
   and available comparison reports. Record the completed result against the tested SHA.
2. In a demo pull request, remove the condition around the payment/outbox insert in
   `src/eventgate_demo/stable.py` so it ignores the inbox insert result. The demo job
   should fail on duplicate delivery and the consumer crash. Save the completed run
   URL, head SHA, and report showing the extra payment.
3. Restore the condition in a follow-up commit. Save the completed successful run URL
   and head SHA. A queued run is not acceptance evidence.
4. In branch protection or a repository ruleset, require the status check named
   **EventGate demo gate** for the protected branch. The workflow alone cannot block a
   merge. Use the [GitHub branch protection instructions](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/managing-a-branch-protection-rule)
   and verify that the required check appears on the pull request.

The workflows and their contract tests are available locally. A native hosted Linux
run, the faulty/fixed demo runs, and repository protection have not been verified
until their completed evidence is recorded.
