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
4. To enforce the gate on merges, require the **EventGate demo gate** status
   check in branch protection or a repository ruleset. Verify that GitHub shows
   the check as required on the pull request. See the
   [GitHub branch protection instructions](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/managing-a-branch-protection-rule).

## Hosted acceptance results

On 2026-11-12, the [Linux acceptance run](https://github.com/cryptoadya/eventgate/actions/runs/43186976664)
tested commit `177ba693ba8e3e4a8171481cc6c7735c3866d665` on Ubuntu 24.04 x86-64,
with Python 3.12.16, Docker `linux/amd64`, and Compose 2.38.2. All 89 unit tests
and 47 integration and end-to-end tests passed, with zero skips. Ruff lint and
format checks and mypy passed. With images prepared, Stable / Stable verification
took 206.96 seconds including cleanup. Image preparation took 39 seconds.

The [demonstration pull request](https://github.com/cryptoadya/eventgate/pull/1)
used that commit as its Baseline. Both runs uploaded `eventgate-demo-evidence`
with the checkout SHAs and comparison reports.

| Candidate commit | Gate result | Evidence |
| --- | --- | --- |
| `937fd12c4b7fa72982cb9f7952465f18ddda6db3` | [Expected FAIL](https://github.com/cryptoadya/eventgate/actions/runs/43196027368) | Duplicate delivery and recovery after the database commit each produced two Candidate payments and one Baseline payment. Verify exited 1. |
| `7557f67b789b61979093cdb23ede405b80e3eef6` | [PASS](https://github.com/cryptoadya/eventgate/actions/runs/43196747054) | All five comparisons passed. Both previously failing scenarios produced one payment per version. Verify exited 0. |

The demonstration PR was closed without merging. For future pull requests,
review the gate result manually before merging to `main`.
