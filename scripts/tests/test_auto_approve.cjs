// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');

const workflow = readFileSync(
  path.join(__dirname, '../../.github/workflows/auto_approve_pull_requests.yml'), 'utf8',
);
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
function workflowScript(index) {
  const lines = workflow.split(/\r?\n/);
  const starts = lines.flatMap((line, offset) => line === '          script: |' ? [offset + 1] : []);
  assert.ok(starts[index], `Approval workflow must contain script ${index}`);
  const body = [];
  for (const line of lines.slice(starts[index])) {
    if (line.trim() && !line.startsWith('            ')) break;
    body.push(line.slice(12));
  }
  return new AsyncFunction('github', 'context', 'core', 'process', body.join('\n'));
}
const evaluate = workflowScript(0);
const contextRepo = { owner: 'owner', repo: 'repo' };
const pullRequest = {
  number: 7, state: 'open', draft: false,
  base: { ref: 'main', repo: { full_name: 'owner/repo' } }, head: { sha: 'current-head' },
};
const completion = pull_requests => ({
  workflow_run: { event: 'pull_request', head_sha: 'current-head', pull_requests },
});

async function ready({
  checks = [], statuses = [], required = ['Linux', 'Coverage'], appChecks = [],
  pr = pullRequest, associated = [pullRequest],
  payload = { check_suite: { head_sha: 'current-head', pull_requests: [{ number: 7 }] } },
  outputs = {},
} = {}) {
  const listForRef = () => {};
  const listAssociated = () => {};
  return evaluate({
    rest: {
      pulls: { get: async () => ({ data: pr }) },
      repos: {
        getBranch: async () => ({ data: { protection: {
          required_status_checks: { contexts: required, checks: appChecks },
        } } }),
        getCombinedStatusForRef: async () => ({ data: { statuses } }),
        listPullRequestsAssociatedWithCommit: listAssociated,
      },
      checks: { listForRef },
    },
    paginate: async (method, options) => {
      if (method === listForRef) {
        assert.equal(options.ref, 'current-head');
        return checks;
      }
      assert.equal(method, listAssociated);
      assert.equal(options.commit_sha, 'current-head');
      return associated;
    },
  }, { repo: contextRepo, payload }, {
    setOutput(name, value) { outputs[name] = value; }, info() {},
  });
}

const success = name => ({ name, status: 'completed', conclusion: 'success' });

test('missing required checks do not approve', async () => {
  assert.equal(await ready({ checks: [success('Linux')] }), false);
  assert.equal(await ready(), false);
});
test('pending or failed required checks do not approve', async () => {
  for (const check of [
    { name: 'Coverage', status: 'in_progress' },
    { name: 'Coverage', status: 'completed', conclusion: 'failure' },
  ]) assert.equal(await ready({ checks: [success('Linux'), check] }), false);
});
test('explicitly skipped optional work satisfies its existing required context', async () => {
  assert.equal(await ready({ checks: [
    success('Linux'), { name: 'Coverage', status: 'completed', conclusion: 'skipped' },
  ] }), true);
});
test('unrelated failed checks do not add new approval gates', async () => {
  assert.equal(await ready({ checks: [
    success('Linux'), success('Coverage'),
    { name: 'Optional audit', status: 'completed', conclusion: 'failure' },
  ] }), true);
});
test('legacy commit statuses are honored', async () => {
  assert.equal(await ready({
    checks: [success('Linux')], statuses: [{ context: 'Coverage', state: 'success' }],
  }), true);
  assert.equal(await ready({
    checks: [success('Linux'), success('Coverage')],
    statuses: [{ context: 'Coverage', state: 'failure' }],
  }), false);
});
test('drafts and missing branch protection do not receive automatic approval', async () => {
  assert.equal(await ready({ checks: [success('Linux'), success('Coverage')],
    pr: { ...pullRequest, draft: true } }), false);
  assert.equal(await ready({ required: [] }), false);
});

test('Actions completion resolves the PR and returns the checked commit', async () => {
  const outputs = {};
  assert.equal(await ready({ payload: completion([{ number: 7 }]), outputs,
    checks: [success('Linux'), success('Coverage')] }), true);
  assert.equal(outputs.pull_request_number, 7);
  assert.equal(outputs.checked_commit, 'current-head');
});

test('Actions completion with no PR array resolves an associated open PR', async () => {
  assert.equal(await ready({ payload: completion([]),
    checks: [success('Linux'), success('Coverage')] }), true);
  assert.equal(await ready({ payload: completion([]), associated: [],
    checks: [success('Linux'), success('Coverage')] }), false);
});

test('stale completion cannot approve a newer PR head', async () => {
  assert.equal(await ready({ payload: completion([{ number: 7 }]),
    pr: { ...pullRequest, head: { sha: 'newer-head' } },
    checks: [success('Linux'), success('Coverage')] }), false);
});

test('closed PRs and push workflow completions do not approve', async () => {
  assert.equal(await ready({ pr: { ...pullRequest, state: 'closed' },
    checks: [success('Linux'), success('Coverage')] }), false);
  assert.equal(await ready({ payload: { workflow_run: {
    ...completion([{ number: 7 }]).workflow_run, event: 'push',
  } }, checks: [success('Linux'), success('Coverage')] }), false);
});

test('a required GitHub App check cannot be supplied by another app or a legacy status', async () => {
  const options = { required: ['Linux'], appChecks: [{ context: 'Linux', app_id: 15368 }] };
  assert.equal(await ready({ ...options,
    checks: [{ ...success('Linux'), app: { id: 15368 } }] }), true);
  assert.equal(await ready({ ...options,
    checks: [{ ...success('Linux'), app: { id: 123 } }] }), false);
  assert.equal(await ready({ ...options,
    statuses: [{ context: 'Linux', state: 'success' }] }), false);
});

async function approve(pr) {
  const reviews = [];
  await workflowScript(1)({ rest: { pulls: {
    get: async () => ({ data: pr }),
    createReview: async review => { reviews.push(review); },
  } } }, { repo: contextRepo }, { info() {} }, {
    env: { PR_NUMBER: '7', CHECKED_COMMIT: 'current-head' },
  });
  return reviews;
}

test('approval is attached to the checked commit', async () => {
  const reviews = await approve(pullRequest);
  assert.equal(reviews.length, 1);
  assert.equal(reviews[0].commit_id, 'current-head');
  assert.equal(reviews[0].event, 'APPROVE');
});

test('a push, closure, or draft conversion between checking and approval cancels approval', async () => {
  for (const pr of [
    { ...pullRequest, head: { sha: 'newer-head' } },
    { ...pullRequest, state: 'closed' },
    { ...pullRequest, draft: true },
  ]) assert.deepEqual(await approve(pr), []);
});
