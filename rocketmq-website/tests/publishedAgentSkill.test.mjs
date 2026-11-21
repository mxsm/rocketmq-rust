import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import test from 'node:test';

import {unzipSync} from 'fflate';

test('the published local cluster skill can be discovered and verified', async () => {
    const build = new URL('../build/', import.meta.url);
    const indexPath = '.well-known/agent-skills/index.json';
    const index = JSON.parse(await readFile(new URL(indexPath, build), 'utf8'));
    const skill = index.skills.find(({name}) => name === 'rocketmq-rust-local-cluster');
    assert.equal(skill?.type, 'archive');

    const indexUrl = new URL(indexPath, 'https://rocketmqrust.com/');
    const archiveUrl = new URL(skill.url, indexUrl);
    assert.equal(archiveUrl.origin, indexUrl.origin);
    const archive = await readFile(new URL(`.${archiveUrl.pathname}`, build));
    assert.equal(skill.digest, `sha256:${createHash('sha256').update(archive).digest('hex')}`);

    const files = unzipSync(archive);
    const source = new URL('../../.agents/skills/rocketmq-rust-local-cluster/', import.meta.url);
    for (const file of ['SKILL.md', 'scripts/prepare_cluster.py', 'references/topologies.md']) {
        assert.ok(files[file], `Missing ${file} in the published skill`);
        assert.deepEqual(Buffer.from(files[file]), await readFile(new URL(file, source)));
    }
});
