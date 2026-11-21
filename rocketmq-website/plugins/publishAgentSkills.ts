import {createHash} from 'node:crypto';
import {mkdir, readFile, writeFile} from 'node:fs/promises';
import path from 'node:path';

import type {Plugin} from '@docusaurus/types';
import {DEFAULT_PARSE_FRONT_MATTER, Globby} from '@docusaurus/utils';
import {zipSync} from 'fflate';
import {createTarGzip} from 'nanotar';

const skillName = 'rocketmq-rust-local-cluster';
const discoveryPath = '.well-known/agent-skills';

async function writeArchive(directory: string, extension: 'tar.gz' | 'zip', bytes: Uint8Array) {
    const hash = createHash('sha256').update(bytes).digest('hex');
    const filename = `${skillName}-${hash}.${extension}`;
    await writeFile(path.join(directory, filename), bytes);
    return {filename, digest: `sha256:${hash}`};
}

export default function publishAgentSkills(): Plugin<unknown> {
    return {
        name: 'publish-agent-skills',
        async postBuild({siteDir, outDir, baseUrl, i18n}) {
            if (i18n.currentLocale !== i18n.defaultLocale) {
                return;
            }

            const sourceDir = path.resolve(siteDir, '../.agents/skills', skillName);
            const manifestPath = path.join(sourceDir, 'SKILL.md');
            const manifest = await readFile(manifestPath);
            const {frontMatter} = await DEFAULT_PARSE_FRONT_MATTER({
                filePath: manifestPath,
                fileContent: manifest.toString('utf8'),
            });
            const {name, description} = frontMatter;
            if (
                name !== skillName ||
                typeof description !== 'string' ||
                !description.trim() ||
                description.length > 1024
            ) {
                throw new Error(`Invalid discovery name or description in ${manifestPath}`);
            }

            const files: Record<string, Uint8Array> = {'SKILL.md': manifest};
            const resources = await Globby(['agents/*.yaml', 'references/**/*.md', 'scripts/**/*.py'], {
                cwd: sourceDir,
                followSymbolicLinks: false,
                onlyFiles: true,
            });
            for (const resource of resources.sort()) {
                files[resource] = await readFile(path.join(sourceDir, resource));
            }

            const output = path.join(outDir, discoveryPath);
            await mkdir(output, {recursive: true});
            const tarFiles = Object.entries(files).map(([name, data]) => ({name, data}));
            const archive = await createTarGzip(tarFiles, {attrs: {mtime: 0, mode: '644'}});
            const {filename, digest} = await writeArchive(output, 'tar.gz', archive);

            // Previously published indexes point at this ZIP while the new index propagates.
            await writeArchive(output, 'zip', zipSync(files, {mtime: new Date(1980, 0, 1)}));
            const index = {
                $schema: 'https://schemas.agentskills.io/discovery/0.2.0/schema.json',
                skills: [
                    {
                        name,
                        description,
                        type: 'archive',
                        url: `${baseUrl}${discoveryPath}/${filename}`,
                        digest,
                    },
                ],
            };

            await writeFile(path.join(output, 'index.json'), JSON.stringify(index, null, 2) + '\n');
        },
    };
}
