import {createHash} from 'node:crypto';
import {mkdir, readFile, writeFile} from 'node:fs/promises';
import path from 'node:path';

import type {Plugin} from '@docusaurus/types';
import {DEFAULT_PARSE_FRONT_MATTER, Globby} from '@docusaurus/utils';
import {zipSync} from 'fflate';

const skillName = 'rocketmq-rust-local-cluster';
const discoveryPath = '.well-known/agent-skills';

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

            // ZIP records include timestamps. Keep the download URL stable across unchanged builds.
            const archive = zipSync(files, {mtime: new Date(1980, 0, 1)});
            const digest = createHash('sha256').update(archive).digest('hex');
            const archiveName = `${skillName}-${digest}.zip`;
            const index = {
                $schema: 'https://schemas.agentskills.io/discovery/0.2.0/schema.json',
                skills: [
                    {
                        name,
                        description,
                        type: 'archive',
                        url: `${baseUrl}${discoveryPath}/${archiveName}`,
                        digest: `sha256:${digest}`,
                    },
                ],
            };

            const output = path.join(outDir, discoveryPath);
            await mkdir(output, {recursive: true});
            await writeFile(path.join(output, archiveName), archive);
            await writeFile(path.join(output, 'index.json'), JSON.stringify(index, null, 2) + '\n');
        },
    };
}
