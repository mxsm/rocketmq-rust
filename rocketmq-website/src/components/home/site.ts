/** Project facts and links shared by the homepage sections. */

/** Latest published release. Bump together with the workspace version when a release ships. */
export const VERSION = '1.0.0';
/** Members of the root Cargo workspace (`[workspace].members` in the repository manifest). */
export const WORKSPACE_CRATES = 28;

export const GITHUB_REPO = 'mxsm/rocketmq-rust';
export const GITHUB_URL = `https://github.com/${GITHUB_REPO}`;
export const RELEASE_URL = `${GITHUB_URL}/releases/tag/v${VERSION}`;
export const DISCUSSIONS_URL = `${GITHUB_URL}/discussions`;
export const GOOD_FIRST_ISSUES_URL = `${GITHUB_URL}/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22`;
export const CRATES_URL = 'https://crates.io/crates/rocketmq-client-rust';

export const INSTALL_COMMAND = 'cargo add rocketmq-client-rust';

/** Shown until the live GitHub count arrives, and when the request fails. */
export const FALLBACK_STARS = 1500;

/** Formats a count the way GitHub does: 987, 1.5k, 12k. */
export function formatCompact(value: number): string {
  if (value < 1000) {
    return String(value);
  }
  const thousands = value / 1000;
  return `${thousands >= 10 ? Math.round(thousands) : thousands.toFixed(1).replace(/\.0$/, '')}k`;
}
