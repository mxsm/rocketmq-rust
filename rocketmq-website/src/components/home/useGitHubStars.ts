import {useEffect, useState} from 'react';
import {FALLBACK_STARS, GITHUB_REPO} from './site';

const CACHE_KEY = 'rmq-home:github-stars';
// Unauthenticated GitHub API calls are limited to 60 per hour per address; one per visitor per hour is plenty.
const CACHE_TTL_MS = 60 * 60 * 1000;

type CachedStars = {
  value: number;
  savedAt: number;
};

function readCache(): number | null {
  try {
    const raw = window.localStorage.getItem(CACHE_KEY);
    if (!raw) {
      return null;
    }
    const cached = JSON.parse(raw) as Partial<CachedStars>;
    if (
      typeof cached.value === 'number' &&
      typeof cached.savedAt === 'number' &&
      Date.now() - cached.savedAt < CACHE_TTL_MS
    ) {
      return cached.value;
    }
  } catch {
    // Storage disabled or the entry is malformed: fall back to the network.
  }
  return null;
}

function writeCache(value: number): void {
  try {
    const entry: CachedStars = {value, savedAt: Date.now()};
    window.localStorage.setItem(CACHE_KEY, JSON.stringify(entry));
  } catch {
    // Storage disabled or full: the count is simply fetched again next time.
  }
}

/**
 * Returns the repository's star count, starting from a static fallback and
 * upgrading to the live value once GitHub answers. Failures keep the fallback.
 */
export function useGitHubStars(): number {
  const [stars, setStars] = useState(FALLBACK_STARS);

  useEffect(() => {
    const cached = readCache();
    if (cached !== null) {
      setStars(cached);
      return undefined;
    }

    const controller = new AbortController();
    fetch(`https://api.github.com/repos/${GITHUB_REPO}`, {
      signal: controller.signal,
      headers: {Accept: 'application/vnd.github+json'},
    })
      .then((response) => {
        if (!response.ok) {
          throw new Error(`GitHub responded with ${response.status}`);
        }
        return response.json() as Promise<{stargazers_count?: unknown}>;
      })
      .then((repository) => {
        const value = repository.stargazers_count;
        if (typeof value === 'number' && Number.isFinite(value)) {
          setStars(value);
          writeCache(value);
        }
      })
      .catch(() => {
        // Offline, rate limited, or aborted on unmount: the fallback stays on screen.
      });

    return () => controller.abort();
  }, []);

  return stars;
}
