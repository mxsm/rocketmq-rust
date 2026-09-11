import { readFileSync } from 'node:fs';
import { runInNewContext } from 'node:vm';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ThemeStore } from './theme';

const bootstrap = readFileSync(new URL('../../public/theme.js', import.meta.url), 'utf8');

function environment(saved: string | null = null, restricted = false) {
    const root = { dataset: {} as Record<string, string>, style: {} as Record<string, string>, classList: { toggle: vi.fn() } };
    const storage = {
        getItem: vi.fn(() => { if (restricted) throw new Error('Unavailable'); return saved; }),
        setItem: vi.fn(() => { if (restricted) throw new Error('Unavailable'); }),
    };
    const window = { localStorage: storage, addEventListener: vi.fn(), removeEventListener: vi.fn() };
    const document = { documentElement: root };
    runInNewContext(bootstrap, { localStorage: storage, document });
    vi.stubGlobal('document', document);
    vi.stubGlobal('window', window);
    return { root, storage, window };
}

afterEach(() => vi.unstubAllGlobals());

describe('document theme lifecycle', () => {
    it.each([null, 'invalid', 'system', 'dark'])('defaults to dark before React for preference %s', saved => {
        const { root } = environment(saved);
        expect(ThemeStore.getSnapshot()).toBe('dark');
        expect(root.style.colorScheme).toBe('dark');
        expect(root.classList.toggle).toHaveBeenLastCalledWith('dark', true);
    });

    it('restores an explicit light preference before React subscribes', () => {
        const { root, storage } = environment('light');
        expect(ThemeStore.getSnapshot()).toBe('light');
        expect(root.style.colorScheme).toBe('light');
        expect(storage.setItem).not.toHaveBeenCalled();
    });

    it('notifies all mounted theme consumers without remounting them and persists the selection', () => {
        const { root, storage } = environment();
        const first = vi.fn();
        const second = vi.fn();
        const unsubscribeFirst = ThemeStore.subscribe(first);
        const unsubscribeSecond = ThemeStore.subscribe(second);
        try {
            ThemeStore.toggle();
            expect(ThemeStore.getSnapshot()).toBe('light');
            expect(root.classList.toggle).toHaveBeenLastCalledWith('dark', false);
            expect(first).toHaveBeenCalledTimes(1);
            expect(second).toHaveBeenCalledTimes(1);
            expect(storage.setItem).toHaveBeenCalledWith('rocketmq-dashboard-theme', 'light');
            ThemeStore.toggle();
            expect(ThemeStore.getSnapshot()).toBe('dark');
        } finally {
            unsubscribeFirst();
            unsubscribeSecond();
        }
    });

    it('can start and change theme when preference reads and writes fail', () => {
        environment(null, true);
        expect(ThemeStore.getSnapshot()).toBe('dark');
        expect(() => ThemeStore.toggle()).not.toThrow();
        expect(ThemeStore.getSnapshot()).toBe('light');
    });

    it('syncs another window, resets on preference removal, and cleans up the listener', () => {
        const { window, storage } = environment();
        const unsubscribe = ThemeStore.subscribe(vi.fn());
        try {
            const onStorage = window.addEventListener.mock.calls[0][1] as (event: Partial<StorageEvent>) => void;
            onStorage({ key: 'unrelated', newValue: 'light' });
            expect(ThemeStore.getSnapshot()).toBe('dark');
            onStorage({ key: 'rocketmq-dashboard-theme', newValue: 'light' });
            expect(ThemeStore.getSnapshot()).toBe('light');
            onStorage({ key: null, newValue: null });
            expect(ThemeStore.getSnapshot()).toBe('dark');
            expect(storage.setItem).not.toHaveBeenCalled();
        } finally {
            unsubscribe();
        }
        expect(window.removeEventListener).toHaveBeenCalledWith('storage', window.addEventListener.mock.calls[0][1]);
    });
});
