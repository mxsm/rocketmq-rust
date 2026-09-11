export type Theme = 'dark' | 'light';

const storageKey = 'rocketmq-dashboard-theme';
const listeners = new Set<() => void>();

function getSnapshot(): Theme {
    return typeof document !== 'undefined' && document.documentElement.dataset.theme === 'light' ? 'light' : 'dark';
}

function apply(theme: Theme) {
    const root = document.documentElement;
    root.dataset.theme = theme;
    root.classList.toggle('dark', theme === 'dark');
    root.style.colorScheme = theme;
    root.style.backgroundColor = theme === 'dark' ? '#171719' : '#f5f5f7';
    listeners.forEach(listener => listener());
}

function onStorage(event: StorageEvent) {
    if (event.key === storageKey || event.key === null) {
        apply(event.newValue === 'light' ? 'light' : 'dark');
    }
}

function subscribe(listener: () => void) {
    if (listeners.size === 0) window.addEventListener('storage', onStorage);
    listeners.add(listener);
    return () => {
        listeners.delete(listener);
        if (listeners.size === 0) window.removeEventListener('storage', onStorage);
    };
}

function setTheme(theme: Theme) {
    apply(theme);
    try {
        window.localStorage.setItem(storageKey, theme);
    } catch {
        // The current window remains usable when preference storage is unavailable.
    }
}

export const ThemeStore = {
    getSnapshot,
    getServerSnapshot: (): Theme => 'dark',
    subscribe,
    setTheme,
    toggle: () => setTheme(getSnapshot() === 'dark' ? 'light' : 'dark'),
};
