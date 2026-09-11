import { useSyncExternalStore } from 'react';
import { ThemeStore } from '../stores/theme';

export function useTheme() {
  const theme = useSyncExternalStore(ThemeStore.subscribe, ThemeStore.getSnapshot, ThemeStore.getServerSnapshot);
  return { theme, isDark: theme === 'dark', toggleTheme: ThemeStore.toggle, setTheme: ThemeStore.setTheme };
}
