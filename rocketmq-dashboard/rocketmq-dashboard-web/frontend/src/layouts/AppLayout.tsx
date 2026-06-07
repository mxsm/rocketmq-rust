import type { ReactNode } from 'react';
import { useEffect, useState } from 'react';
import Sidebar from './Sidebar';
import Header from './Header';

interface AppLayoutProps {
  children: ReactNode;
}

export default function AppLayout({ children }: AppLayoutProps) {
  const [theme, setTheme] = useState<'light' | 'dark'>(() =>
    window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
  );

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
  }, [theme]);

  return (
    <div className="app-shell">
      <Sidebar />
      <div className="workspace">
        <Header theme={theme} onThemeToggle={() => setTheme((value) => (value === 'dark' ? 'light' : 'dark'))} />
        <main className="content">{children}</main>
      </div>
    </div>
  );
}
