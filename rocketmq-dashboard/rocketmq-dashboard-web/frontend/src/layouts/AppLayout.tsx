import type { ReactNode } from 'react';
import { useEffect, useRef, useState } from 'react';
import { Sheet, SheetContent, SheetDescription, SheetTitle } from '../components/ui/Sheet';
import Sidebar from './Sidebar';
import Header from './Header';

interface AppLayoutProps {
  children: ReactNode;
}

export default function AppLayout({ children }: AppLayoutProps) {
  const [mobileNavigationOpen, setMobileNavigationOpen] = useState(false);
  const navigationTriggerRef = useRef<HTMLButtonElement>(null);
  const restoreNavigationFocusRef = useRef(false);

  useEffect(() => {
    document.documentElement.dataset.theme = 'dark';
  }, []);

  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth >= 1024) {
        restoreNavigationFocusRef.current = false;
        setMobileNavigationOpen(false);
      }
    };

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  return (
    <div className="app-shell">
      <div className="desktop-sidebar">
        <Sidebar />
      </div>
      <div className="workspace">
        <Header
          menuButtonRef={navigationTriggerRef}
          onMenuOpen={() => {
            restoreNavigationFocusRef.current = true;
            setMobileNavigationOpen(true);
          }}
        />
        <main className="content">{children}</main>
      </div>
      <Sheet open={mobileNavigationOpen} onOpenChange={setMobileNavigationOpen}>
        <SheetContent
          side="left"
          className="mobile-navigation-sheet"
          onCloseAutoFocus={(event) => {
            event.preventDefault();
            if (restoreNavigationFocusRef.current) {
              navigationTriggerRef.current?.focus();
            }
            restoreNavigationFocusRef.current = false;
          }}
        >
          <SheetTitle className="sr-only">Navigation</SheetTitle>
          <SheetDescription className="sr-only">RocketMQ dashboard workspaces</SheetDescription>
          <Sidebar onNavigate={() => setMobileNavigationOpen(false)} />
        </SheetContent>
      </Sheet>
    </div>
  );
}
