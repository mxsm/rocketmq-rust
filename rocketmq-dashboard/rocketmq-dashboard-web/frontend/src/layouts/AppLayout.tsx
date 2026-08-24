import type { ReactNode } from 'react';
import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Sheet, SheetContent, SheetDescription, SheetTitle } from '../components/ui/Sheet';
import Sidebar from './Sidebar';
import Header from './Header';

interface AppLayoutProps {
  children: ReactNode;
}

export default function AppLayout({ children }: AppLayoutProps) {
  const [mobileNavigationOpen, setMobileNavigationOpen] = useState(false);
  const [auditWarning, setAuditWarning] = useState<string | null>(null);
  const navigationTriggerRef = useRef<HTMLButtonElement>(null);
  const restoreNavigationFocusRef = useRef(false);
  const navigate = useNavigate();

  useEffect(() => {
    document.documentElement.dataset.theme = 'dark';
  }, []);

  useEffect(() => {
    const onSessionExpired = () => navigate('/login', { replace: true });
    window.addEventListener('rocketmq-auth-expired', onSessionExpired);
    return () => window.removeEventListener('rocketmq-auth-expired', onSessionExpired);
  }, [navigate]);

  useEffect(() => {
    const onAuditWarning = (event: Event) => {
      const warning = event as CustomEvent<string>;
      setAuditWarning(warning.detail || 'The change was applied, but its audit event could not be persisted.');
    };
    window.addEventListener('rocketmq-audit-warning', onAuditWarning);
    return () => window.removeEventListener('rocketmq-audit-warning', onAuditWarning);
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
        <main className="content">
          {auditWarning ? <div className="audit-warning" role="status">{auditWarning}</div> : null}
          {children}
        </main>
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
