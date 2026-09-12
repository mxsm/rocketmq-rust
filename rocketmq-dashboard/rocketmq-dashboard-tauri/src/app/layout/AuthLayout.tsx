import type { ReactNode } from 'react';
import '../../styles/auth.css';

export function AuthLayout({ children }: { children: ReactNode }) {
    return (
        <main className="auth-console">
            <div className="auth-console-grid" aria-hidden="true" />
            <div className="auth-shell">{children}</div>
        </main>
    );
}
