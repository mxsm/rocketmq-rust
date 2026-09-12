import type { ReactNode } from 'react';
import '../../styles/auth.css';

export function AuthLayout({ children }: { children: ReactNode }) {
    return <main className="ops-auth-layout"><div className="ops-auth-shell">{children}</div></main>;
}
