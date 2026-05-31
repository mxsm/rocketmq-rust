import React from 'react';

interface AuthLayoutProps {
  children: React.ReactNode;
}

export const AuthLayout = ({ children }: AuthLayoutProps) => {
  return (
    <div className="auth-console">
      <div className="auth-console-grid" aria-hidden="true" />
      <div className="auth-shell">
        {children}
      </div>
    </div>
  );
};
