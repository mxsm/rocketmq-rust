import React from 'react';
import { AuthStarfield } from './AuthStarfield';

interface AuthLayoutProps {
  children: React.ReactNode;
}

export const AuthLayout = ({ children }: AuthLayoutProps) => {
  return (
    <div className="min-h-screen w-full bg-gray-950 flex items-center justify-center relative overflow-hidden">
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top_right,_var(--tw-gradient-stops))] from-blue-900/25 via-gray-950 to-gray-950" />
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_bottom_left,_var(--tw-gradient-stops))] from-indigo-900/18 via-gray-950 to-gray-950" />
      <AuthStarfield />

      <div className="relative z-10 w-full max-w-6xl mx-auto p-4 md:p-8 flex flex-col md:flex-row items-center justify-between gap-12">
        {children}
      </div>
    </div>
  );
};
