import React from 'react';

interface SidebarItemProps {
  icon: React.ElementType;
  label: string;
  active: boolean;
  onClick: () => void;
}

export const SidebarItem = ({ icon: Icon, label, active, onClick }: SidebarItemProps) => (
  <button
    type="button"
    onClick={onClick}
    className={`app-nav-item ${active ? 'is-active' : ''}`}
    aria-current={active ? 'page' : undefined}
  >
    <Icon className="app-nav-icon" />
    <span className="app-nav-label">{label}</span>
  </button>
);
