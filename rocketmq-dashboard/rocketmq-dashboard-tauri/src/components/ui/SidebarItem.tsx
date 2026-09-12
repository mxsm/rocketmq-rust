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
    className={`desktop-nav-item ${active ? 'is-active' : ''}`}
    aria-current={active ? 'page' : undefined}
  >
    <Icon className="desktop-nav-icon" aria-hidden="true" />
    <span>{label}</span>
  </button>
);
