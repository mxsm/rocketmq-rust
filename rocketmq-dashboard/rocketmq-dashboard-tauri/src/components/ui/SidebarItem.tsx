import React from 'react';
import { motion } from 'motion/react';

interface SidebarItemProps {
  icon: React.ElementType;
  label: string;
  active: boolean;
  onClick: () => void;
}

export const SidebarItem = ({ icon: Icon, label, active, onClick }: SidebarItemProps) => (
  <button
    onClick={onClick}
    className={`app-nav-item ${active ? 'is-active' : ''}`}
    aria-current={active ? 'page' : undefined}
  >
    <Icon className="app-nav-icon" />
    <span className="app-nav-label">{label}</span>
    {active && (
      <motion.div
        layoutId="active-indicator"
        className="app-nav-marker"
      />
    )}
  </button>
);
