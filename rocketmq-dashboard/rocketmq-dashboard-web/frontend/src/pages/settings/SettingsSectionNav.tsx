import { Database, Network, ShieldCheck } from 'lucide-react';
import { Button } from '../../components/ui/Button';
import { cn } from '../../lib/cn';

export type SettingsSection = 'connection' | 'security' | 'storage';

interface SettingsSectionNavProps {
  activeSection: SettingsSection;
  onSelect: (section: SettingsSection) => void;
}

const sections = [
  { id: 'connection' as const, label: 'Connection', description: 'NameServers and selection', icon: Network },
  { id: 'security' as const, label: 'Security', description: 'VIP and TLS transport', icon: ShieldCheck },
  { id: 'storage' as const, label: 'Storage', description: 'Persistence backend', icon: Database }
];

export default function SettingsSectionNav({ activeSection, onSelect }: SettingsSectionNavProps) {
  return (
    <nav className="settings-section-nav" aria-label="OPS settings sections">
      {sections.map((section) => {
        const Icon = section.icon;
        const isActive = activeSection === section.id;
        return (
          <Button
            key={section.id}
            type="button"
            variant="ghost"
            className={cn('settings-section-link', isActive && 'is-active')}
            aria-label={section.label}
            aria-current={isActive ? 'page' : undefined}
            onClick={() => onSelect(section.id)}
          >
            <Icon size={16} aria-hidden="true" />
            <span>
              <strong>{section.label}</strong>
              <small className="sr-only">{section.description}</small>
            </span>
          </Button>
        );
      })}
    </nav>
  );
}
