import { Check, ChevronDown, Search } from 'lucide-react';
import { useEffect, useId, useMemo, useRef, useState } from 'react';

export interface SelectMenuOption {
  value: string;
  label: string;
  disabled?: boolean;
}

interface SelectMenuProps {
  value: string;
  options: SelectMenuOption[];
  onChange: (value: string) => void;
  ariaLabel: string;
  className?: string;
  searchable?: boolean;
  searchPlaceholder?: string;
}

export default function SelectMenu({ value, options, onChange, ariaLabel, className, searchable = false, searchPlaceholder = 'Search' }: SelectMenuProps) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const menuId = useId();
  const rootRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const selected = options.find((option) => option.value === value);
  const normalizedQuery = query.trim().toLowerCase();
  const filteredOptions = useMemo(() => {
    if (!searchable || normalizedQuery === '') {
      return options;
    }
    return options.filter((option) => `${option.label} ${option.value}`.toLowerCase().includes(normalizedQuery));
  }, [normalizedQuery, options, searchable]);

  useEffect(() => {
    if (!open) return;
    const closeOnOutsidePress = (event: PointerEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setOpen(false);
      }
    };
    document.addEventListener('pointerdown', closeOnOutsidePress);
    document.addEventListener('keydown', closeOnEscape);
    return () => {
      document.removeEventListener('pointerdown', closeOnOutsidePress);
      document.removeEventListener('keydown', closeOnEscape);
    };
  }, [open]);

  useEffect(() => {
    if (!open) {
      setQuery('');
      return;
    }
    if (searchable) {
      window.requestAnimationFrame(() => searchRef.current?.focus());
    }
  }, [open, searchable]);

  return (
    <div className={`select-menu ${className ?? ''}`} ref={rootRef}>
      <button
        type="button"
        className="select-menu-trigger"
        aria-expanded={open}
        aria-haspopup="listbox"
        aria-controls={menuId}
        aria-label={ariaLabel}
        onClick={() => setOpen((value) => !value)}
      >
        <span>{selected?.label ?? value}</span>
        <ChevronDown size={14} aria-hidden="true" />
      </button>
      {open ? (
        <div className="select-menu-popover">
          {searchable ? (
            <label className="select-menu-search">
              <Search size={14} aria-hidden="true" />
              <input
                ref={searchRef}
                value={query}
                placeholder={searchPlaceholder}
                aria-label={`${ariaLabel} search`}
                onChange={(event) => setQuery(event.target.value)}
              />
            </label>
          ) : null}
          <div className="select-menu-options" id={menuId} role="listbox" aria-label={ariaLabel}>
            {filteredOptions.map((option) => (
              <button
                type="button"
                className="select-menu-option"
                role="option"
                aria-selected={option.value === value}
                disabled={option.disabled}
                key={option.value}
                onClick={() => {
                  onChange(option.value);
                  setOpen(false);
                }}
              >
                <span>{option.label}</span>
                {option.value === value ? <Check size={14} aria-hidden="true" /> : null}
              </button>
            ))}
            {filteredOptions.length === 0 ? (
              <div className="select-menu-empty" role="presentation">
                No matching options
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}
