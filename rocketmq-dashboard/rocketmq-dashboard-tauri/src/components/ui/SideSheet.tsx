import React, { useEffect, useMemo, useState } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import { Activity, Check, Copy, Database, ListFilter, Search, Settings2, X } from 'lucide-react';

type SheetType = 'Status' | 'Config' | null;

interface SideSheetProps {
  isOpen: boolean;
  onClose: () => void;
  title: string;
  data: Record<string, unknown>;
  type?: SheetType;
}

interface DetailEntry {
  key: string;
  value: string;
  category: string;
  valueType: 'boolean' | 'empty' | 'number' | 'text';
}

const CATEGORY_ORDER = ['All', 'Broker', 'Runtime', 'Storage', 'Messaging', 'Security', 'Other'];

const categorizeKey = (key: string) => {
  const normalized = key.toLowerCase();

  if (normalized.includes('acl') || normalized.includes('auth') || normalized.includes('permission')) {
    return 'Security';
  }

  if (
    normalized.includes('disk') ||
    normalized.includes('commitlog') ||
    normalized.includes('consumequeue') ||
    normalized.includes('flush') ||
    normalized.includes('store')
  ) {
    return 'Storage';
  }

  if (
    normalized.includes('topic') ||
    normalized.includes('message') ||
    normalized.includes('queue') ||
    normalized.includes('subscription') ||
    normalized.includes('dispatch')
  ) {
    return 'Messaging';
  }

  if (
    normalized.includes('tps') ||
    normalized.includes('ratio') ||
    normalized.includes('offset') ||
    normalized.includes('time') ||
    normalized.includes('timestamp') ||
    normalized.includes('active')
  ) {
    return 'Runtime';
  }

  if (
    normalized.includes('broker') ||
    normalized.includes('name') ||
    normalized.includes('addr') ||
    normalized.includes('port') ||
    normalized.includes('cluster') ||
    normalized.includes('version')
  ) {
    return 'Broker';
  }

  return 'Other';
};

const getValueType = (value: string): DetailEntry['valueType'] => {
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return 'empty';
  }

  if (normalized === 'true' || normalized === 'false') {
    return 'boolean';
  }

  if (!Number.isNaN(Number(normalized))) {
    return 'number';
  }

  return 'text';
};

const inferType = (title: string, explicitType?: SheetType): Exclude<SheetType, null> => {
  if (explicitType) {
    return explicitType;
  }

  return title.toLowerCase().startsWith('status') ? 'Status' : 'Config';
};

const extractBrokerLabel = (title: string) => {
  const match = title.match(/\[(.+?)]\[(.+?)]/);
  return match ? `${match[1]}-${match[2]}` : title;
};

export const SideSheet = ({ isOpen, onClose, title, data, type }: SideSheetProps) => {
  const [query, setQuery] = useState('');
  const [activeCategory, setActiveCategory] = useState('All');
  const [copiedKey, setCopiedKey] = useState('');

  useEffect(() => {
    if (isOpen) {
      setQuery('');
      setActiveCategory('All');
      setCopiedKey('');
    }
  }, [isOpen, title]);

  const sheetType = inferType(title, type);

  const entries = useMemo<DetailEntry[]>(
    () =>
      Object.entries(data).map(([key, value]) => {
        const stringValue = value == null ? '' : String(value);
        return {
          key,
          value: stringValue,
          category: categorizeKey(key),
          valueType: getValueType(stringValue),
        };
      }),
    [data]
  );

  const brokerAddr = entries.find((entry) => entry.key.toLowerCase() === 'brokeraddr')?.value;
  const filledCount = entries.filter((entry) => entry.valueType !== 'empty').length;
  const booleanCount = entries.filter((entry) => entry.valueType === 'boolean').length;
  const numericCount = entries.filter((entry) => entry.valueType === 'number').length;

  const categorySummary = useMemo(
    () =>
      CATEGORY_ORDER.map((category) => ({
        name: category,
        count: category === 'All' ? entries.length : entries.filter((entry) => entry.category === category).length,
      })).filter((category) => category.name === 'All' || category.count > 0),
    [entries]
  );

  const filteredEntries = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();

    return entries.filter((entry) => {
      const matchesCategory = activeCategory === 'All' || entry.category === activeCategory;
      const matchesQuery =
        !normalizedQuery ||
        entry.key.toLowerCase().includes(normalizedQuery) ||
        entry.value.toLowerCase().includes(normalizedQuery);

      return matchesCategory && matchesQuery;
    });
  }, [activeCategory, entries, query]);

  const handleCopy = async (key: string, value: string) => {
    try {
      await navigator.clipboard.writeText(value);
      setCopiedKey(key);
      window.setTimeout(() => setCopiedKey(''), 1200);
    } catch (error) {
      console.error('Failed to copy detail value', error);
    }
  };

  const handleCopyAll = async () => {
    const content = entries.map((entry) => `${entry.key}=${entry.value}`).join('\n');
    await handleCopy('__all__', content);
  };

  const Icon = sheetType === 'Status' ? Activity : Settings2;

  return (
    <AnimatePresence>
      {isOpen && (
        <>
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 0.46 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
            className="ops-sheet-backdrop"
          />
          <motion.aside
            initial={{ x: '100%' }}
            animate={{ x: 0 }}
            exit={{ x: '100%' }}
            transition={{ type: 'spring', stiffness: 300, damping: 30 }}
            className={`ops-sheet ops-detail-sheet is-${sheetType.toLowerCase()}`}
            aria-label={`${sheetType} details`}
          >
            <header className="ops-detail-header">
              <div className="ops-detail-title-wrap">
                <span className="ops-detail-icon">
                  <Icon className="ops-detail-icon-svg" />
                </span>
                <div>
                  <span className="ops-detail-kicker">{sheetType} Inspector</span>
                  <h2 className="ops-sheet-title">{title}</h2>
                  <p>
                    {brokerAddr ? `${brokerAddr} · ` : ''}
                    {extractBrokerLabel(title)} · {entries.length} entries · {filledCount} filled
                  </p>
                </div>
              </div>
              <button onClick={onClose} className="ops-icon-button ops-detail-close" aria-label="Close details">
                <X className="ops-detail-close-icon" />
              </button>
            </header>

            <section className="ops-detail-toolbar" aria-label="Detail filters">
              <label className="ops-detail-search">
                <Search className="ops-detail-search-icon" />
                <span className="sr-only">Search detail entries</span>
                <input
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                  placeholder="Search key or value"
                />
              </label>
              <button type="button" className="ops-detail-tool-button" onClick={() => void handleCopyAll()}>
                {copiedKey === '__all__' ? <Check className="ops-button-icon" /> : <Copy className="ops-button-icon" />}
                Copy all
              </button>
            </section>

            <section className="ops-detail-summary" aria-label="Detail summary">
              <div>
                <span>Entries</span>
                <strong>{entries.length}</strong>
              </div>
              <div>
                <span>Filled</span>
                <strong>{filledCount}</strong>
              </div>
              <div>
                <span>Boolean</span>
                <strong>{booleanCount}</strong>
              </div>
              <div>
                <span>Numeric</span>
                <strong>{numericCount}</strong>
              </div>
            </section>

            <div className="ops-detail-body">
              <nav className="ops-detail-categories" aria-label="Detail categories">
                <div className="ops-detail-category-title">
                  <ListFilter className="ops-button-icon" />
                  Categories
                </div>
                {categorySummary.map((category) => (
                  <button
                    type="button"
                    key={category.name}
                    onClick={() => setActiveCategory(category.name)}
                    className={activeCategory === category.name ? 'is-active' : undefined}
                  >
                    <span>{category.name}</span>
                    <strong>{category.count}</strong>
                  </button>
                ))}
              </nav>

              <section className="ops-detail-list" aria-label={`${sheetType} key value entries`}>
                <div className="ops-detail-list-head">
                  <span>Key</span>
                  <span>Value</span>
                  <span>Action</span>
                </div>
                <div className="ops-detail-rows">
                  {filteredEntries.length ? (
                    filteredEntries.map((entry) => (
                      <article key={entry.key} className={`ops-detail-row is-${entry.valueType}`}>
                        <div className="ops-detail-key">
                          <strong>{entry.key}</strong>
                          <span>{entry.category}</span>
                        </div>
                        <code className="ops-detail-value">{entry.value || 'Not set'}</code>
                        <button
                          type="button"
                          className="ops-icon-button ops-detail-copy"
                          onClick={() => void handleCopy(entry.key, entry.value)}
                          aria-label={`Copy ${entry.key}`}
                        >
                          {copiedKey === entry.key ? <Check className="ops-button-icon" /> : <Copy className="ops-button-icon" />}
                        </button>
                      </article>
                    ))
                  ) : (
                    <div className="ops-detail-no-results">
                      Try another key, value, or category filter.
                    </div>
                  )}
                </div>
              </section>
            </div>
          </motion.aside>
        </>
      )}
    </AnimatePresence>
  );
};
