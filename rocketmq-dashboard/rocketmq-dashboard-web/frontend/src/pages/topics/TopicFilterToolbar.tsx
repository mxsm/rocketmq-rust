import { ListFilter } from 'lucide-react';
import type { ReactNode } from 'react';
import QueryToolbar from '../../components/QueryToolbar';
import { Button } from '../../components/ui/Button';
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger
} from '../../components/ui/DropdownMenu';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '../../components/ui/Select';
import type { TopicCategory, TopicFilters, TopicMessageType } from './topic-model';

interface TopicFilterToolbarProps {
  filters: TopicFilters;
  clusterOptions: string[];
  brokerOptions: string[];
  onFiltersChange: (filters: TopicFilters) => void;
  actions?: ReactNode;
}

const messageTypeOptions: Array<{ value: TopicMessageType; label: string }> = [
  { value: 'NORMAL', label: 'Normal' },
  { value: 'DELAY', label: 'Delay' },
  { value: 'FIFO', label: 'FIFO' },
  { value: 'TRANSACTION', label: 'Transaction' },
  { value: 'UNSPECIFIED', label: 'Unspecified' }
];

const categoryOptions: Array<{ value: TopicCategory; label: string }> = [
  { value: 'APPLICATION', label: 'Application' },
  { value: 'RETRY', label: 'Retry' },
  { value: 'DLQ', label: 'DLQ' },
  { value: 'SYSTEM', label: 'System' }
];

export default function TopicFilterToolbar({
  filters,
  clusterOptions,
  brokerOptions,
  onFiltersChange,
  actions
}: TopicFilterToolbarProps) {
  const update = <Key extends keyof TopicFilters>(key: Key, value: TopicFilters[Key]) => {
    onFiltersChange({ ...filters, [key]: value });
  };

  const reset = () => {
    onFiltersChange({
      query: '',
      brokerName: 'all',
      clusterName: 'all',
      messageTypes: [],
      categories: []
    });
  };

  return (
    <QueryToolbar
      searchValue={filters.query}
      searchPlaceholder="Filter topics"
      onSearchChange={(value) => update('query', value)}
      onReset={reset}
      actions={actions}
    >
      <MultiSelectFilter
        label="Message types"
        allLabel="All types"
        countLabel="types"
        options={messageTypeOptions}
        selected={filters.messageTypes}
        onChange={(messageTypes) => update('messageTypes', messageTypes)}
      />
      <MultiSelectFilter
        label="Categories"
        allLabel="All categories"
        countLabel="categories"
        options={categoryOptions}
        selected={filters.categories}
        onChange={(categories) => update('categories', categories)}
      />
      <div className="native-filter-field">
        <span>Cluster</span>
        <Select value={filters.clusterName} onValueChange={(value) => update('clusterName', value)}>
          <SelectTrigger aria-label="Cluster filter">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All clusters</SelectItem>
            {clusterOptions.map((cluster) => <SelectItem key={cluster} value={cluster}>{cluster}</SelectItem>)}
          </SelectContent>
        </Select>
      </div>
      <div className="native-filter-field">
        <span>Broker</span>
        <Select value={filters.brokerName} onValueChange={(value) => update('brokerName', value)}>
          <SelectTrigger aria-label="Broker filter">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All brokers</SelectItem>
            {brokerOptions.map((broker) => <SelectItem key={broker} value={broker}>{broker}</SelectItem>)}
          </SelectContent>
        </Select>
      </div>
    </QueryToolbar>
  );
}

interface MultiSelectFilterProps<Value extends string> {
  label: string;
  allLabel: string;
  countLabel: string;
  options: Array<{ value: Value; label: string }>;
  selected: Value[];
  onChange: (selected: Value[]) => void;
}

function MultiSelectFilter<Value extends string>({
  label,
  allLabel,
  countLabel,
  options,
  selected,
  onChange
}: MultiSelectFilterProps<Value>) {
  const summary = selectionSummary(selected, options, allLabel, countLabel);

  return (
    <DropdownMenu modal={false}>
      <DropdownMenuTrigger asChild>
        <Button type="button" variant="outline" size="sm" aria-label={`${label}: ${summary}`}>
          <ListFilter size={14} aria-hidden="true" />
          {summary}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start">
        <DropdownMenuLabel>{label}</DropdownMenuLabel>
        <DropdownMenuSeparator />
        {options.map((option) => (
          <DropdownMenuCheckboxItem
            key={option.value}
            checked={selected.includes(option.value)}
            aria-label={option.label}
            onCheckedChange={(checked) => {
              onChange(checked === true
                ? [...selected, option.value]
                : selected.filter((value) => value !== option.value));
            }}
          >
            {option.label}
          </DropdownMenuCheckboxItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function selectionSummary<Value extends string>(
  selected: Value[],
  options: Array<{ value: Value; label: string }>,
  allLabel: string,
  countLabel: string
) {
  if (selected.length === 0) return allLabel;
  if (selected.length === 1) return options.find((option) => option.value === selected[0])?.label ?? selected[0];
  return `${selected.length} ${countLabel}`;
}
