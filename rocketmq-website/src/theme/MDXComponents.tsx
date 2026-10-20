/**
 * Extends the default MDX component map used by documentation pages and release notes.
 */

import React from 'react';
import MDXComponents from '@theme-original/MDXComponents';

/**
 * Markdown tables render inside a scroll container. The container carries the
 * border and rounded corners (see css/content.css) and lets a wide table scroll
 * sideways instead of stretching the page.
 */
function Table(props: React.ComponentProps<'table'>): React.JSX.Element {
  return (
    <div className="rmq-table-wrap">
      <table {...props} />
    </div>
  );
}

export default {
  ...MDXComponents,
  table: Table,
};
