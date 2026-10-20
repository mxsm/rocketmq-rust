/**
 * Wraps the stock Mermaid component so wide diagrams stay legible.
 *
 * Mermaid renders an SVG that shrinks to the column width, which turns the
 * labels of a wide flowchart into specks. The wrapper publishes the diagram's
 * natural width as a CSS variable; css/content.css uses it to stop shrinking at
 * a readable scale and lets the card scroll sideways instead.
 */

import React, {useEffect, useRef} from 'react';
import Mermaid from '@theme-original/Mermaid';
import type {Props} from '@theme/Mermaid';

const NATURAL_WIDTH_PROPERTY = '--rmq-diagram-width';

export default function MermaidWrapper(props: Props): React.JSX.Element {
  const hostRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const host = hostRef.current;
    if (!host) {
      return undefined;
    }

    // Mermaid writes the natural width of the diagram as an inline max-width on the SVG.
    const publish = (): void => {
      const naturalWidth = parseFloat(host.querySelector('svg')?.style.maxWidth ?? '');
      if (Number.isFinite(naturalWidth)) {
        host.style.setProperty(NATURAL_WIDTH_PROPERTY, `${naturalWidth}px`);
      } else {
        host.style.removeProperty(NATURAL_WIDTH_PROPERTY);
      }
    };

    publish();
    // The SVG arrives after an async render and is replaced when the colour mode changes.
    const observer = new MutationObserver(publish);
    observer.observe(host, {childList: true, subtree: true});
    return () => observer.disconnect();
  }, []);

  return (
    <div ref={hostRef} className="rmq-diagram">
      <Mermaid {...props} />
    </div>
  );
}
