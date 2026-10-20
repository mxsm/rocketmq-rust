/**
 * Colours for Mermaid diagrams, passed to Mermaid as `themeCSS` (see docusaurus.config.ts).
 *
 * Mermaid inlines its theme into every SVG under an id selector, so the site
 * stylesheet cannot restyle a diagram without `!important`. `themeCSS` is
 * appended to that inlined theme instead, and because the SVG is part of the
 * page it can read the tokens that css/custom.css defines for each colour mode.
 *
 * The selectors mirror the ones in Mermaid's flowchart, sequence and state
 * themes, which are the diagram kinds the documentation uses. Only colours and
 * corner radii change: Mermaid lays a diagram out from measured text, so font
 * sizes and weights have to stay as it measured them.
 */
export const diagramThemeCss = `
  /* Flowchart and state nodes */
  .node rect,
  .node circle,
  .node ellipse,
  .node polygon,
  .node path {
    fill: var(--rmq-diagram-node);
    stroke: var(--rmq-diagram-node-border);
  }
  .node rect.label-container {
    rx: 8px;
    ry: 8px;
  }
  .nodeLabel,
  .label text {
    color: var(--ifm-color-content);
    fill: var(--ifm-color-content);
  }

  /* Edges and their labels */
  .flowchart-link,
  .edgePath .path,
  .transition {
    stroke: var(--rmq-diagram-line);
  }
  .marker {
    fill: var(--rmq-diagram-line);
    stroke: var(--rmq-diagram-line);
  }
  /* State transitions end in a marker that carries no class, only a generated id. */
  marker[id$='barbEnd'] path {
    fill: var(--rmq-diagram-line);
  }
  .edgeLabel,
  .edgeLabel p,
  .label div .edgeLabel {
    background-color: var(--rmq-surface);
    color: var(--rmq-text-muted);
  }
  .labelBkg {
    background-color: transparent;
  }

  /* Groups */
  .cluster rect {
    fill: var(--rmq-surface-hover);
    stroke: var(--rmq-border-strong);
    stroke-dasharray: 5 4;
    rx: 10px;
    ry: 10px;
  }
  .cluster-label span,
  .cluster-label text {
    color: var(--rmq-text-muted);
    fill: var(--rmq-text-muted);
  }

  /* State diagram entry point */
  .node circle.state-start {
    fill: var(--rmq-diagram-line);
    stroke: var(--rmq-diagram-line);
  }

  /* Sequence diagrams */
  .actor {
    fill: var(--rmq-diagram-node);
    stroke: var(--rmq-diagram-node-border);
    rx: 8px;
    ry: 8px;
  }
  text.actor > tspan,
  .messageText {
    fill: var(--ifm-color-content);
  }
  .actor-line {
    stroke: var(--rmq-border-strong);
  }
  .messageLine0,
  .messageLine1 {
    stroke: var(--rmq-diagram-line);
  }
  #arrowhead path,
  #crosshead path {
    fill: var(--rmq-diagram-line);
    stroke: var(--rmq-diagram-line);
  }
  .labelBox {
    fill: var(--rmq-diagram-node);
    stroke: var(--rmq-border-strong);
  }
  .loopLine {
    stroke: var(--rmq-border-strong);
  }
  .labelText,
  .labelText > tspan,
  .loopText,
  .loopText > tspan {
    fill: var(--rmq-text-muted);
  }
  .note {
    fill: var(--rmq-accent-soft);
    stroke: var(--rmq-link-underline);
  }
  .noteText,
  .noteText > tspan {
    fill: var(--ifm-color-content);
  }
  .activation0,
  .activation1,
  .activation2 {
    fill: var(--rmq-diagram-node);
    stroke: var(--rmq-diagram-node-border);
  }
`;
