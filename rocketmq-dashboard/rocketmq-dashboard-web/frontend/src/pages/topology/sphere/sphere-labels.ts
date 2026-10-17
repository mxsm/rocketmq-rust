/**
 * Placement of the HTML labels over the sphere stage. Labels are placed by priority; a label that
 * would overlap one already placed tries its other positions and is hidden when none is free.
 */

export type LabelPlacement = 'side' | 'near' | 'above' | 'below' | 'centre';

export interface LabelRequest {
  id: string;
  /** Higher priorities are placed first. */
  priority: number;
  /** Anchor point on the stage, in CSS pixels. */
  x: number;
  y: number;
  width: number;
  height: number;
  /**
   * `side` and `near` put the label beside its anchor, away from `centreX` when there is room;
   * `above` and `below` centre it over or under the anchor; `centre` centres it on the anchor.
   */
  placement: LabelPlacement;
  centreX: number;
  /** Obstacles with the same owner do not block this label. */
  owner: string | null;
}

export interface LabelObstacle {
  x: number;
  y: number;
  width: number;
  height: number;
  owner: string | null;
}

export interface PlacedLabel {
  id: string;
  x: number;
  y: number;
}

/** Gap between a node and its label beside it. */
const SIDE_GAP = 13;
/** Smaller gap for the single-line labels drawn next to a master. */
const NEAR_GAP = 9;
const TITLE_GAP = 10;
/** Distance kept from the stage edges. */
const EDGE = 4;
/** Labels at or above this priority may move above or below their node to stay inside the stage. */
export const KEY_PRIORITY = 70;
/** Labels at or above this priority are shown even when every position is taken. */
export const FORCED_PRIORITY = 900;

interface Box {
  x: number;
  y: number;
  width: number;
  height: number;
}

function candidates(request: LabelRequest, stage: { width: number; height: number }): Box[] {
  const { x, y, width, height } = request;
  const box = (left: number, top: number) => ({ x: left, y: top, width, height });
  switch (request.placement) {
    case 'above':
      return [box(x - width / 2, y - height - TITLE_GAP)];
    case 'below':
      return [box(x - width / 2, y + TITLE_GAP)];
    case 'centre':
      return [box(x - width / 2, y - height / 2)];
    default: {
      const gap = request.placement === 'near' ? NEAR_GAP : SIDE_GAP;
      const right = x + gap;
      const left = x - gap - width;
      const outward = x >= request.centreX ? right : left;
      const inward = outward === right ? left : right;
      const options = [box(outward, y - height / 2), box(inward, y - height / 2), box(outward, y - height - 3), box(outward, y + 3)];
      if (request.priority >= KEY_PRIORITY) {
        // Pulled back inside the stage, above or below the node, for nodes close to an edge.
        const pulled = Math.min(Math.max(x - width / 2, EDGE), stage.width - width - EDGE);
        options.push(box(pulled, y - height - 9), box(pulled, y + 9));
      }
      return options;
    }
  }
}

function inside(box: Box, stage: { width: number; height: number }) {
  return box.x >= EDGE && box.y >= EDGE && box.x + box.width <= stage.width - EDGE && box.y + box.height <= stage.height - EDGE;
}

function clear(box: Box, others: readonly LabelObstacle[], owner: string | null) {
  return others.every(
    (other) =>
      (other.owner !== null && other.owner === owner)
      || box.x >= other.x + other.width + 4
      || other.x >= box.x + box.width + 4
      || box.y >= other.y + other.height + 2
      || other.y >= box.y + box.height + 2
  );
}

function clamp(box: Box, stage: { width: number; height: number }): Box {
  return {
    ...box,
    x: Math.min(Math.max(box.x, EDGE), stage.width - box.width - EDGE),
    y: Math.min(Math.max(box.y, EDGE), stage.height - box.height - EDGE)
  };
}

/**
 * Places `requests` in priority order, keeping clear of `obstacles` and of each other. Requests of
 * equal priority keep their order. Returns the labels that found a place.
 */
export function placeLabels(
  requests: readonly LabelRequest[],
  obstacles: readonly LabelObstacle[],
  stage: { width: number; height: number }
): PlacedLabel[] {
  const taken: LabelObstacle[] = [...obstacles];
  const placed: PlacedLabel[] = [];
  const ordered = requests.map((request, index) => ({ request, index })).sort((left, right) => right.request.priority - left.request.priority || left.index - right.index);
  for (const { request } of ordered) {
    const options = candidates(request, stage);
    let chosen = options.find((box) => inside(box, stage) && clear(box, taken, request.owner));
    if (!chosen && request.priority >= FORCED_PRIORITY) chosen = clamp(options[0], stage);
    if (!chosen) continue;
    taken.push({ ...chosen, owner: null });
    placed.push({ id: request.id, x: chosen.x, y: chosen.y });
  }
  return placed;
}
