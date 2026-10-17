import { describe, expect, it } from 'vitest';
import { placeLabels, type LabelObstacle, type LabelRequest } from './sphere-labels';

const stage = { width: 800, height: 600 };

function label(id: string, overrides: Partial<LabelRequest> = {}): LabelRequest {
  return { id, priority: 50, x: 400, y: 300, width: 100, height: 30, placement: 'side', centreX: 400, owner: null, ...overrides };
}

const positions = (requests: LabelRequest[], obstacles: LabelObstacle[] = []) =>
  Object.fromEntries(placeLabels(requests, obstacles, stage).map(({ id, x, y }) => [id, [x, y]]));

describe('placeLabels', () => {
  it('puts a side label on the side away from the centre', () => {
    expect(positions([label('right', { x: 500 }), label('left', { x: 300, y: 100 })])).toEqual({
      right: [513, 285],
      left: [187, 85]
    });
  });

  it('moves a label to a free position and drops a lower one when none is left', () => {
    const placed = positions([label('third', { x: 500 }), label('first', { x: 500, priority: 90 }), label('second', { x: 500, priority: 60 })]);

    expect(placed).toEqual({ first: [513, 285], second: [387, 285] });
  });

  it('keeps labels inside the stage', () => {
    expect(positions([label('edge', { x: 780 })])).toEqual({ edge: [667, 285] });
  });

  it('lets an owner overlap its own obstacle but nobody else', () => {
    const disc: LabelObstacle = { x: 600, y: 250, width: 200, height: 100, owner: 'nameservers' };

    expect(positions([label('broker', { x: 780 })], [disc])).toEqual({});
    expect(positions([label('nameserver', { x: 780, owner: 'nameservers' })], [disc])).toEqual({ nameserver: [667, 285] });
  });

  it('pulls a key label back inside the stage above its node', () => {
    const blocked: LabelObstacle = { x: 640, y: 295, width: 160, height: 60, owner: null };

    expect(positions([label('master', { x: 780, priority: 90 })], [blocked])).toEqual({ master: [696, 261] });
  });

  it('shows the selected label even when every position is taken', () => {
    const everything: LabelObstacle = { x: 0, y: 0, width: 800, height: 600, owner: null };

    expect(positions([label('selected', { x: 500, priority: 1000 }), label('other', { x: 300 })], [everything])).toEqual({ selected: [513, 285] });
  });

  it('centres titles over, under or on their anchor', () => {
    expect(positions([
      label('over', { placement: 'above', y: 100 }),
      label('under', { placement: 'below', y: 500 }),
      label('on', { placement: 'centre', x: 200 })
    ])).toEqual({ over: [350, 60], under: [350, 510], on: [150, 285] });
  });
});
