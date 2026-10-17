import { act, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { renderAtRoute } from '../../../test/render';
import { fixtureProducers, fixtureSnapshot } from '../topology-fixtures';
import { ALL_CLUSTERS, selectTopologyView } from '../topology-model';
import TopologySphere, { type TopologySphereProps } from './TopologySphere';
import { TopologySphereScene, type SphereSceneHandlers } from './sphere-scene';

// jsdom has no WebGL; the scene is replaced by a recorder of the calls the component makes.
vi.mock('./sphere-scene', () => ({ TopologySphereScene: vi.fn() }));

const scene = {
  setModel: vi.fn(),
  setReducedMotion: vi.fn(),
  setPaused: vi.fn(),
  setHidden: vi.fn(),
  markRefresh: vi.fn(),
  select: vi.fn(),
  reset: vi.fn(),
  dispose: vi.fn()
};
let handlers: SphereSceneHandlers;

function sphereProps(overrides: Partial<TopologySphereProps> = {}): TopologySphereProps {
  return {
    view: selectTopologyView(fixtureSnapshot(), { scope: ALL_CLUSTERS, producerLimit: 6, consumerLimit: 6 }),
    refreshCount: 1,
    reducedMotion: false,
    hidden: false,
    expanded: { producers: false, consumers: false },
    onToggleGroups: vi.fn(),
    onUnavailable: vi.fn(),
    ...overrides
  };
}

function renderSphere(overrides: Partial<TopologySphereProps> = {}) {
  const props = sphereProps(overrides);
  return { props, ...renderAtRoute(<TopologySphere {...props} />, '/topology') };
}

describe('TopologySphere', () => {
  beforeEach(() => {
    for (const method of Object.values(scene)) method.mockReset();
    vi.mocked(TopologySphereScene).mockImplementation(function construct(_elements, sceneHandlers) {
      handlers = sceneHandlers;
      return scene as unknown as TopologySphereScene;
    });
  });

  it('hands the model to the scene and lists what needs attention, most severe first', () => {
    renderSphere();
    const focus = screen.getByRole('complementary', { name: 'Focus' });

    expect(scene.setModel).toHaveBeenCalledWith(expect.objectContaining({ structure: expect.any(String) }));
    expect(scene.markRefresh).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('group', { name: 'Cluster sphere' })).toHaveAccessibleDescription(/no slave registered: broker-c/);
    const attention = within(focus).getByRole('region', { name: 'Needs attention' });
    expect(within(attention).getAllByRole('button').map((row) => row.textContent)).toEqual([
      '10.0.4.12:9876unavailable',
      'order-consumer1,012 TPS · lag 8,420',
      'payment-consumer486 TPS · lag 4,108'
    ]);
    expect(within(focus).getByRole('region', { name: 'Broker groups · 4' })).toHaveTextContent('broker-cSingle master');
  });

  it('selects a node from the panel and shows its readings, page and links', async () => {
    const user = userEvent.setup();
    renderSphere();
    const focus = screen.getByRole('complementary', { name: 'Focus' });

    await user.click(within(within(focus).getByRole('region', { name: /^Consumer groups/ })).getByRole('button', { name: /^order-consumer/ }));
    expect(scene.select).toHaveBeenCalledWith('consumer:order-consumer');
    act(() => handlers.onSelect('consumer:order-consumer'));

    expect(within(focus).getByRole('heading', { name: 'order-consumer' })).toBeInTheDocument();
    expect(within(focus).getByText('Lag').nextElementSibling).toHaveTextContent('8,420');
    expect(within(focus).getByRole('link', { name: 'Open consumer group' })).toHaveAttribute('href', '/consumers/order-consumer');
    const linked = within(focus).getByRole('region', { name: 'Linked nodes' });
    expect(within(linked).getAllByRole('button').map((row) => row.textContent)).toEqual(['broker-aconsume', 'broker-bconsume', '127.0.0.1:9876route']);
    await user.click(within(focus).getByRole('button', { name: 'Clear focus' }));
    expect(scene.select).toHaveBeenLastCalledWith(null);
  });

  it('offers every group when the view holds only the busiest ones', async () => {
    const user = userEvent.setup();
    const producers = [...fixtureProducers, { topic: 'order-events', producerGroup: 'extra-producer', connectionCount: 1 }];
    const view = selectTopologyView(fixtureSnapshot({ producers }), { scope: ALL_CLUSTERS, producerLimit: 6, consumerLimit: 6 });
    const { props } = renderSphere({ view });

    expect(screen.getByRole('region', { name: 'Producer groups · 6 of 7' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: /^Show all producer groups/ }));
    expect(props.onToggleGroups).toHaveBeenCalledWith('producers');
  });

  it('pauses motion and turns the spheres back on request', async () => {
    const user = userEvent.setup();
    renderSphere();

    await user.click(screen.getByRole('button', { name: 'Pause motion' }));
    expect(scene.setPaused).toHaveBeenLastCalledWith(true);
    expect(screen.getByRole('button', { name: 'Resume motion' })).toHaveAttribute('aria-pressed', 'true');
    await user.click(screen.getByRole('button', { name: 'Reset view' }));
    expect(scene.reset).toHaveBeenCalledTimes(1);
  });

  it('stops drawing while the page is hidden', () => {
    const { rerender, props } = renderSphere();

    rerender(
      <MemoryRouter initialEntries={['/topology']} future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
        <TopologySphere {...props} hidden />
      </MemoryRouter>
    );
    expect(scene.setHidden).toHaveBeenLastCalledWith(true);
  });

  it('reports a browser that cannot start WebGL after all', () => {
    vi.mocked(TopologySphereScene).mockImplementation(function construct() {
      throw new Error('Error creating WebGL context.');
    });
    const { props } = renderSphere();

    expect(props.onUnavailable).toHaveBeenCalledTimes(1);
  });

  it('disposes the scene when it unmounts', () => {
    const { unmount } = renderSphere();

    unmount();
    expect(scene.dispose).toHaveBeenCalledTimes(1);
  });
});
