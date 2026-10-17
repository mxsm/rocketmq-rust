import { Pause, Play, RotateCcw } from 'lucide-react';
import { useEffect, useId, useMemo, useRef, useState } from 'react';
import type { GroupSide } from '../TopologyMap';
import { describeTopology, type TopologyView } from '../topology-model';
import SphereFocusPanel from './SphereFocusPanel';
import { buildSphereModel } from './sphere-model';
import { TopologySphereScene } from './sphere-scene';
import './topology-sphere.css';

export interface TopologySphereProps {
  view: TopologyView;
  /** Completed refreshes; a change sends one round of route answers to the client groups. */
  refreshCount: number;
  reducedMotion: boolean;
  /** Stops drawing, for a hidden page. */
  hidden: boolean;
  expanded: Record<GroupSide, boolean>;
  onToggleGroups: (side: GroupSide) => void;
  /** The browser could not create a WebGL context after all. */
  onUnavailable: () => void;
}

/** The sphere view of the topology: a WebGL stage with HTML labels, and a focus panel beside it. */
export default function TopologySphere({ view, refreshCount, reducedMotion, hidden, expanded, onToggleGroups, onUnavailable }: TopologySphereProps) {
  const model = useMemo(() => buildSphereModel(view), [view]);
  const stageRef = useRef<HTMLDivElement>(null);
  const labelsRef = useRef<HTMLDivElement>(null);
  const controlsRef = useRef<HTMLDivElement>(null);
  const sceneRef = useRef<TopologySphereScene | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [paused, setPaused] = useState(false);
  const summaryId = useId();
  // The scene is created once; later values reach it through the effects below.
  const initial = useRef({ reducedMotion, onUnavailable });

  useEffect(() => {
    const stage = stageRef.current;
    const labels = labelsRef.current;
    if (!stage || !labels) return undefined;
    let scene: TopologySphereScene;
    try {
      scene = new TopologySphereScene(
        { stage, labels, controls: controlsRef.current },
        { onSelect: setSelected },
        { reducedMotion: initial.current.reducedMotion }
      );
    } catch {
      initial.current.onUnavailable();
      return undefined;
    }
    sceneRef.current = scene;
    return () => {
      sceneRef.current = null;
      scene.dispose();
    };
  }, []);

  useEffect(() => sceneRef.current?.setModel(model), [model]);
  useEffect(() => sceneRef.current?.setReducedMotion(reducedMotion), [reducedMotion]);
  useEffect(() => sceneRef.current?.setPaused(paused), [paused]);
  useEffect(() => sceneRef.current?.setHidden(hidden), [hidden]);
  useEffect(() => {
    if (refreshCount > 0) sceneRef.current?.markRefresh();
  }, [refreshCount]);

  const pauseLabel = paused ? 'Resume motion' : 'Pause motion';
  return (
    <div className="topology-sphere">
      <div className="topology-sphere-column">
        {/* The scene adds its own canvas as the stage's first child. */}
        <div ref={stageRef} className="topology-sphere-stage" role="group" aria-label="Cluster sphere" aria-describedby={summaryId}>
          <p id={summaryId} className="sr-only">{describeTopology(view)}</p>
          <div ref={labelsRef} className="topology-sphere-labels" />
          <div ref={controlsRef} className="topology-sphere-controls">
            <button type="button" className="topology-sphere-control" aria-label={pauseLabel} title={pauseLabel} aria-pressed={paused} onClick={() => setPaused((value) => !value)}>
              {paused ? <Play size={15} aria-hidden="true" /> : <Pause size={15} aria-hidden="true" />}
            </button>
            <button type="button" className="topology-sphere-control" aria-label="Reset view" title="Reset view" onClick={() => sceneRef.current?.reset()}>
              <RotateCcw size={15} aria-hidden="true" />
            </button>
          </div>
        </div>
        <p className="topology-sphere-hint">Drag a sphere to turn it · select a node to bring it forward and trace its links · Esc clears the focus</p>
      </div>
      <SphereFocusPanel
        model={model}
        selected={selected}
        onSelect={(id) => sceneRef.current?.select(id)}
        expanded={expanded}
        onToggleGroups={onToggleGroups}
      />
    </div>
  );
}
