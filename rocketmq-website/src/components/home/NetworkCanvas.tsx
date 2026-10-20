import React, {useEffect, useRef} from 'react';

type NetworkCanvasProps = {
  className?: string;
};

type MeshNode = {
  x: number;
  y: number;
  vx: number;
  vy: number;
  radius: number;
  /** Hubs stand in for brokers: they glow, emit heartbeats, and originate most traffic. */
  hub: boolean;
  tint: number;
  phase: number;
  /** Decaying highlight set when a pulse arrives or the pointer is near. */
  heat: number;
};

type Pulse = {
  from: number;
  to: number;
  progress: number;
  speed: number;
  tint: number;
  hops: number;
};

/** Produce, route, and consume accents shared with the rest of the homepage. */
const TINTS: ReadonlyArray<readonly [number, number, number]> = [
  [255, 132, 48],
  [152, 114, 255],
  [66, 208, 240],
];
const TINT_CSS = TINTS.map(([r, g, b]) => `rgb(${r}, ${g}, ${b})`);
const TINT_CLEAR = TINTS.map(([r, g, b]) => `rgba(${r}, ${g}, ${b}, 0)`);
const IDLE_NODE_CSS = 'rgb(168, 184, 228)';
const EDGE_CSS = 'rgb(128, 148, 214)';
const POINTER_CSS = 'rgb(255, 160, 92)';

const POINTER_RADIUS = 190;
const MAX_CANVAS_PIXELS = 3_400_000;
const TAU = Math.PI * 2;

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

/**
 * Decorative hero backdrop: a drifting mesh of nodes with message pulses hopping
 * between neighbours. Pauses off-screen and renders a single still frame when
 * the visitor prefers reduced motion.
 */
export default function NetworkCanvas({className}: NetworkCanvasProps): React.JSX.Element {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    const context = canvas?.getContext('2d');
    if (!canvas || !context) {
      return undefined;
    }

    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)');
    const pointer = {x: 0, y: 0, active: false};

    let width = 0;
    let height = 0;
    let linkDistance = 150;
    let maxPulses = 0;
    let nodes: MeshNode[] = [];
    let neighbors: number[][] = [];
    // Flat [from, to, strength] triples rebuilt every frame.
    let edges: number[] = [];
    let pulses: Pulse[] = [];
    let frame = 0;
    let lastTime = 0;
    let onScreen = false;

    const seed = (): void => {
      const target = clamp(Math.round((width * height) / 15500), 24, 96);
      const columns = Math.max(1, Math.round(Math.sqrt((target * width) / Math.max(height, 1))));
      const rows = Math.max(1, Math.ceil(target / columns));
      const count = columns * rows;
      const cellWidth = width / columns;
      const cellHeight = height / rows;

      nodes = [];
      for (let index = 0; index < count; index += 1) {
        const column = index % columns;
        const row = Math.floor(index / columns);
        const hub = Math.random() < 0.14;
        const angle = Math.random() * TAU;
        const drift = 3 + Math.random() * 6;
        nodes.push({
          // Jittered grid keeps coverage even without looking regular.
          x: (column + 0.15 + Math.random() * 0.7) * cellWidth,
          y: (row + 0.15 + Math.random() * 0.7) * cellHeight,
          vx: Math.cos(angle) * drift,
          vy: Math.sin(angle) * drift,
          radius: 1 + Math.random() * 0.9,
          hub,
          tint: Math.floor(Math.random() * TINTS.length),
          phase: Math.random() * TAU,
          heat: 0,
        });
      }
      neighbors = nodes.map(() => []);
      pulses = [];
      linkDistance = clamp(Math.min(width, height) * 0.26, 110, 185);
      maxPulses = clamp(Math.round(count * 0.34), 8, 30);
    };

    const spawnPulse = (): void => {
      for (let attempt = 0; attempt < 6; attempt += 1) {
        const from = Math.floor(Math.random() * nodes.length);
        const options = neighbors[from];
        // Bias traffic toward hubs so the mesh reads as brokers exchanging messages.
        if (options.length === 0 || (!nodes[from].hub && Math.random() < 0.55)) {
          continue;
        }
        pulses.push({
          from,
          to: options[Math.floor(Math.random() * options.length)],
          progress: 0,
          speed: 90 + Math.random() * 110,
          tint: nodes[from].hub ? nodes[from].tint : Math.floor(Math.random() * TINTS.length),
          hops: 2 + Math.floor(Math.random() * 4),
        });
        return;
      }
    };

    const update = (delta: number): void => {
      for (const node of nodes) {
        node.x += node.vx * delta;
        node.y += node.vy * delta;
        if ((node.x < -24 && node.vx < 0) || (node.x > width + 24 && node.vx > 0)) {
          node.vx = -node.vx;
        }
        if ((node.y < -24 && node.vy < 0) || (node.y > height + 24 && node.vy > 0)) {
          node.vy = -node.vy;
        }
        node.heat = Math.max(0, node.heat - delta * 1.5);
      }

      for (const list of neighbors) {
        list.length = 0;
      }
      edges.length = 0;
      const limit = linkDistance * linkDistance;
      for (let a = 0; a < nodes.length; a += 1) {
        for (let b = a + 1; b < nodes.length; b += 1) {
          const dx = nodes[a].x - nodes[b].x;
          const dy = nodes[a].y - nodes[b].y;
          const distanceSquared = dx * dx + dy * dy;
          if (distanceSquared < limit) {
            neighbors[a].push(b);
            neighbors[b].push(a);
            edges.push(a, b, 1 - Math.sqrt(distanceSquared) / linkDistance);
          }
        }
      }

      for (let index = pulses.length - 1; index >= 0; index -= 1) {
        const pulse = pulses[index];
        const source = nodes[pulse.from];
        const target = nodes[pulse.to];
        const distance = Math.hypot(target.x - source.x, target.y - source.y) || 1;
        pulse.progress += (pulse.speed * delta) / distance;
        if (pulse.progress < 1) {
          continue;
        }

        target.heat = 1;
        const options = neighbors[pulse.to];
        if (pulse.hops > 0 && options.length > 1) {
          let next = options[Math.floor(Math.random() * options.length)];
          if (next === pulse.from) {
            next = options[(options.indexOf(next) + 1) % options.length];
          }
          pulse.from = pulse.to;
          pulse.to = next;
          pulse.progress = 0;
          pulse.hops -= 1;
        } else {
          pulses.splice(index, 1);
        }
      }

      if (pulses.length < maxPulses && Math.random() < delta * 9) {
        spawnPulse();
      }
    };

    const fillCircle = (x: number, y: number, radius: number): void => {
      context.beginPath();
      context.arc(x, y, radius, 0, TAU);
      context.fill();
    };

    const render = (time: number): void => {
      context.clearRect(0, 0, width, height);

      context.lineWidth = 1;
      context.strokeStyle = EDGE_CSS;
      for (let index = 0; index < edges.length; index += 3) {
        const source = nodes[edges[index]];
        const target = nodes[edges[index + 1]];
        context.globalAlpha = edges[index + 2] * 0.2;
        context.beginPath();
        context.moveTo(source.x, source.y);
        context.lineTo(target.x, target.y);
        context.stroke();
      }

      if (pointer.active) {
        context.strokeStyle = POINTER_CSS;
        for (const node of nodes) {
          const distance = Math.hypot(node.x - pointer.x, node.y - pointer.y);
          if (distance < POINTER_RADIUS) {
            const strength = 1 - distance / POINTER_RADIUS;
            node.heat = Math.max(node.heat, strength);
            context.globalAlpha = strength * 0.42;
            context.beginPath();
            context.moveTo(node.x, node.y);
            context.lineTo(pointer.x, pointer.y);
            context.stroke();
          }
        }
      }

      for (const node of nodes) {
        const color = TINT_CSS[node.tint];
        if (node.hub) {
          const breathe = 0.5 + 0.5 * Math.sin(time * 1.4 + node.phase);
          context.fillStyle = color;
          context.globalAlpha = 0.09 + 0.07 * breathe + node.heat * 0.2;
          fillCircle(node.x, node.y, 9 + 4 * breathe);

          // Heartbeat ring, echoing brokers reporting to the NameServer.
          const ring = (time * 0.22 + node.phase) % 1;
          context.strokeStyle = color;
          context.globalAlpha = (1 - ring) * 0.3;
          context.beginPath();
          context.arc(node.x, node.y, 6 + ring * 30, 0, TAU);
          context.stroke();

          context.globalAlpha = 0.95;
          fillCircle(node.x, node.y, 2.6);
        } else {
          context.fillStyle = node.heat > 0.04 ? color : IDLE_NODE_CSS;
          context.globalAlpha = 0.34 + node.heat * 0.62;
          fillCircle(node.x, node.y, node.radius + node.heat * 1.3);
        }
      }

      context.lineCap = 'round';
      for (const pulse of pulses) {
        const source = nodes[pulse.from];
        const target = nodes[pulse.to];
        const headX = source.x + (target.x - source.x) * pulse.progress;
        const headY = source.y + (target.y - source.y) * pulse.progress;
        const tail = Math.max(0, pulse.progress - 0.24);
        const tailX = source.x + (target.x - source.x) * tail;
        const tailY = source.y + (target.y - source.y) * tail;

        const gradient = context.createLinearGradient(tailX, tailY, headX, headY);
        gradient.addColorStop(0, TINT_CLEAR[pulse.tint]);
        gradient.addColorStop(1, TINT_CSS[pulse.tint]);
        context.globalAlpha = 0.95;
        context.strokeStyle = gradient;
        context.lineWidth = 1.8;
        context.beginPath();
        context.moveTo(tailX, tailY);
        context.lineTo(headX, headY);
        context.stroke();

        context.fillStyle = TINT_CSS[pulse.tint];
        context.globalAlpha = 0.24;
        fillCircle(headX, headY, 5.5);
        context.fillStyle = '#ffffff';
        context.globalAlpha = 1;
        fillCircle(headX, headY, 1.5);
      }
      context.lineCap = 'butt';
      context.globalAlpha = 1;
    };

    const tick = (now: number): void => {
      frame = window.requestAnimationFrame(tick);
      // Clamp so a throttled or resumed tab cannot teleport the mesh.
      const delta = Math.min(0.05, lastTime ? (now - lastTime) / 1000 : 0.016);
      lastTime = now;
      update(delta);
      render(now / 1000);
    };

    const stop = (): void => {
      window.cancelAnimationFrame(frame);
      frame = 0;
      lastTime = 0;
    };

    const sync = (): void => {
      const shouldRun = onScreen && !document.hidden && !reducedMotion.matches;
      if (shouldRun && !frame) {
        frame = window.requestAnimationFrame(tick);
      } else if (!shouldRun && frame) {
        stop();
      }
      if (!shouldRun && reducedMotion.matches) {
        pulses = [];
        update(0);
        render(0);
      }
    };

    const resize = (): void => {
      const bounds = canvas.getBoundingClientRect();
      const nextWidth = Math.round(bounds.width);
      const nextHeight = Math.round(bounds.height);
      if (nextWidth === 0 || nextHeight === 0) {
        return;
      }
      // Mobile browser chrome resizes the viewport constantly; only rebuild the mesh for real changes.
      const reseed = nodes.length === 0 || nextWidth !== width || Math.abs(nextHeight - height) > 140;
      width = nextWidth;
      height = nextHeight;
      // Cap the backing store so very large displays do not pay for pixels the soft glow cannot show.
      const ratio = Math.max(1, Math.min(window.devicePixelRatio || 1, 2, Math.sqrt(MAX_CANVAS_PIXELS / (width * height))));
      canvas.width = Math.round(width * ratio);
      canvas.height = Math.round(height * ratio);
      context.setTransform(ratio, 0, 0, ratio, 0, 0);
      if (reseed) {
        seed();
      }
      if (!frame) {
        update(0);
        render(0);
      }
    };

    const handlePointerMove = (event: PointerEvent): void => {
      if (event.pointerType === 'touch') {
        return;
      }
      const bounds = canvas.getBoundingClientRect();
      pointer.x = event.clientX - bounds.left;
      pointer.y = event.clientY - bounds.top;
      pointer.active = pointer.x >= 0 && pointer.x <= bounds.width && pointer.y >= 0 && pointer.y <= bounds.height;
    };
    const handlePointerLeave = (): void => {
      pointer.active = false;
    };

    const resizeObserver = new ResizeObserver(resize);
    resizeObserver.observe(canvas);
    const visibilityObserver = new IntersectionObserver(([entry]) => {
      onScreen = entry.isIntersecting;
      sync();
    });
    visibilityObserver.observe(canvas);

    resize();
    window.addEventListener('pointermove', handlePointerMove, {passive: true});
    document.documentElement.addEventListener('pointerleave', handlePointerLeave);
    document.addEventListener('visibilitychange', sync);
    reducedMotion.addEventListener('change', sync);

    return () => {
      stop();
      resizeObserver.disconnect();
      visibilityObserver.disconnect();
      window.removeEventListener('pointermove', handlePointerMove);
      document.documentElement.removeEventListener('pointerleave', handlePointerLeave);
      document.removeEventListener('visibilitychange', sync);
      reducedMotion.removeEventListener('change', sync);
    };
  }, []);

  return <canvas ref={canvasRef} className={className} aria-hidden="true" />;
}
