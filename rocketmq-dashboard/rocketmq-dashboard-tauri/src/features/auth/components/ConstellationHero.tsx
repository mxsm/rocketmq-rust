import React, { useEffect, useRef, useState } from 'react';
import rocketImage from 'rocketmq-rust:asset/rocketmq-rust.png';

interface NodeSpec {
  id: string;
  x: number;
  y: number;
  size: number;
  glow: number;
}

interface EdgeSpec {
  from: string;
  to: string;
}

interface Point {
  x: number;
  y: number;
}

const constellationNodes: NodeSpec[] = [
  { id: 'dubhe', x: 0.18, y: 0.2, size: 5.2, glow: 1 },
  { id: 'merak', x: 0.28, y: 0.35, size: 4.8, glow: 0.92 },
  { id: 'phecda', x: 0.43, y: 0.42, size: 4.3, glow: 0.86 },
  { id: 'megrez', x: 0.54, y: 0.28, size: 4.2, glow: 0.82 },
  { id: 'alioth', x: 0.67, y: 0.24, size: 4.9, glow: 0.95 },
  { id: 'mizar', x: 0.79, y: 0.34, size: 4.6, glow: 0.88 },
  { id: 'alkaid', x: 0.9, y: 0.47, size: 5.4, glow: 1 },
  { id: 'trace-1', x: 0.23, y: 0.62, size: 3.1, glow: 0.55 },
  { id: 'trace-2', x: 0.38, y: 0.72, size: 2.8, glow: 0.48 },
  { id: 'trace-3', x: 0.7, y: 0.66, size: 2.9, glow: 0.46 },
  { id: 'trace-4', x: 0.84, y: 0.18, size: 2.7, glow: 0.42 },
];

const constellationEdges: EdgeSpec[] = [
  { from: 'dubhe', to: 'merak' },
  { from: 'merak', to: 'phecda' },
  { from: 'phecda', to: 'megrez' },
  { from: 'megrez', to: 'alioth' },
  { from: 'alioth', to: 'mizar' },
  { from: 'mizar', to: 'alkaid' },
  { from: 'dubhe', to: 'megrez' },
  { from: 'trace-1', to: 'merak' },
  { from: 'trace-2', to: 'phecda' },
  { from: 'trace-3', to: 'mizar' },
  { from: 'trace-4', to: 'alioth' },
];

const clamp = (value: number, min: number, max: number) => {
  return Math.min(max, Math.max(min, value));
};

export const ConstellationHero = () => {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState({ width: 500, height: 500 });
  const [pointer, setPointer] = useState<Point | null>(null);
  const [draggedId, setDraggedId] = useState<string | null>(null);
  const [dragOffsets, setDragOffsets] = useState<Record<string, Point>>({});

  useEffect(() => {
    const node = containerRef.current;

    if (!node) {
      return;
    }

    const updateSize = () => {
      setSize({
        width: node.clientWidth || 500,
        height: node.clientHeight || 500,
      });
    };

    updateSize();

    const observer = new ResizeObserver(() => {
      updateSize();
    });

    observer.observe(node);

    const handlePointerUp = () => {
      setDraggedId(null);
    };

    window.addEventListener('pointerup', handlePointerUp);

    return () => {
      observer.disconnect();
      window.removeEventListener('pointerup', handlePointerUp);
    };
  }, []);

  const getBasePosition = (node: NodeSpec) => {
    return {
      x: node.x * size.width,
      y: node.y * size.height,
    };
  };

  const getDisplayPosition = (node: NodeSpec) => {
    const base = getBasePosition(node);
    const dragOffset = dragOffsets[node.id] ?? { x: 0, y: 0 };
    let x = base.x + dragOffset.x;
    let y = base.y + dragOffset.y;

    if (pointer) {
      const dx = pointer.x - x;
      const dy = pointer.y - y;
      const distance = Math.hypot(dx, dy);
      const magneticRadius = 170;

      if (distance < magneticRadius) {
        const influence = (1 - distance / magneticRadius) * 18;
        x += (dx / Math.max(distance, 1)) * influence;
        y += (dy / Math.max(distance, 1)) * influence;
      }
    }

    return { x, y };
  };

  const displayNodes = constellationNodes.map((node) => {
    return {
      ...node,
      position: getDisplayPosition(node),
    };
  });

  const nodeMap = new Map(displayNodes.map((node) => [node.id, node]));

  const getPointerPoint = (clientX: number, clientY: number) => {
    const rect = containerRef.current?.getBoundingClientRect();

    if (!rect) {
      return null;
    }

    return {
      x: clientX - rect.left,
      y: clientY - rect.top,
    };
  };

  const handlePointerMove = (event: React.PointerEvent<HTMLDivElement>) => {
    const nextPointer = getPointerPoint(event.clientX, event.clientY);

    if (!nextPointer) {
      return;
    }

    setPointer(nextPointer);

    if (draggedId) {
      const node = constellationNodes.find((item) => item.id === draggedId);

      if (!node) {
        return;
      }

      const base = getBasePosition(node);
      const maxOffsetX = size.width * 0.14;
      const maxOffsetY = size.height * 0.14;

      setDragOffsets((current) => ({
        ...current,
        [draggedId]: {
          x: clamp(nextPointer.x - base.x, -maxOffsetX, maxOffsetX),
          y: clamp(nextPointer.y - base.y, -maxOffsetY, maxOffsetY),
        },
      }));
    }
  };

  const handlePointerLeave = () => {
    setPointer(null);
    setDraggedId(null);
  };

  return (
    <div
      ref={containerRef}
      className="relative w-full max-w-[560px] aspect-square flex items-center justify-center"
      onPointerMove={handlePointerMove}
      onPointerLeave={handlePointerLeave}
    >
      <style>
        {`
          @keyframes auth-constellation-pulse {
            0%, 100% {
              opacity: 0.55;
              transform: scale(0.92);
            }
            50% {
              opacity: 1;
              transform: scale(1.18);
            }
          }

          @keyframes auth-constellation-float {
            0%, 100% {
              transform: translate3d(0, 0, 0);
            }
            50% {
              transform: translate3d(0, -8px, 0);
            }
          }

          @keyframes auth-constellation-trace {
            0%, 100% {
              stroke-dashoffset: 0;
              opacity: 0.42;
            }
            50% {
              stroke-dashoffset: -14;
              opacity: 0.9;
            }
          }
        `}
      </style>

      <div className="absolute inset-0 rounded-full bg-[radial-gradient(circle_at_center,_rgba(59,130,246,0.18),_transparent_55%)] blur-[100px]" />
      <div className="absolute inset-[6%] rounded-full border border-sky-300/10 bg-[radial-gradient(circle_at_50%_45%,_rgba(125,211,252,0.08),_transparent_60%)] backdrop-blur-[2px]" />

      <svg
        viewBox={`0 0 ${size.width} ${size.height}`}
        className="absolute inset-0 h-full w-full overflow-visible"
        aria-hidden="true"
      >
        <defs>
          <filter id="constellation-glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="3.5" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          <linearGradient id="constellation-line" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="rgba(255,255,255,0.2)" />
            <stop offset="50%" stopColor="rgba(125,211,252,0.95)" />
            <stop offset="100%" stopColor="rgba(255,255,255,0.12)" />
          </linearGradient>
        </defs>

        {constellationEdges.map((edge) => {
          const from = nodeMap.get(edge.from);
          const to = nodeMap.get(edge.to);

          if (!from || !to) {
            return null;
          }

          const midX = (from.position.x + to.position.x) / 2;
          const midY = (from.position.y + to.position.y) / 2 - size.height * 0.035;
          const pointerBoost = pointer
            ? Math.max(
                0,
                1 - Math.hypot(pointer.x - midX, pointer.y - midY) / 220,
              )
            : 0;
          const isActive = draggedId === edge.from || draggedId === edge.to;
          const opacity = 0.22 + pointerBoost * 0.48 + (isActive ? 0.2 : 0);

          return (
            <path
              key={`${edge.from}-${edge.to}`}
              d={`M ${from.position.x} ${from.position.y} Q ${midX} ${midY} ${to.position.x} ${to.position.y}`}
              fill="none"
              stroke="url(#constellation-line)"
              strokeWidth={1.2 + pointerBoost * 0.8}
              strokeLinecap="round"
              strokeDasharray="8 10"
              opacity={opacity}
              style={{
                filter: 'drop-shadow(0 0 7px rgba(125, 211, 252, 0.38))',
                animation: `auth-constellation-trace ${4.6 - pointerBoost * 1.8}s linear infinite`,
              }}
            />
          );
        })}

        {displayNodes.map((node) => {
          const pointerBoost = pointer
            ? Math.max(
                0,
                1 - Math.hypot(pointer.x - node.position.x, pointer.y - node.position.y) / 150,
              )
            : 0;
          const active = draggedId === node.id;
          const haloRadius = 18 + pointerBoost * 24;
          const fillOpacity = 0.7 + pointerBoost * 0.3;

          return (
            <g
              key={node.id}
              transform={`translate(${node.position.x}, ${node.position.y})`}
              style={{
                cursor: active ? 'grabbing' : 'grab',
                animation: `auth-constellation-float ${4.5 + node.glow * 2}s ease-in-out infinite`,
              }}
              onPointerDown={(event) => {
                event.preventDefault();
                setDraggedId(node.id);
              }}
            >
              <circle
                r={haloRadius}
                fill="rgba(125, 211, 252, 0.08)"
                style={{
                  animation: `auth-constellation-pulse ${2.6 + node.glow * 1.8}s ease-in-out infinite`,
                }}
              />
              <circle
                r={node.size + pointerBoost * 1.8}
                fill={`rgba(255, 255, 255, ${fillOpacity})`}
                filter="url(#constellation-glow)"
              />
              <circle
                r={node.size * 1.8 + pointerBoost * 1.2}
                fill="none"
                stroke={`rgba(191, 219, 254, ${0.3 + pointerBoost * 0.45})`}
                strokeWidth="1"
              />
            </g>
          );
        })}
      </svg>

      <div className="absolute inset-[14%] rounded-full bg-[radial-gradient(circle_at_center,_rgba(2,6,23,0.04),_rgba(2,6,23,0.3)_68%,_transparent)]" />
      <div className="absolute left-[10%] top-[11%] z-20 h-14 w-14 rounded-full bg-[#040816] shadow-[0_0_28px_18px_rgba(4,8,22,0.96)]" />
      <div className="absolute top-[11%] right-[8%] rounded-full border border-sky-300/20 bg-slate-950/55 px-3 py-1 text-[10px] font-medium uppercase tracking-[0.35em] text-sky-100/80 shadow-[0_0_24px_rgba(56,189,248,0.12)] backdrop-blur-md">
        Constellation Drift
      </div>
      <div className="absolute bottom-[12%] left-[8%] max-w-[13rem] text-left text-[11px] uppercase tracking-[0.25em] text-sky-100/45">
        Hover to attract stars.
        <br />
        Drag a node to redraw the line.
      </div>

      <img
        src={rocketImage}
        alt="RocketMQ Rust"
        className="relative z-10 h-full w-full object-contain px-[12%] drop-shadow-[0_0_50px_rgba(56,189,248,0.22)]"
      />
    </div>
  );
};
