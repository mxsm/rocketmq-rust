/**
 * OrbBackground - Centered around MQ logo
 */

import React, { useState, useEffect, useRef } from 'react';

interface Orb {
  id: number;
  angle: number;
  distance: number;
  size: number;
  duration: number;
  delay: number;
  color: string;
}

const generateOrbs = (count: number): Orb[] => {
  const colors = [
    'radial-gradient(circle, rgba(37, 99, 235, 0.8) 0%, rgba(37, 99, 235, 0) 60%)',
    'radial-gradient(circle, rgba(0, 191, 165, 0.7) 0%, rgba(0, 191, 165, 0) 60%)',
    'radial-gradient(circle, rgba(76, 175, 80, 0.7) 0%, rgba(76, 175, 80, 0) 60%)',
    'radial-gradient(circle, rgba(0, 188, 212, 0.7) 0%, rgba(0, 188, 212, 0) 60%)',
  'radial-gradient(circle, rgba(139, 92, 246, 0.6) 0%, rgba(139, 92, 246, 0) 60%)',
    'radial-gradient(circle, rgba(255, 107, 107, 0.6) 0%, rgba(255, 107, 107, 0) 60%)',
  ];

  return Array.from({ length: count }, (_, i) => ({
    id: i,
    angle: (360 / count) * i,
    distance: 100 + Math.random() * 50,
    size: Math.random() * 150 + 100,
    duration: Math.random() * 10 + 8,
    delay: Math.random() * 2,
    color: colors[i % colors.length],
  }));
};

export default function OrbBackgroundCentered(): React.JSX.Element {
  const [orbPositions, setOrbPositions] = useState<{ x: number; y: number }[]>([]);
  const orbsRef = useRef<HTMLDivElement>(null);
  const orbs = generateOrbs(6);

  useEffect(() => {
    // Calculate positions centered on container
    if (orbsRef.current) {
      const container = orbsRef.current;
      const centerX = 50;
      const centerY = 50;

      const positions = orbs.map((orb) => {
        const angleRad = (orb.angle * Math.PI) / 180;
        const x = centerX + Math.cos(angleRad) * (orb.distance / container.offsetWidth * 100);
        const y = centerY + Math.sin(angleRad) * (orb.distance / container.offsetHeight * 100);
        return { x, y };
      });

      setOrbPositions(positions);
    }
  }, []);

  return (
    <div className="orb-centered-container" ref={orbsRef} style={{
      position: 'absolute',
      top: '50%',
      left: '50%',
      transform: 'translate(-50%, -50%)',
      width: '600px',
      height: '600px',
      pointerEvents: 'none',
      zIndex: 1,
    }}>
      {orbPositions.map((pos, index) => (
        <div
          key={orbs[index].id}
          className="orb-centered-orb"
          style={{
            position: 'absolute',
            left: `${pos.x}%`,
            top: `${pos.y}%`,
            width: `${orbs[index].size}px`,
            height: `${orbs[index].size}px`,
            background: orbs[index].color,
            borderRadius: '50%',
            filter: 'blur(40px)',
            opacity: '0.8',
            animation: `orb-rotate-${orbs[index].id} ${orbs[index].duration}s ease-in-out infinite`,
            animationDelay: `${orbs[index].delay}s`,
            transform: 'translate(-50%, -50%)',
          }}
        />
      ))}
    </div>
  );
}
