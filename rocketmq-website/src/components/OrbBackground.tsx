/**
 * OrbBackground - Advanced animated orb background with mouse interaction
 * Creates glowing, floating orbs that respond to mouse movement
 */

import React, { useState, useEffect, useRef } from 'react';
import styles from './OrbBackground.module.css';

interface Orb {
  id: number;
  size: number;
  x: number;
  y: number;
  duration: number;
  delay: number;
  color: string;
  blurAmount: number;
}

const generateOrbs = (count: number): Orb[] => {
  const colors = [
    'radial-gradient(circle, rgba(37, 99, 235, 0.6) 0%, rgba(37, 99, 235, 0) 70%)',
    'radial-gradient(circle, rgba(0, 191, 165, 0.5) 0%, rgba(0, 191, 165, 0) 70%)',
    'radial-gradient(circle, rgba(76, 175, 80, 0.5) 0%, rgba(76, 175, 80, 0) 70%)',
    'radial-gradient(circle, rgba(0, 188, 212, 0.5) 0%, rgba(0, 188, 212, 0) 70%)',
    'radial-gradient(circle, rgba(139, 92, 246, 0.4) 0%, rgba(139, 92, 246, 0) 70%)',
  ];

  return Array.from({ length: count }, (_, i) => ({
    id: i,
    size: Math.random() * 500 + 300,
    x: Math.random() * 100,
    y: Math.random() * 100,
    duration: Math.random() * 25 + 20,
    delay: Math.random() * 5,
    color: colors[i % colors.length],
    blurAmount: Math.random() * 40 + 50,
  }));
};

export default function OrbBackground(): React.JSX.Element {
  const [mousePosition, setMousePosition] = useState({ x: 50, y: 50 });
  const orbsRef = useRef<HTMLDivElement>(null);
  const orbs = generateOrbs(8);

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!orbsRef.current) return;

      const rect = orbsRef.current.getBoundingClientRect();
      const x = ((e.clientX - rect.left) / rect.width) * 100;
      const y = ((e.clientY - rect.top) / rect.height) * 100;

      setMousePosition({ x, y });
    };

    const element = orbsRef.current;
    if (element) {
      element.addEventListener('mousemove', handleMouseMove);
    }

    return () => {
      if (element) {
        element.removeEventListener('mousemove', handleMouseMove);
      }
    };
  }, []);

  return (
    <div className={styles.orbContainer} ref={orbsRef}>
      {orbs.map((orb) => (
        <div
          key={orb.id}
          className={styles.orb}
          style={{
            width: `${orb.size}px`,
            height: `${orb.size}px`,
            left: `${orb.x}%`,
            top: `${orb.y}%`,
            animationDuration: `${orb.duration}s`,
            animationDelay: `${orb.delay}s`,
            background: orb.color,
            filter: `blur(${orb.blurAmount}px)`,
          }}
        />
      ))}
      {/* Mouse-following orb */}
      <div
        className={styles.mouseOrb}
        style={{
          left: `${mousePosition.x}%`,
          top: `${mousePosition.y}%`,
        }}
      />
    </div>
  );
}
