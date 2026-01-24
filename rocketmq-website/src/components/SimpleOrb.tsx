/**
 * Simple Orb Background - Using pure CSS with animation
 */

import React from 'react';

export default function SimpleOrb(): React.JSX.Element {
  const animationStyles = `
    @keyframes float1 {
      0%, 100% {
        transform: translate(-50%, -50%) translate(0, 0);
      }
      50% {
        transform: translate(-50%, -50%) translate(30px, -30px);
      }
    }

    @keyframes float2 {
      0%, 100% {
        transform: translate(-50%, -50%) translate(0, 0);
      }
      50% {
        transform: translate(-50%, -50%) translate(-30px, 30px);
      }
    }

    @keyframes float3 {
      0%, 100% {
        transform: translate(-50%, -50%) translate(0, 0);
      }
      50% {
        transform: translate(-50%, -50%) translate(-20px, 20px);
      }
    }
  `;

  return (
    <>
      {/* Orb 1 - Blue */}
      <div
        style={{
          position: 'absolute',
          top: '50%',
          left: '50%',
          width: '200px',
          height: '200px',
          borderRadius: '50%',
          background: 'radial-gradient(circle, rgba(37, 99, 235, 0.8) 0%, rgba(37, 99, 235, 0) 60%)',
          filter: 'blur(50px)',
          transform: 'translate(-50%, -50%)',
          animation: 'float1 8s ease-in-out infinite',
          zIndex: 1,
          pointerEvents: 'none',
        }}
      />
      {/* Orb 2 - Cyan */}
      <div
        style={{
          position: 'absolute',
          top: '50%',
          left: '50%',
          width: '180px',
          height: '180px',
          borderRadius: '50%',
          background: 'radial-gradient(circle, rgba(0, 191, 165, 0.7) 0%, rgba(0, 191, 165, 0) 60%)',
          filter: 'blur(50px)',
          transform: 'translate(-50%, -50%)',
          animation: 'float2 10s ease-in-out infinite',
          zIndex: 1,
          pointerEvents: 'none',
        }}
      />
      {/* Orb 3 - Green */}
      <div
        style={{
          position: 'absolute',
          top: '50%',
          left: '50%',
          width: '220px',
          height: '220px',
          borderRadius: '50%',
          background: 'radial-gradient(circle, rgba(76, 175, 80, 0.7) 0%, rgba(76, 175, 80, 0) 60%)',
          filter: 'blur(50px)',
          transform: 'translate(-50%, -50%)',
          animation: 'float3 12s ease-in-out infinite',
          zIndex: 1,
          pointerEvents: 'none',
        }}
      />
      {/* Add animation styles */}
      <style dangerouslySetInnerHTML={{ __html: animationStyles }} />
    </>
  );
}
