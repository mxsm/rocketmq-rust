/**
 * Development Warning Banner - Artistic Glassmorphism Design
 * A beautiful, dismissible banner warning about active development
 */

import React, { useState, useEffect } from 'react';

export default function DevWarningBanner(): React.JSX.Element | null {
  const [isDark, setIsDark] = useState(true);
  const [isVisible, setIsVisible] = useState(true);
  const [isMounted, setIsMounted] = useState(false);

  useEffect(() => {
    setIsMounted(true);

    // Check if user has previously dismissed the banner
    const dismissed = localStorage.getItem('dev-warning-dismissed');
    if (dismissed === 'true') {
      setIsVisible(false);
    }

    // Detect color mode from document
    const detectColorMode = () => {
      const dataTheme = document.documentElement.getAttribute('data-theme');
      setIsDark(dataTheme !== 'light');
    };

    detectColorMode();

    // Listen for theme changes
    const observer = new MutationObserver(detectColorMode);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['data-theme'],
    });

    return () => observer.disconnect();
  }, []);

  const handleDismiss = () => {
    setIsVisible(false);
    localStorage.setItem('dev-warning-dismissed', 'true');
  };

  if (!isVisible) return null;

  return (
    <div
      className={`dev-warning-banner ${isDark ? 'dark' : 'light'} ${
        isMounted ? 'mounted' : ''
      }`}
      style={{
        position: 'relative',
        width: '100%',
        background: isDark
          ? 'linear-gradient(135deg, ' +
            'rgba(220, 38, 38, 0.15) 0%, ' +
            'rgba(234, 179, 8, 0.12) 25%, ' +
            'rgba(168, 85, 247, 0.1) 50%, ' +
            'rgba(6, 182, 212, 0.12) 75%, ' +
            'rgba(220, 38, 38, 0.15) 100%)'
          : 'linear-gradient(135deg, ' +
            'rgba(239, 68, 68, 0.08) 0%, ' +
            'rgba(234, 179, 8, 0.1) 25%, ' +
            'rgba(168, 85, 247, 0.08) 50%, ' +
            'rgba(6, 182, 212, 0.1) 75%, ' +
            'rgba(239, 68, 68, 0.08) 100%)',
        backdropFilter: 'blur(12px)',
        WebkitBackdropFilter: 'blur(12px)',
        borderBottom: isDark
          ? '1px solid rgba(220, 38, 38, 0.3)'
          : '1px solid rgba(220, 38, 38, 0.2)',
        padding: '14px 20px',
        boxShadow: isDark
          ? '0 4px 20px rgba(220, 38, 38, 0.15), ' +
            'inset 0 1px 0 rgba(255, 255, 255, 0.05)'
          : '0 4px 20px rgba(220, 38, 38, 0.1), ' +
            'inset 0 1px 0 rgba(255, 255, 255, 0.8)',
        animation: 'slideDown 0.5s ease-out, shimmer 8s ease-in-out infinite',
        backgroundSize: '200% 200%',
        overflow: 'hidden',
      }}
    >
      {/* Animated glow effect */}
      <div
        style={{
          position: 'absolute',
          top: '-50%',
          left: '-50%',
          width: '200%',
          height: '200%',
          background: isDark
            ? 'radial-gradient(circle, rgba(220, 38, 38, 0.15) 0%, transparent 50%)'
            : 'radial-gradient(circle, rgba(239, 68, 68, 0.1) 0%, transparent 50%)',
          animation: 'glow-pulse 4s ease-in-out infinite',
          pointerEvents: 'none',
        }}
      />

      {/* Content container */}
      <div
        style={{
          position: 'relative',
          maxWidth: '1400px',
          margin: '0 auto',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          gap: '14px',
          flexWrap: 'wrap',
          zIndex: 1,
        }}
      >
        {/* Warning icon with animation */}
        <span
          style={{
            fontSize: '22px',
            animation: 'pulse-glow 2s ease-in-out infinite',
            filter: isDark
              ? 'drop-shadow(0 0 8px rgba(234, 179, 8, 0.6))'
              : 'drop-shadow(0 0 6px rgba(234, 179, 8, 0.4))',
          }}
        >
          ⚠️
        </span>

        {/* Warning text */}
        <span
          style={{
            fontSize: '15px',
            fontWeight: 600,
            color: isDark ? '#fef3c7' : '#92400e',
            textAlign: 'center',
            textShadow: isDark
              ? '0 1px 2px rgba(0, 0, 0, 0.3)'
              : '0 1px 1px rgba(255, 255, 255, 0.8)',
            letterSpacing: '0.02em',
          }}
        >
          RocketMQ-Rust is under active development. APIs may change before the 1.0
          release.
        </span>

        {/* Status badge */}
        <span
          style={{
            padding: '4px 12px',
            borderRadius: '20px',
            fontSize: '12px',
            fontWeight: 700,
            textTransform: 'uppercase',
            letterSpacing: '0.05em',
            background: isDark
              ? 'linear-gradient(135deg, rgba(220, 38, 38, 0.3) 0%, rgba(234, 179, 8, 0.3) 100%)'
              : 'linear-gradient(135deg, rgba(239, 68, 68, 0.15) 0%, rgba(234, 179, 8, 0.15) 100%)',
            border: isDark
              ? '1px solid rgba(234, 179, 8, 0.4)'
              : '1px solid rgba(234, 179, 8, 0.3)',
            color: isDark ? '#fde047' : '#b45309',
            boxShadow: isDark
              ? '0 2px 8px rgba(234, 179, 8, 0.2), inset 0 1px 0 rgba(255, 255, 255, 0.1)'
              : '0 2px 8px rgba(234, 179, 8, 0.15), inset 0 1px 0 rgba(255, 255, 255, 0.5)',
            backdropFilter: 'blur(8px)',
          }}
        >
          Beta
        </span>

        {/* Close button */}
        <button
          onClick={handleDismiss}
          aria-label="Dismiss warning"
          style={{
            position: 'absolute',
            right: '20px',
            top: '50%',
            transform: 'translateY(-50%)',
            background: isDark
              ? 'rgba(255, 255, 255, 0.05)'
              : 'rgba(0, 0, 0, 0.05)',
            border: isDark
              ? '1px solid rgba(255, 255, 255, 0.1)'
              : '1px solid rgba(0, 0, 0, 0.1)',
            borderRadius: '8px',
            width: '32px',
            height: '32px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            cursor: 'pointer',
            transition: 'all 0.25s cubic-bezier(0.4, 0, 0.2, 1)',
            color: isDark
              ? 'rgba(255, 255, 255, 0.6)'
              : 'rgba(0, 0, 0, 0.5)',
            fontSize: '18px',
            fontWeight: 300,
            lineHeight: 1,
            padding: 0,
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.background = isDark
              ? 'rgba(220, 38, 38, 0.2)'
              : 'rgba(220, 38, 38, 0.1)';
            e.currentTarget.style.borderColor = isDark
              ? 'rgba(220, 38, 38, 0.4)'
              : 'rgba(220, 38, 38, 0.3)';
            e.currentTarget.style.transform = 'translateY(-50%) scale(1.05)';
            e.currentTarget.style.color = isDark ? '#fca5a5' : '#dc2626';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = isDark
              ? 'rgba(255, 255, 255, 0.05)'
              : 'rgba(0, 0, 0, 0.05)';
            e.currentTarget.style.borderColor = isDark
              ? '1px solid rgba(255, 255, 255, 0.1)'
              : '1px solid rgba(0, 0, 0, 0.1)';
            e.currentTarget.style.transform = 'translateY(-50%) scale(1)';
            e.currentTarget.style.color = isDark
              ? 'rgba(255, 255, 255, 0.6)'
              : 'rgba(0, 0, 0, 0.5)';
          }}
        >
          ×
        </button>
      </div>

      {/* CSS animations via style tag */}
      <style>{`
        @keyframes slideDown {
          from {
            opacity: 0;
            transform: translateY(-100%);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }

        @keyframes shimmer {
          0%, 100% {
            background-position: 0% 50%;
          }
          50% {
            background-position: 100% 50%;
          }
        }

        @keyframes glow-pulse {
          0%, 100% {
            opacity: 0.3;
            transform: translate(-50%, -50%) scale(1);
          }
          50% {
            opacity: 0.6;
            transform: translate(-50%, -50%) scale(1.1);
          }
        }

        @keyframes pulse-glow {
          0%, 100% {
            transform: scale(1);
          }
          50% {
            transform: scale(1.1);
          }
        }

        .dev-warning-banner {
          transition: opacity 0.3s ease, transform 0.3s ease;
        }

        @media (max-width: 768px) {
          .dev-warning-banner {
            padding: 12px 16px;
          }

          .dev-warning-banner button {
            position: static !important;
            transform: none !important;
            margin-left: auto;
          }

          .dev-warning-banner button:hover {
            transform: scale(1.05) !important;
          }
        }

        @media (prefers-reduced-motion: reduce) {
          .dev-warning-banner {
            animation: none !important;
          }

          .dev-warning-banner span[style*="animation"] {
            animation: none !important;
          }
        }
      `}</style>
    </div>
  );
}
