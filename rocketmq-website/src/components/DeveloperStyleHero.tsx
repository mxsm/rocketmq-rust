/**
 * Developer Style Hero - Inspired by modern developer portfolios
 * Features: Grid background, purple/cyan theme, code syntax elements
 * Supports light/dark theme switching
 */

import React from 'react';
import Link from '@docusaurus/Link';
import {useColorMode} from '@docusaurus/theme-common';
import {translate} from '@docusaurus/Translate';

export default function DeveloperStyleHero(): React.JSX.Element {
    const {colorMode} = useColorMode();
    const isDark = colorMode === 'dark';

    // Generate random gradient colors
    const [gradientColors] = React.useState(() => {
        const colors = [];
        for (let i = 0; i < 5; i++) {
            const hue = Math.floor(Math.random() * 360);
            const saturation = 70 + Math.floor(Math.random() * 20); // 70-90%
            const lightness = 55 + Math.floor(Math.random() * 15); // 55-70%
            colors.push(`hsl(${hue}, ${saturation}%, ${lightness}%)`);
        }
        return colors;
    });

    const gradientString = `linear-gradient(135deg, ${gradientColors[0]} 0%, ${gradientColors[1]} 25%, ${gradientColors[2]} 50%, ${gradientColors[3]} 75%, ${gradientColors[4]} 100%)`;

    // Random rocket movement state with direction and natural trajectory
    const [rocketPosition, setRocketPosition] = React.useState({x: 50, y: 50, angle: 45, speed: 2});
    const [isAnimating, setIsAnimating] = React.useState(false);

    React.useEffect(() => {
        const moveRocket = () => {
            setRocketPosition(prev => {
                // Generate a new target with more constrained, natural movement
                // Use previous position to maintain momentum-like behavior
                const angleVariation = (Math.random() - 0.5) * 120; // -60 to +60 degrees variation
                const currentAngle = (prev.angle + 45) * (Math.PI / 180); // Convert back to radians
                const newAngle = currentAngle + angleVariation * (Math.PI / 180);

                // Distance varies from 20-40% for smoother, more noticeable movement
                const distance = 20 + Math.random() * 20;

                let newX = prev.x + Math.cos(newAngle) * distance;
                let newY = prev.y + Math.sin(newAngle) * distance;

                // Bounce off edges with some randomness for more natural behavior
                if (newX < 5) {
                    newX = 5 + Math.random() * 10;
                } else if (newX > 95) {
                    newX = 95 - Math.random() * 10;
                }

                if (newY < 5) {
                    newY = 5 + Math.random() * 10;
                } else if (newY > 95) {
                    newY = 95 - Math.random() * 10;
                }

                // Calculate angle based on actual movement direction
                const deltaX = newX - prev.x;
                const deltaY = newY - prev.y;
                const movementAngle = Math.atan2(deltaY, deltaX) * (180 / Math.PI);

                // Adjust angle so rocket points in the direction of movement
                const adjustedAngle = movementAngle - 45;

                // Variable speed with slight variations for organic feel
                const speed = 2 + Math.random() * 1; // 2s to 3s

                return {x: newX, y: newY, angle: adjustedAngle, speed};
            });
        };

        // Initial movement after a short delay
        const initialDelay = setTimeout(() => {
            moveRocket();
            const interval = setInterval(moveRocket, 3000);
            return () => clearInterval(interval);
        }, 800);

        return () => clearTimeout(initialDelay);
    }, []);

    return (
        <div style={{
            position: 'relative',
            minHeight: '60vh',
            background: isDark ? '#0f172a' : '#f8fafc',
            overflow: 'hidden',
        }}>
            {/* Add keyframes animation */}
            <style>
                {`
          @keyframes gradientFlow {
            0% {
              background-position: 0% 50%;
            }
            50% {
              background-position: 100% 50%;
            }
            100% {
              background-position: 0% 50%;
            }
          }
          @keyframes pulse {
            0%, 100% {
              opacity: 1;
            }
            50% {
              opacity: 0.8;
            }
          }
          @keyframes wave {
            0%, 100% {
              transform: translateY(0);
            }
            50% {
              transform: translateY(-10px);
            }
          }
          @keyframes shine {
            0% {
              left: -100%;
            }
            20%, 100% {
              left: 100%;
            }
          }
        `}
            </style>
            {/* Grid Background */}
            <div style={{
                position: 'absolute',
                inset: 0,
                backgroundImage: `
          linear-gradient(${isDark ? 'rgba(168, 85, 247, 0.03)' : 'rgba(168, 85, 247, 0.06)'} 1px, transparent 1px),
          linear-gradient(90deg, ${isDark ? 'rgba(168, 85, 247, 0.03)' : 'rgba(168, 85, 247, 0.06)'} 1px, transparent 1px)
        `,
                backgroundSize: '50px 50px',
                opacity: isDark ? 0.5 : 0.8,
            }}/>

            {/* Purple Glow Orbs */}
            <div style={{
                position: 'absolute',
                top: '20%',
                right: '15%',
                width: '400px',
                height: '400px',
                borderRadius: '50%',
                background: `radial-gradient(circle, ${isDark ? 'rgba(168, 85, 247, 0.15)' : 'rgba(168, 85, 247, 0.08)'} 0%, transparent 70%)`,
                filter: 'blur(80px)',
                animation: 'floatOrb1 8s ease-in-out infinite',
            }}/>

            <div style={{
                position: 'absolute',
                bottom: '20%',
                left: '10%',
                width: '300px',
                height: '300px',
                borderRadius: '50%',
                background: `radial-gradient(circle, ${isDark ? 'rgba(6, 182, 212, 0.1)' : 'rgba(6, 182, 212, 0.05)'} 0%, transparent 70%)`,
                filter: 'blur(60px)',
                animation: 'floatOrb2 10s ease-in-out infinite',
            }}/>

            {/* Main Content */}
            <div style={{
                position: 'relative',
                maxWidth: '1200px',
                margin: '0 auto',
                padding: '40px 24px 30px',
                display: 'grid',
                gridTemplateColumns: '1fr 1fr',
                gap: '50px',
                alignItems: 'center',
                zIndex: 1,
            }}>
                {/* Left Column - Content */}
                <div>
                    {/* Main heading */}
                    <h1 style={{
                        fontSize: '72px',
                        fontWeight: 800,
                        lineHeight: 1.1,
                        margin: '0 0 4px 0',
                        letterSpacing: '-1px',
                    }}>
                        {'RocketMQ-Rust'.split('').map((char, index) => (
                            <span
                                key={index}
                                style={{
                                    display: 'inline-block',
                                    background: gradientString,
                                    backgroundSize: '200% 200%',
                                    WebkitBackgroundClip: 'text',
                                    WebkitTextFillColor: 'transparent',
                                    backgroundClip: 'text',
                                    animation: 'wave 2s ease-in-out infinite, gradientFlow 4s ease infinite',
                                    animationDelay: `${index * 0.1}s, 0s`,
                                }}
                            >
                {char === '-' ? '\u2011' : char}
              </span>
                        ))}
                    </h1>

                    {/* Subtitle with code comment style */}
                    <div style={{
                        fontFamily: 'monospace',
                        fontSize: '18px',
                        color: '#06b6d4',
                        marginBottom: '16px',
                        opacity: 0.9,
                        fontWeight: 500,
                    }}>
                        {translate({
                            id: 'homepage.hero.subtitle',
                            message: '',
                        })}
                    </div>

                    {/* Description */}
                    <p style={{
                        fontSize: '18px',
                        lineHeight: 1.6,
                        color: isDark ? '#94a3b8' : '#475569',
                        margin: '0 0 32px 0',
                        maxWidth: '520px',
                    }}>
                        {translate({
                            id: 'homepage.hero.description',
                            message: 'A high-performance, memory-safe Apache RocketMQ implementation built with Rust for modern distributed systems.',
                        })}
                    </p>

                    {/* Buttons */}
                    <div style={{display: 'flex', gap: '16px', flexWrap: 'wrap'}}>
                        <Link
                            to="/docs/introduction"
                            style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                gap: '8px',
                                padding: '14px 32px',
                                background: 'linear-gradient(135deg, #a855f7 0%, #06b6d4 100%)',
                                color: '#fff',
                                textDecoration: 'none',
                                borderRadius: '12px',
                                fontWeight: 600,
                                fontSize: '16px',
                                transition: 'all 0.3s ease',
                                boxShadow: '0 4px 14px rgba(168, 85, 247, 0.4)',
                                border: 'none',
                            }}
                            onMouseEnter={(e) => {
                                e.currentTarget.style.transform = 'translateY(-2px)';
                                e.currentTarget.style.boxShadow = '0 6px 20px rgba(168, 85, 247, 0.5)';
                            }}
                            onMouseLeave={(e) => {
                                e.currentTarget.style.transform = 'translateY(0)';
                                e.currentTarget.style.boxShadow = '0 4px 14px rgba(168, 85, 247, 0.4)';
                            }}
                        >
                            {translate({
                                id: 'homepage.hero.getStarted',
                                message: 'Get Started',
                            })}
                            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                                <path d="M6 3L11 8L6 13" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                            </svg>
                        </Link>

                        <Link
                            to="https://github.com/mxsm/rocketmq-rust"
                            style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                gap: '8px',
                                padding: '14px 32px',
                                background: 'transparent',
                                color: '#a855f7',
                                textDecoration: 'none',
                                borderRadius: '12px',
                                fontWeight: 600,
                                fontSize: '16px',
                                border: '2px solid #a855f7',
                                transition: 'all 0.3s ease',
                            }}
                            onMouseEnter={(e) => {
                                e.currentTarget.style.background = 'rgba(168, 85, 247, 0.1)';
                                e.currentTarget.style.transform = 'translateY(-2px)';
                            }}
                            onMouseLeave={(e) => {
                                e.currentTarget.style.background = 'transparent';
                                e.currentTarget.style.transform = 'translateY(0)';
                            }}
                        >
                            {translate({
                                id: 'homepage.hero.github',
                                message: 'GitHub',
                            })} {"</>"}
                        </Link>
                    </div>

                    {/* Announcement Banner */}
                    <Link
                        to="/docs/release-notes"
                        style={{
                            marginTop: '32px',
                            padding: '12px 20px',
                            background: isDark ? 'rgba(30, 27, 75, 0.6)' : 'rgba(248, 249, 250, 0.6)',
                            border: isDark ? '1px solid rgba(168, 85, 247, 0.2)' : '1px solid rgba(168, 85, 247, 0.15)',
                            borderRadius: '12px',
                            backdropFilter: 'blur(10px)',
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '10px',
                            textDecoration: 'none',
                            cursor: 'pointer',
                            transition: 'all 0.3s ease',
                        }}
                        onMouseEnter={(e) => {
                            e.currentTarget.style.background = isDark ? 'rgba(30, 27, 75, 0.8)' : 'rgba(248, 249, 250, 0.8)';
                            e.currentTarget.style.transform = 'translateY(-2px) scale(1.05)';
                            e.currentTarget.style.boxShadow = '0 4px 12px rgba(168, 85, 247, 0.3)';
                        }}
                        onMouseLeave={(e) => {
                            e.currentTarget.style.background = isDark ? 'rgba(30, 27, 75, 0.6)' : 'rgba(248, 249, 250, 0.6)';
                            e.currentTarget.style.transform = 'translateY(0) scale(1)';
                            e.currentTarget.style.boxShadow = 'none';
                        }}
                    >
                        <span style={{fontSize: '18px'}}>🎉</span>
                        <span style={{
                            fontSize: '15px',
                            fontWeight: 700,
                            background: 'linear-gradient(135deg, #a855f7 0%, #06b6d4 25%, #f59e0b 50%, #ec4899 75%, #a855f7 100%)',
                            backgroundSize: '200% 200%',
                            WebkitBackgroundClip: 'text',
                            WebkitTextFillColor: 'transparent',
                            backgroundClip: 'text',
                            animation: 'gradientFlow 3s ease infinite',
                        }}>
              {translate({
                  id: 'homepage.announcement.title',
                  message: 'RocketMQ-Rust v0.7.0 is out!',
              })}
            </span>
                        <span style={{fontSize: '18px', animation: 'pulse 2s ease-in-out infinite'}}>🚀</span>
                    </Link>
                </div>

                {/* Right Column - Visual Element */}
                <div style={{position: 'relative', height: '450px'}}>
                    {/* Main Circle with Glow */}
                    <div style={{
                        position: 'absolute',
                        top: '50%',
                        left: '50%',
                        transform: 'translate(-50%, -50%)',
                        width: '320px',
                        height: '320px',
                        borderRadius: '50%',
                        background: isDark
                            ? 'linear-gradient(135deg, #1e293b 0%, #0f172a 100%)'
                            : 'linear-gradient(135deg, #ffffff 0%, #f1f5f9 100%)',
                        border: `2px solid ${isDark ? 'rgba(168, 85, 247, 0.2)' : 'rgba(168, 85, 247, 0.3)'}`,
                        boxShadow: isDark
                            ? `0 0 80px rgba(168, 85, 247, 0.4), inset 0 0 80px rgba(168, 85, 247, 0.15)`
                            : `0 0 60px rgba(168, 85, 247, 0.2), inset 0 0 60px rgba(168, 85, 247, 0.05)`,
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        animation: 'pulseGlow 4s ease-in-out infinite',
                    }}>
                        {/* MQ Logo in center */}
                        <svg viewBox="0 0 200 200" style={{width: '180px', height: '180px'}}>
                            <defs>
                                <linearGradient id="purpleGradient" x1="0%" y1="0%" x2="100%" y2="100%">
                                    <stop offset="0%" style={{stopColor: '#a855f7', stopOpacity: 1}}/>
                                    <stop offset="100%" style={{stopColor: '#06b6d4', stopOpacity: 1}}/>
                                </linearGradient>
                            </defs>
                            <circle cx="100" cy="100" r="80" fill="none" stroke="url(#purpleGradient)" strokeWidth="2" opacity="0.3"/>
                            <circle cx="100" cy="100" r="60" fill="none" stroke="url(#purpleGradient)" strokeWidth="2" opacity="0.5"/>
                            <circle cx="100" cy="100" r="40" fill="url(#purpleGradient)" opacity="0.2"/>
                            <path d="M100 50 L100 150 M50 100 L150 100" stroke="url(#purpleGradient)" strokeWidth="2" opacity="0.5"/>
                            <circle cx="100" cy="100" r="30" fill="url(#purpleGradient)"/>
                            <text x="100" y="108" textAnchor="middle" fill="#fff" fontSize="24" fontWeight="bold">MQ</text>
                        </svg>
                    </div>

                    {/* Floating Tech Cards */}
                    <div style={{
                        position: 'absolute',
                        top: '5%',
                        right: '-20px',
                        padding: '20px 24px',
                        background: isDark ? '#1e293b' : '#ffffff',
                        borderRadius: '16px',
                        border: `1px solid ${isDark ? 'rgba(168, 85, 247, 0.2)' : 'rgba(168, 85, 247, 0.2)'}`,
                        boxShadow: isDark ? '0 6px 30px rgba(0, 0, 0, 0.4)' : '0 6px 30px rgba(0, 0, 0, 0.1)',
                        animation: 'floatCard1 6s ease-in-out infinite',
                    }}>
                        <div style={{display: 'flex', alignItems: 'center', gap: '16px'}}>
                            <div style={{
                                width: '52px',
                                height: '52px',
                                borderRadius: '12px',
                                background: 'linear-gradient(135deg, #a855f7 0%, #06b6d4 100%)',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                            }}>
                                <span style={{color: '#fff', fontSize: '20px', fontWeight: 'bold', fontFamily: 'monospace'}}>🦀</span>
                            </div>
                            <div>
                                <div style={{color: isDark ? '#fff' : '#0f172a', fontSize: '18px', fontWeight: 600}}>Rust</div>
                                <div style={{color: isDark ? '#64748b' : '#64748b', fontSize: '14px'}}>Memory safety & Fast</div>
                            </div>
                        </div>
                    </div>

                    <div style={{
                        position: 'absolute',
                        bottom: '10%',
                        left: '-80px',
                        padding: '20px 24px',
                        background: isDark ? '#1e293b' : '#ffffff',
                        borderRadius: '16px',
                        border: `1px solid ${isDark ? 'rgba(168, 85, 247, 0.2)' : 'rgba(168, 85, 247, 0.2)'}`,
                        boxShadow: isDark ? '0 6px 30px rgba(0, 0, 0, 0.4)' : '0 6px 30px rgba(0, 0, 0, 0.1)',
                        animation: 'floatCard2 7s ease-in-out infinite',
                    }}>
                        <div style={{display: 'flex', alignItems: 'center', gap: '16px'}}>
                            <div style={{
                                width: '52px',
                                height: '52px',
                                borderRadius: '12px',
                                background: 'linear-gradient(135deg, #a855f7 0%, #06b6d4 100%)',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                            }}>
                                <span style={{color: '#fff', fontSize: '20px', fontWeight: 'bold'}}>⚡</span>
                            </div>
                            <div>
                                <div style={{color: isDark ? '#fff' : '#0f172a', fontSize: '18px', fontWeight: 600}}>High Performance</div>
                                <div style={{color: isDark ? '#64748b' : '#64748b', fontSize: '14px'}}>Async & Zero-Copy</div>
                            </div>
                        </div>
                    </div>

                    <div style={{
                        position: 'absolute',
                        top: '55%',
                        right: '-100px',
                        padding: '20px 24px',
                        background: isDark ? '#1e293b' : '#ffffff',
                        borderRadius: '16px',
                        border: `1px solid ${isDark ? 'rgba(168, 85, 247, 0.2)' : 'rgba(168, 85, 247, 0.2)'}`,
                        boxShadow: isDark ? '0 6px 30px rgba(0, 0, 0, 0.4)' : '0 6px 30px rgba(0, 0, 0, 0.1)',
                        animation: 'floatCard3 8s ease-in-out infinite',
                    }}>
                        <div style={{display: 'flex', alignItems: 'center', gap: '16px'}}>
                            <div style={{
                                width: '52px',
                                height: '52px',
                                borderRadius: '12px',
                                background: 'linear-gradient(135deg, #a855f7 0%, #06b6d4 100%)',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                            }}>
                                <span style={{color: '#fff', fontSize: '20px', fontWeight: 'bold'}}>🛡️</span>
                            </div>
                            <div>
                                <div style={{color: isDark ? '#fff' : '#0f172a', fontSize: '18px', fontWeight: 600}}>High Availability</div>
                                <div style={{color: isDark ? '#64748b' : '#64748b', fontSize: '14px'}}>Master-Slave & Controller</div>
                            </div>
                        </div>
                    </div>

                    {/* Random Moving Rocket */}
                    <div style={{
                        position: 'absolute',
                        top: `${rocketPosition.y}%`,
                        left: `${rocketPosition.x}%`,
                        fontSize: '48px',
                        transition: `all ${rocketPosition.speed}s cubic-bezier(0.25, 0.46, 0.45, 0.94)`,
                        transform: `rotate(${rocketPosition.angle}deg) scale(1)`,
                        filter: 'drop-shadow(0 4px 12px rgba(168, 85, 247, 0.4))',
                        zIndex: 10,
                        willChange: 'transform, top, left',
                        transformOrigin: 'center center',
                    }}>
                        🚀
                    </div>
                </div>
            </div>

            {/* Animation Styles */}
            <style>{`
        @keyframes floatOrb1 {
          0%, 100% { transform: translate(0, 0); }
          50% { transform: translate(30px, -30px); }
        }

        @keyframes floatOrb2 {
          0%, 100% { transform: translate(0, 0); }
          50% { transform: translate(-20px, 20px); }
        }

        @keyframes pulseGlow {
          0%, 100% {
            box-shadow: 0 0 60px rgba(168, 85, 247, 0.3), inset 0 0 60px rgba(168, 85, 247, 0.1);
          }
          50% {
            box-shadow: 0 0 80px rgba(168, 85, 247, 0.5), inset 0 0 80px rgba(168, 85, 247, 0.2);
          }
        }

        @keyframes floatCard1 {
          0%, 100% { transform: translateY(0); }
          50% { transform: translateY(-20px); }
        }

        @keyframes floatCard2 {
          0%, 100% { transform: translateY(0); }
          50% { transform: translateY(20px); }
        }

        @keyframes floatCard3 {
          0%, 100% { transform: translateX(0); }
          50% { transform: translateX(20px); }
        }

        @media (max-width: 996px) {
          /* Stack content on tablet/mobile */
          div[style*="gridTemplateColumns: 1fr 1fr"] {
            grid-template-columns: 1fr !important;
            text-align: center;
            padding: 60px 24px 40px !important;
          }

          h1[style*="fontSize"] {
            font-size: 52px !important;
          }

          div[style*="height: 450px"] {
            height: 380px !important;
            marginTop: 40px;
          }

          div[style*="width: 320px"] {
            width: 280px !important;
            height: 280px !important;
          }
        }

        @media (max-width: 768px) {
          h1[style*="fontSize"] {
            font-size: 42px !important;
          }

          div[style*="height: 450px"] {
            height: 320px !important;
          }

          div[style*="width: 320px"] {
            width: 240px !important;
            height: 240px !important;
          }
        }
      `}</style>
        </div>
    );
}
