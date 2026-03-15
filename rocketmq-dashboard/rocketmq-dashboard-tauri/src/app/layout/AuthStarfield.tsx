import React from 'react';

interface StarSpec {
  top: number;
  left: number;
  size: number;
  opacity: number;
  blur: number;
  glow: number;
  duration: number;
  delay: number;
}

interface MeteorSpec {
  top: number;
  left: number;
  length: number;
  thickness: number;
  duration: number;
  delay: number;
  opacity: number;
  angle: number;
  travelX: number;
  travelY: number;
}

const createSeededRandom = (seed: number) => {
  let current = seed;

  return () => {
    current = (current * 1664525 + 1013904223) % 4294967296;
    return current / 4294967296;
  };
};

const createStars = (seed: number, count: number, depth: number): StarSpec[] => {
  const random = createSeededRandom(seed);

  return Array.from({ length: count }, () => {
    const size = 0.8 + random() * depth;

    return {
      top: random() * 100,
      left: random() * 100,
      size,
      opacity: 0.3 + random() * 0.6,
      blur: random() * 0.8,
      glow: 6 + random() * 18,
      duration: 2.8 + random() * 4.2,
      delay: random() * 5,
    };
  });
};

const createMeteors = (seed: number, count: number): MeteorSpec[] => {
  const random = createSeededRandom(seed);

  return Array.from({ length: count }, () => ({
    top: -12 + random() * 72,
    left: -28 + random() * 128,
    length: 150 + random() * 120,
    thickness: 1.4 + random() * 1.4,
    duration: 1.25 + random() * 1.6,
    delay: random() * 11,
    opacity: 0.7 + random() * 0.22,
    angle: 18 + random() * 14,
    travelX: 880 + random() * 320,
    travelY: 340 + random() * 160,
  }));
};

const stars = [
  ...createStars(11, 48, 1.4),
  ...createStars(23, 28, 2),
  ...createStars(37, 16, 2.6),
];

const meteors = [...createMeteors(71, 4), ...createMeteors(113, 3)];

export const AuthStarfield = () => {
  return (
    <div className="pointer-events-none absolute inset-0 overflow-hidden" aria-hidden="true">
      <style>
        {`
          @keyframes auth-star-twinkle {
            0%, 100% {
              opacity: 0.18;
              transform: scale(0.9);
            }
            50% {
              opacity: 1;
              transform: scale(1.35);
            }
          }

          @keyframes auth-star-drift {
            0% {
              transform: translate3d(0, 0, 0);
            }
            50% {
              transform: translate3d(10px, -14px, 0);
            }
            100% {
              transform: translate3d(-8px, 12px, 0);
            }
          }

          @keyframes auth-nebula-breathe {
            0%, 100% {
              transform: scale(1) translate3d(0, 0, 0);
              opacity: 0.38;
            }
            50% {
              transform: scale(1.08) translate3d(-18px, 14px, 0);
              opacity: 0.62;
            }
          }

          @keyframes auth-meteor-fall {
            0% {
              opacity: 0;
              transform: translate3d(0, 0, 0);
            }
            6% {
              opacity: 0.95;
            }
            55% {
              opacity: 0.78;
            }
            100% {
              opacity: 0;
              transform: translate3d(var(--meteor-travel-x), var(--meteor-travel-y), 0);
            }
          }

          @keyframes auth-meteor-head {
            0% {
              opacity: 0;
              transform: translate(35%, -50%) scale(0.45);
            }
            10% {
              opacity: 1;
              transform: translate(35%, -50%) scale(1);
            }
            58% {
              opacity: 0.7;
              transform: translate(35%, -50%) scale(0.82);
            }
            100% {
              opacity: 0;
              transform: translate(35%, -50%) scale(0.35);
            }
          }
        `}
      </style>

      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,_rgba(56,189,248,0.14),_transparent_34%),radial-gradient(circle_at_20%_25%,_rgba(59,130,246,0.16),_transparent_30%),radial-gradient(circle_at_80%_10%,_rgba(14,165,233,0.12),_transparent_28%)]" />
      <div className="absolute inset-0 bg-[linear-gradient(180deg,_rgba(2,6,23,0.1)_0%,_rgba(2,6,23,0.46)_48%,_rgba(2,6,23,0.9)_100%)]" />

      <div
        className="absolute left-[-12%] top-[-10%] h-[24rem] w-[24rem] rounded-full bg-cyan-400/10 blur-[140px]"
        style={{ animation: 'auth-nebula-breathe 18s ease-in-out infinite' }}
      />
      <div
        className="absolute bottom-[-18%] right-[-10%] h-[28rem] w-[28rem] rounded-full bg-blue-500/12 blur-[160px]"
        style={{ animation: 'auth-nebula-breathe 22s ease-in-out infinite 2s' }}
      />
      <div
        className="absolute left-[22%] top-[14%] h-[18rem] w-[18rem] rounded-full bg-sky-300/8 blur-[120px]"
        style={{ animation: 'auth-nebula-breathe 16s ease-in-out infinite 1s' }}
      />

      <div className="absolute inset-0">
        {stars.map((star, index) => (
          <span
            key={`star-${index}`}
            className="absolute rounded-full bg-white"
            style={{
              top: `${star.top}%`,
              left: `${star.left}%`,
              width: `${star.size}px`,
              height: `${star.size}px`,
              opacity: star.opacity,
              filter: `blur(${star.blur}px)`,
              boxShadow: `0 0 ${star.glow}px rgba(255, 255, 255, 0.8)`,
              animation: `auth-star-twinkle ${star.duration}s ease-in-out ${star.delay}s infinite, auth-star-drift ${star.duration * 2.4}s ease-in-out ${star.delay / 2}s infinite alternate`,
            }}
          />
        ))}
      </div>

      <div className="absolute inset-0 opacity-95">
        {meteors.map((meteor, index) => (
          <div
            key={`meteor-${index}`}
            className="absolute"
            style={
              {
                top: `${meteor.top}%`,
                left: `${meteor.left}%`,
                '--meteor-travel-x': `${meteor.travelX}px`,
                '--meteor-travel-y': `${meteor.travelY}px`,
                animation: `auth-meteor-fall ${meteor.duration}s cubic-bezier(0.22, 0.61, 0.36, 1) ${meteor.delay}s infinite`,
              } as React.CSSProperties
            }
          >
            <div
              className="relative"
              style={{
                transform: `rotate(${meteor.angle}deg)`,
                transformOrigin: 'right center',
              }}
            >
              <span
                className="absolute right-0 top-1/2 block -translate-y-1/2 rounded-full bg-gradient-to-r from-transparent via-sky-200/55 to-white"
                style={{
                  width: `${meteor.length}px`,
                  height: `${meteor.thickness}px`,
                  opacity: meteor.opacity,
                  boxShadow: '0 0 12px rgba(125, 211, 252, 0.4)',
                }}
              />
              <span
                className="absolute right-0 top-1/2 block -translate-y-1/2 rounded-full bg-gradient-to-r from-transparent via-sky-300/18 to-sky-100/85 blur-[2px]"
                style={{
                  width: `${meteor.length * 0.92}px`,
                  height: `${meteor.thickness * 3.4}px`,
                  opacity: meteor.opacity * 0.78,
                }}
              />
              <span
                className="absolute right-0 top-1/2 rounded-full bg-white"
                style={{
                  width: `${meteor.thickness * 4.2}px`,
                  height: `${meteor.thickness * 4.2}px`,
                  opacity: meteor.opacity,
                  boxShadow: '0 0 14px rgba(255,255,255,0.95), 0 0 28px rgba(125, 211, 252, 0.65)',
                  animation: `auth-meteor-head ${meteor.duration}s ease-out ${meteor.delay}s infinite`,
                }}
              />
            </div>
          </div>
        ))}
      </div>

      <div className="absolute inset-x-0 bottom-0 h-48 bg-[linear-gradient(180deg,_transparent,_rgba(2,6,23,0.75))]" />
    </div>
  );
};
