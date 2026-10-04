import React from 'react';

type IconProps = {
  className?: string;
};

function StrokeIcon({className, children}: IconProps & {children: React.ReactNode}): React.JSX.Element {
  return (
    <svg
      className={className}
      viewBox="0 0 24 24"
      width="1em"
      height="1em"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true">
      {children}
    </svg>
  );
}

export function ArrowRightIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <path d="M5 12h14M13 6l6 6-6 6" />
    </StrokeIcon>
  );
}

export function GitHubIcon({className}: IconProps): React.JSX.Element {
  return (
    <svg className={className} viewBox="0 0 24 24" width="1em" height="1em" fill="currentColor" aria-hidden="true">
      <path d="M12 2a10 10 0 0 0-3.16 19.49c.5.09.68-.22.68-.48v-1.86c-2.78.6-3.37-1.18-3.37-1.18-.45-1.16-1.11-1.47-1.11-1.47-.91-.62.07-.61.07-.61 1 .07 1.53 1.03 1.53 1.03.9 1.53 2.35 1.09 2.92.83.09-.65.35-1.09.64-1.34-2.22-.25-4.55-1.11-4.55-4.94 0-1.09.39-1.98 1.03-2.68-.1-.25-.45-1.27.1-2.64 0 0 .84-.27 2.75 1.02A9.6 9.6 0 0 1 12 6.84a9.5 9.5 0 0 1 2.5.34c1.91-1.3 2.75-1.02 2.75-1.02.55 1.37.2 2.39.1 2.64.64.7 1.03 1.59 1.03 2.68 0 3.84-2.34 4.68-4.57 4.93.36.31.68.92.68 1.86V21c0 .27.18.58.69.48A10 10 0 0 0 12 2Z" />
    </svg>
  );
}

export function StarIcon({className}: IconProps): React.JSX.Element {
  return (
    <svg className={className} viewBox="0 0 24 24" width="1em" height="1em" fill="currentColor" aria-hidden="true">
      <path d="m12 2.6 2.9 6 6.6.9-4.8 4.6 1.2 6.5L12 17.5l-5.9 3.1 1.2-6.5L2.5 9.5l6.6-.9L12 2.6Z" />
    </svg>
  );
}

export function CopyIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <rect x="9" y="9" width="11" height="11" rx="2.5" />
      <path d="M5 15V6.5A2.5 2.5 0 0 1 7.5 4H15" />
    </StrokeIcon>
  );
}

export function CheckIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <path d="m5 12.5 4.5 4.5L19 7.5" />
    </StrokeIcon>
  );
}

export function PlayIcon({className}: IconProps): React.JSX.Element {
  return (
    <svg className={className} viewBox="0 0 24 24" width="1em" height="1em" fill="currentColor" aria-hidden="true">
      <path d="M8 5.6v12.8a1 1 0 0 0 1.5.86l10.6-6.4a1 1 0 0 0 0-1.72L9.5 4.74A1 1 0 0 0 8 5.6Z" />
    </svg>
  );
}

export function PauseIcon({className}: IconProps): React.JSX.Element {
  return (
    <svg className={className} viewBox="0 0 24 24" width="1em" height="1em" fill="currentColor" aria-hidden="true">
      <rect x="6.5" y="5" width="4" height="14" rx="1.2" />
      <rect x="13.5" y="5" width="4" height="14" rx="1.2" />
    </svg>
  );
}

export function ShieldIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <path d="M12 3 20 6v6c0 4.6-3.2 7.7-8 9-4.8-1.3-8-4.4-8-9V6l8-3Z" />
      <path d="m8.8 12 2.3 2.3 4.3-4.6" />
    </StrokeIcon>
  );
}

export function BoltIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <path d="M13 2 4.5 13.5H11L10 22l8.5-11.5H12L13 2Z" />
    </StrokeIcon>
  );
}

export function PlugIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <path d="M9 3v5M15 3v5M6.5 8h11v3.5a5.5 5.5 0 0 1-11 0V8ZM12 17v4" />
    </StrokeIcon>
  );
}

export function LayersIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <path d="m12 3 9 4.5-9 4.5-9-4.5L12 3Z" />
      <path d="m3 12 9 4.5 9-4.5M3 16.5 12 21l9-4.5" />
    </StrokeIcon>
  );
}

export function ShuffleIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <path d="M3 7h3.5c2.5 0 3.8 1.4 5 3.5s2.5 6.5 5 6.5H21M3 17h3.5c1.3 0 2.3-.5 3.1-1.3M21 7h-4.5c-1.3 0-2.3.5-3.1 1.3" />
      <path d="m18 4 3 3-3 3M18 14l3 3-3 3" />
    </StrokeIcon>
  );
}

export function QuorumIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <circle cx="12" cy="5.5" r="2.5" />
      <circle cx="5.5" cy="17.5" r="2.5" />
      <circle cx="18.5" cy="17.5" r="2.5" />
      <path d="M10.8 7.7 6.8 15.3M13.2 7.7l4 7.6M8 17.5h8" />
    </StrokeIcon>
  );
}

export function LockIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <rect x="4.5" y="10.5" width="15" height="10" rx="2.5" />
      <path d="M8 10.5V7.5a4 4 0 0 1 8 0v3M12 14.5v2" />
    </StrokeIcon>
  );
}

export function ActivityIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <path d="M3 12h4l2.5-7 5 14 2.5-7h4" />
    </StrokeIcon>
  );
}

export function BookIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <path d="M4 5.5A2.5 2.5 0 0 1 6.5 3H20v15H6.5A2.5 2.5 0 0 0 4 20.5v-15Z" />
      <path d="M4 20.5A2.5 2.5 0 0 0 6.5 21H20v-3" />
    </StrokeIcon>
  );
}

export function ChatIcon(props: IconProps): React.JSX.Element {
  return (
    <StrokeIcon {...props}>
      <path d="M20 14.5a2.5 2.5 0 0 1-2.5 2.5H9l-5 4V6.5A2.5 2.5 0 0 1 6.5 4h11A2.5 2.5 0 0 1 20 6.5v8Z" />
    </StrokeIcon>
  );
}
