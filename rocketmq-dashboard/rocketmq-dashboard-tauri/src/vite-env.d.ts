// Type declarations for Vite custom module aliases
declare module 'sonner@2.0.3' {
  export * from 'sonner';
}

declare module 'lucide-react@0.487.0' {
  export * from 'lucide-react';
}

declare module 'class-variance-authority@0.7.1' {
  export * from 'class-variance-authority';
}

declare module 'next-themes@0.4.6' {
  export * from 'next-themes';
}

declare module 'react-hook-form@7.55.0' {
  export * from 'react-hook-form';
}

declare module 'react-day-picker@8.10.1' {
  export * from 'react-day-picker';
}

declare module 'embla-carousel-react@8.6.0' {
  export * from 'embla-carousel-react';
  import useEmblaCarousel from 'embla-carousel-react';
  export default useEmblaCarousel;
}

declare module 'cmdk@1.1.1' {
  export * from 'cmdk';
}

declare module 'input-otp@1.4.2' {
  export * from 'input-otp';
}

declare module 'vaul@1.1.2' {
  export * from 'vaul';
}

declare module 'recharts@2.15.2' {
  export * from 'recharts';
}

declare module 'react-resizable-panels@2.1.7' {
  export * from 'react-resizable-panels';
}

declare module '@radix-ui/react-accordion@1.2.3' {
  export * from '@radix-ui/react-accordion';
}

declare module '@radix-ui/react-alert-dialog@1.1.6' {
  export * from '@radix-ui/react-alert-dialog';
}

declare module '@radix-ui/react-aspect-ratio@1.1.2' {
  export * from '@radix-ui/react-aspect-ratio';
}

declare module '@radix-ui/react-avatar@1.1.3' {
  export * from '@radix-ui/react-avatar';
}

declare module '@radix-ui/react-checkbox@1.1.4' {
  export * from '@radix-ui/react-checkbox';
}

declare module '@radix-ui/react-collapsible@1.1.3' {
  export * from '@radix-ui/react-collapsible';
}

declare module '@radix-ui/react-context-menu@2.2.6' {
  export * from '@radix-ui/react-context-menu';
}

declare module '@radix-ui/react-dialog@1.1.6' {
  export * from '@radix-ui/react-dialog';
}

declare module '@radix-ui/react-dropdown-menu@2.1.6' {
  export * from '@radix-ui/react-dropdown-menu';
}

declare module '@radix-ui/react-hover-card@1.1.6' {
  export * from '@radix-ui/react-hover-card';
}

declare module '@radix-ui/react-label@2.1.2' {
  export * from '@radix-ui/react-label';
}

declare module '@radix-ui/react-menubar@1.1.6' {
  export * from '@radix-ui/react-menubar';
}

declare module '@radix-ui/react-navigation-menu@1.2.5' {
  export * from '@radix-ui/react-navigation-menu';
}

declare module '@radix-ui/react-popover@1.1.6' {
  export * from '@radix-ui/react-popover';
}

declare module '@radix-ui/react-progress@1.1.2' {
  export * from '@radix-ui/react-progress';
}

declare module '@radix-ui/react-radio-group@1.2.3' {
  export * from '@radix-ui/react-radio-group';
}

declare module '@radix-ui/react-scroll-area@1.2.3' {
  export * from '@radix-ui/react-scroll-area';
}

declare module '@radix-ui/react-select@2.1.6' {
  export * from '@radix-ui/react-select';
}

declare module '@radix-ui/react-separator@1.1.2' {
  export * from '@radix-ui/react-separator';
}

declare module '@radix-ui/react-slider@1.2.3' {
  export * from '@radix-ui/react-slider';
}

declare module '@radix-ui/react-slot@1.1.2' {
  export * from '@radix-ui/react-slot';
}

declare module '@radix-ui/react-switch@1.1.3' {
  export * from '@radix-ui/react-switch';
}

declare module '@radix-ui/react-tabs@1.1.3' {
  export * from '@radix-ui/react-tabs';
}

declare module '@radix-ui/react-toggle@1.1.2' {
  export * from '@radix-ui/react-toggle';
}

declare module '@radix-ui/react-toggle-group@1.1.2' {
  export * from '@radix-ui/react-toggle-group';
}

declare module '@radix-ui/react-tooltip@1.1.8' {
  export * from '@radix-ui/react-tooltip';
}

// Custom asset imports
declare module 'rocketmq-rust:asset/rocketmq-rust-logo.png' {
  const value: string;
  export default value;
}

declare module 'rocketmq-rust:asset/rocketmq-rust.png' {
  const value: string;
  export default value;
}

// CSS modules
declare module '*.css' {
  const content: Record<string, string>;
  export default content;
}
