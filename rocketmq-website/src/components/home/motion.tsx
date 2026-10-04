import React, {useEffect, useRef, useState, type CSSProperties, type RefObject} from 'react';
import clsx from 'clsx';
import styles from './motion.module.css';

const REDUCED_MOTION_QUERY = '(prefers-reduced-motion: reduce)';

/**
 * Attribute the homepage root sets once it has hydrated. Scroll-reveal styles
 * only hide content when it is present, so server-rendered and no-JS output
 * stays fully visible.
 */
export const MOTION_ATTRIBUTE = 'data-rmq-motion';

/** Tracks the reduced-motion preference. Always `false` while server rendering. */
export function usePrefersReducedMotion(): boolean {
  const [reduced, setReduced] = useState(false);

  useEffect(() => {
    const query = window.matchMedia(REDUCED_MOTION_QUERY);
    const update = (): void => setReduced(query.matches);
    update();
    query.addEventListener('change', update);
    return () => query.removeEventListener('change', update);
  }, []);

  return reduced;
}

type InViewOptions = {
  /** Stop observing after the first intersection. */
  once?: boolean;
  rootMargin?: string;
  threshold?: number;
};

/** Reports whether the referenced element intersects the viewport. */
export function useInView<T extends Element>({
  once = false,
  rootMargin = '0px',
  threshold = 0,
}: InViewOptions = {}): [RefObject<T | null>, boolean] {
  const ref = useRef<T>(null);
  const [inView, setInView] = useState(false);

  useEffect(() => {
    const element = ref.current;
    if (!element) {
      return undefined;
    }
    if (typeof IntersectionObserver === 'undefined') {
      setInView(true);
      return undefined;
    }

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setInView(true);
          if (once) {
            observer.disconnect();
          }
        } else if (!once) {
          setInView(false);
        }
      },
      {rootMargin, threshold},
    );
    observer.observe(element);
    return () => observer.disconnect();
  }, [once, rootMargin, threshold]);

  return [ref, inView];
}

type RevealProps = {
  children: React.ReactNode;
  className?: string;
  /** Stagger offset in milliseconds. */
  delay?: number;
  as?: 'div' | 'li' | 'article' | 'section';
};

/** Fades and lifts its children into place the first time they scroll into view. */
export function Reveal({children, className, delay = 0, as = 'div'}: RevealProps): React.JSX.Element {
  const [ref, inView] = useInView<HTMLElement>({once: true, rootMargin: '0px 0px -8% 0px'});

  return React.createElement(
    as,
    {
      ref,
      className: clsx(styles.reveal, inView && styles.revealIn, className),
      style: {'--reveal-delay': `${delay}ms`} as CSSProperties,
    },
    children,
  );
}
