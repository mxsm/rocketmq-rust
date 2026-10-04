import React from 'react';
import clsx from 'clsx';
import {Reveal} from './motion';
import styles from './primitives.module.css';

type SectionHeadingProps = {
  eyebrow: string;
  title: string;
  lead?: string;
  align?: 'center' | 'start';
  /** Element id for `aria-labelledby` on the owning section. */
  id?: string;
};

export default function SectionHeading({
  eyebrow,
  title,
  lead,
  align = 'center',
  id,
}: SectionHeadingProps): React.JSX.Element {
  return (
    <Reveal className={clsx(styles.heading, align === 'start' && styles.headingStart)}>
      <span className={styles.eyebrow}>{eyebrow}</span>
      <h2 id={id} className={styles.title}>
        {title}
      </h2>
      {lead && <p className={styles.lead}>{lead}</p>}
    </Reveal>
  );
}
