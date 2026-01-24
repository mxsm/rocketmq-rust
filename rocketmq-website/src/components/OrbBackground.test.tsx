/**
 * Test component to verify OrbBackground is working
 */

import React from 'react';
import styles from './OrbBackground.test.module.css';

export default function OrbBackgroundTest(): React.JSX.Element {
  return (
    <div className={styles.testContainer}>
      <div className={styles.testOrb} />
      <div className={styles.testInfo}>
        If you can see a RED circle in the hero background, OrbBackground is working!
      </div>
    </div>
  );
}
