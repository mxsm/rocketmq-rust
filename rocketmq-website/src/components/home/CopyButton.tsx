import React, {useCallback, useEffect, useRef, useState} from 'react';
import clsx from 'clsx';
import {translate} from '@docusaurus/Translate';
import {CheckIcon, CopyIcon} from './icons';
import styles from './primitives.module.css';

const FEEDBACK_MS = 1800;

type CopyButtonProps = {
  /** Text placed on the clipboard. */
  text: string;
  className?: string;
};

export default function CopyButton({text, className}: CopyButtonProps): React.JSX.Element {
  const [copied, setCopied] = useState(false);
  const timer = useRef<number | undefined>(undefined);

  useEffect(() => () => window.clearTimeout(timer.current), []);

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      // Clipboard access can be denied (insecure context, permissions); leave the button idle.
      return;
    }
    setCopied(true);
    window.clearTimeout(timer.current);
    timer.current = window.setTimeout(() => setCopied(false), FEEDBACK_MS);
  }, [text]);

  const label = copied
    ? translate({id: 'homepage.copy.done', message: 'Copied'})
    : translate({id: 'homepage.copy.label', message: 'Copy to clipboard'});

  return (
    <button
      type="button"
      className={clsx(styles.copyButton, copied && styles.copyButtonDone, className)}
      onClick={handleCopy}
      aria-label={label}
      title={label}>
      {copied ? <CheckIcon /> : <CopyIcon />}
    </button>
  );
}
