import React, {useEffect, useId, useRef, useState} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import {translate} from '@docusaurus/Translate';
import CopyButton from './CopyButton';
import SectionHeading from './SectionHeading';
import {ArrowRightIcon} from './icons';
import {Reveal} from './motion';
import {GITHUB_URL} from './site';
import primitives from './primitives.module.css';
import styles from './GetStarted.module.css';

type Shell = 'unix' | 'windows';

const SHELLS: Record<Shell, {label: string; prompt: string}> = {
  unix: {label: 'macOS / Linux', prompt: '$'},
  windows: {label: 'Windows', prompt: 'PS>'},
};

// Commands mirror the repository README quick start.
const CLONE_COMMANDS = [`git clone ${GITHUB_URL}.git`, 'cd rocketmq-rust', 'cargo build --workspace'];
const NAMESRV_COMMANDS = ['cargo run --bin rocketmq-namesrv-rust'];
const BROKER_COMMANDS: Record<Shell, string[]> = {
  unix: [
    'export ROCKETMQ_HOME="$(pwd)/.rocketmq"',
    'mkdir -p "$ROCKETMQ_HOME/conf"',
    'cargo run --bin rocketmq-broker-rust -- -n 127.0.0.1:9876',
  ],
  windows: [
    '$env:ROCKETMQ_HOME = "$PWD\\.rocketmq"',
    'New-Item -ItemType Directory -Force "$env:ROCKETMQ_HOME\\conf" | Out-Null',
    'cargo run --bin rocketmq-broker-rust -- -n 127.0.0.1:9876',
  ],
};
const CLIENT_COMMANDS = [
  'cargo run -p rocketmq-client-rust --example consumer',
  'cargo run -p rocketmq-client-rust --example producer',
];

/** Light-touch colouring: the program, its flags, and quoted or variable arguments. */
function CommandLine({line}: {line: string}): React.JSX.Element {
  const parts = line.split(/(\s+)/);
  return (
    <>
      {parts.map((part, index) => {
        if (index === 0) {
          return (
            <span key={index} className={styles.program}>
              {part}
            </span>
          );
        }
        if (/^-/.test(part)) {
          return (
            <span key={index} className={styles.flag}>
              {part}
            </span>
          );
        }
        if (/^["$]/.test(part)) {
          return (
            <span key={index} className={styles.value}>
              {part}
            </span>
          );
        }
        return <React.Fragment key={index}>{part}</React.Fragment>;
      })}
    </>
  );
}

function CommandBlock({commands, shell}: {commands: string[]; shell: Shell}): React.JSX.Element {
  return (
    <div className={styles.command}>
      <pre className={styles.commandBody}>
        {commands.map((line) => (
          <span key={line} className={styles.commandLine}>
            <span className={styles.prompt} aria-hidden="true">
              {SHELLS[shell].prompt}
            </span>
            <span>
              <CommandLine line={line} />
            </span>
          </span>
        ))}
      </pre>
      <CopyButton className={styles.commandCopy} text={commands.join('\n')} />
    </div>
  );
}

export default function GetStarted(): React.JSX.Element {
  const headingId = useId();
  const listRef = useRef<HTMLOListElement>(null);
  const [shell, setShell] = useState<Shell>('unix');

  // Default to the visitor's platform once we are in the browser.
  useEffect(() => {
    if (/win/i.test(window.navigator.platform)) {
      setShell('windows');
    }
  }, []);

  // Grow the timeline rail with the scroll position and light up the steps it has passed.
  useEffect(() => {
    const list = listRef.current;
    if (!list) {
      return undefined;
    }

    let frame = 0;
    const update = (): void => {
      frame = 0;
      const bounds = list.getBoundingClientRect();
      const anchor = window.innerHeight * 0.62;
      const progress = Math.min(1, Math.max(0, (anchor - bounds.top) / bounds.height));
      list.style.setProperty('--progress', progress.toFixed(4));
      for (const item of Array.from(list.children) as HTMLElement[]) {
        item.toggleAttribute('data-reached', item.getBoundingClientRect().top < anchor);
      }
    };
    const schedule = (): void => {
      if (!frame) {
        frame = window.requestAnimationFrame(update);
      }
    };

    update();
    window.addEventListener('scroll', schedule, {passive: true});
    window.addEventListener('resize', schedule);
    return () => {
      window.cancelAnimationFrame(frame);
      window.removeEventListener('scroll', schedule);
      window.removeEventListener('resize', schedule);
    };
  }, []);

  const steps: Array<{title: string; text: string; commands: string[]}> = [
    {
      title: translate({id: 'homepage.start.clone.title', message: 'Clone and build'}),
      text: translate({
        id: 'homepage.start.clone.text',
        message: 'Fetch the workspace and build it with the pinned Rust 1.95 toolchain.',
      }),
      commands: CLONE_COMMANDS,
    },
    {
      title: translate({id: 'homepage.start.namesrv.title', message: 'Start the NameServer'}),
      text: translate({
        id: 'homepage.start.namesrv.text',
        message: 'The route registry for brokers and clients. It listens on 127.0.0.1:9876 by default.',
      }),
      commands: NAMESRV_COMMANDS,
    },
    {
      title: translate({id: 'homepage.start.broker.title', message: 'Start a Broker'}),
      text: translate({
        id: 'homepage.start.broker.text',
        message: 'Give the Broker a home directory for its store and point it at the NameServer.',
      }),
      commands: BROKER_COMMANDS[shell],
    },
    {
      title: translate({id: 'homepage.start.client.title', message: 'Send and consume'}),
      text: translate({
        id: 'homepage.start.client.text',
        message: 'Start the consumer first, then run the producer from a second terminal.',
      }),
      commands: CLIENT_COMMANDS,
    },
  ];

  return (
    <section className={clsx(primitives.section, styles.section)} aria-labelledby={headingId}>
      <div className={clsx(primitives.container, styles.layout)}>
        <div className={styles.intro}>
          <SectionHeading
            id={headingId}
            align="start"
            eyebrow={translate({id: 'homepage.start.eyebrow', message: 'Quick start'})}
            title={translate({id: 'homepage.start.title', message: 'From clone to first message'})}
            lead={translate({
              id: 'homepage.start.lead',
              message: 'Four steps stand up a local cluster from source and push your first messages through it.',
            })}
          />

          <Reveal delay={100}>
            <div
              className={styles.shells}
              role="group"
              aria-label={translate({id: 'homepage.start.shell.label', message: 'Command shell'})}>
              {(Object.keys(SHELLS) as Shell[]).map((option) => (
                <button
                  key={option}
                  type="button"
                  className={clsx(styles.shell, shell === option && styles.shellActive)}
                  aria-pressed={shell === option}
                  onClick={() => setShell(option)}>
                  {SHELLS[option].label}
                </button>
              ))}
            </div>

            <div className={styles.links}>
              <Link className={primitives.textLink} to="/docs/getting-started/quick-start">
                {translate({id: 'homepage.start.link.guide', message: 'Full first-message tutorial'})}
                <ArrowRightIcon />
              </Link>
              <Link className={primitives.textLink} to="/docs/getting-started/installation">
                {translate({id: 'homepage.start.link.install', message: 'Installation options'})}
                <ArrowRightIcon />
              </Link>
            </div>
          </Reveal>
        </div>

        <ol ref={listRef} className={styles.steps}>
          {steps.map((step, index) => (
            <li key={step.title} className={styles.step}>
              <span className={styles.marker} aria-hidden="true">
                {index + 1}
              </span>
              <Reveal className={styles.stepCard} delay={60}>
                <h3 className={styles.stepTitle}>{step.title}</h3>
                <p className={styles.stepText}>{step.text}</p>
                <CommandBlock commands={step.commands} shell={shell} />
              </Reveal>
            </li>
          ))}
        </ol>
      </div>
    </section>
  );
}
