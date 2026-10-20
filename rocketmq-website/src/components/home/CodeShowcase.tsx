import React, {useId, useRef, useState, type CSSProperties} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import {translate} from '@docusaurus/Translate';
import {Highlight, type PrismTheme} from 'prism-react-renderer';
import CopyButton from './CopyButton';
import SectionHeading from './SectionHeading';
import {ArrowRightIcon} from './icons';
import {Reveal, useInView} from './motion';
import {VERSION} from './site';
import primitives from './primitives.module.css';
import styles from './CodeShowcase.module.css';

/** Palette tuned to the homepage; the site-wide Prism themes follow the colour mode instead. */
const CODE_THEME: PrismTheme = {
  plain: {color: '#c9d1e4', backgroundColor: 'transparent'},
  styles: [
    {types: ['comment'], style: {color: '#5d667d', fontStyle: 'italic'}},
    {types: ['keyword', 'boolean'], style: {color: '#c792ea'}},
    {types: ['string', 'char'], style: {color: '#c3e88d'}},
    {types: ['number'], style: {color: '#f78c6c'}},
    {types: ['function', 'function-definition'], style: {color: '#82aaff'}},
    {types: ['macro', 'attribute'], style: {color: '#ffcb6b'}},
    {types: ['class-name', 'type-definition', 'namespace'], style: {color: '#ffb86b'}},
    {types: ['table', 'key'], style: {color: '#82aaff'}},
    {types: ['punctuation', 'operator'], style: {color: '#8a94ad'}},
  ],
};

// Both samples follow rocketmq-website/examples/first-message, which is compiled against the workspace.
const PRODUCER_CODE = `use std::error::Error;
use std::sync::Arc;

use rocketmq_client_rust::{ClientRuntime, DefaultMQProducer};
use rocketmq_model::common::message::message_single::Message;

async fn produce(client: Arc<ClientRuntime>) -> Result<(), Box<dyn Error>> {
    let mut producer = DefaultMQProducer::builder(client)
        .producer_group("order_producer")
        .name_server_addr("127.0.0.1:9876")
        .build();
    producer.start().await?;

    let message = Message::builder()
        .topic("OrderTopic")
        .tags("created")
        .body("order-1024")
        .build()?;

    if let Some(sent) = producer.send_with_timeout(message, 3_000).await? {
        println!("{} at offset {}", sent.send_status, sent.queue_offset);
    }

    producer.shutdown().await;
    Ok(())
}`;

const CONSUMER_CODE = `use std::error::Error;
use std::sync::Arc;

use rocketmq_client_rust::{ClientRuntime, DefaultLitePullConsumer};
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;

async fn consume(client: Arc<ClientRuntime>) -> Result<(), Box<dyn Error>> {
    let consumer = DefaultLitePullConsumer::builder(client)
        .consumer_group("order_consumer")
        .name_server_addr("127.0.0.1:9876")
        .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .auto_commit(false)
        .build()?;
    consumer.subscribe("OrderTopic").await?;
    consumer.start().await?;

    loop {
        let messages = consumer.poll_with_timeout(1_000).await;
        for message in &messages {
            println!("received {}", message.msg_id());
        }
        if !messages.is_empty() {
            consumer.commit_all().await?;
        }
    }
}`;

const MANIFEST_CODE = `[dependencies]
rocketmq-client-rust = "${VERSION}"
rocketmq-model = "${VERSION}"
rocketmq-runtime = "${VERSION}"
rocketmq-observability = "${VERSION}"
tokio = { version = "1", features = ["macros", "signal", "time"] }`;

type Callout = {
  title: string;
  text: string;
  /** Inclusive, 1-based line range the callout refers to. */
  lines: [number, number];
};

type Sample = {
  id: string;
  file: string;
  language: 'rust' | 'toml';
  code: string;
  callouts: Callout[];
};

export default function CodeShowcase(): React.JSX.Element {
  const baseId = useId();
  const tabRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const [windowRef, inView] = useInView<HTMLDivElement>({once: true, threshold: 0.2});
  const [activeIndex, setActiveIndex] = useState(0);
  const [hovered, setHovered] = useState<number | null>(null);
  const [pinned, setPinned] = useState<number | null>(null);

  const samples: Sample[] = [
    {
      id: 'producer',
      file: 'producer.rs',
      language: 'rust',
      code: PRODUCER_CODE,
      callouts: [
        {
          lines: [8, 12],
          title: translate({id: 'homepage.code.producer.builder.title', message: 'Fluent, typed builders'}),
          text: translate({
            id: 'homepage.code.producer.builder.text',
            message: 'Groups, NameServer addresses and timeouts are plain builder calls on a runtime you own.',
          }),
        },
        {
          lines: [14, 18],
          title: translate({id: 'homepage.code.producer.message.title', message: 'Validated messages'}),
          text: translate({
            id: 'homepage.code.producer.message.text',
            message: 'A message is checked when it is built, before it ever reaches the network.',
          }),
        },
        {
          lines: [20, 22],
          title: translate({id: 'homepage.code.producer.result.title', message: 'Results you have to handle'}),
          text: translate({
            id: 'homepage.code.producer.result.text',
            message: 'Every send returns a typed result carrying the status, message ID and queue offset.',
          }),
        },
      ],
    },
    {
      id: 'consumer',
      file: 'consumer.rs',
      language: 'rust',
      code: CONSUMER_CODE,
      callouts: [
        {
          lines: [8, 15],
          title: translate({id: 'homepage.code.consumer.setup.title', message: 'Explicit setup'}),
          text: translate({
            id: 'homepage.code.consumer.setup.text',
            message: 'Consumer group, start position and commit mode are all visible at the call site.',
          }),
        },
        {
          lines: [17, 21],
          title: translate({id: 'homepage.code.consumer.loop.title', message: 'You drive the loop'}),
          text: translate({
            id: 'homepage.code.consumer.loop.text',
            message: 'LitePull hands you batches on your schedule. Push and POP are there when you want callbacks or receipts.',
          }),
        },
        {
          lines: [22, 24],
          title: translate({id: 'homepage.code.consumer.commit.title', message: 'Progress on your terms'}),
          text: translate({
            id: 'homepage.code.consumer.commit.text',
            message: 'Commit offsets only after your own processing has succeeded.',
          }),
        },
      ],
    },
    {
      id: 'manifest',
      file: 'Cargo.toml',
      language: 'toml',
      code: MANIFEST_CODE,
      callouts: [
        {
          lines: [2, 5],
          title: translate({id: 'homepage.code.manifest.crates.title', message: 'Published on crates.io'}),
          text: translate({
            id: 'homepage.code.manifest.crates.text',
            message: 'Client, model, runtime and observability crates are released together under one version.',
          }),
        },
        {
          lines: [6, 6],
          title: translate({id: 'homepage.code.manifest.tokio.title', message: 'Built on Tokio'}),
          text: translate({
            id: 'homepage.code.manifest.tokio.text',
            message: 'Async end to end, alongside the macros, signals and timers you already use.',
          }),
        },
      ],
    },
  ];

  const sample = samples[activeIndex];
  const focusIndex = hovered ?? pinned;
  const focus = focusIndex === null ? null : sample.callouts[focusIndex]?.lines ?? null;

  const selectTab = (index: number): void => {
    setActiveIndex(index);
    setHovered(null);
    setPinned(null);
  };

  const handleTabKeyDown = (event: React.KeyboardEvent<HTMLButtonElement>): void => {
    const offset = event.key === 'ArrowRight' ? 1 : event.key === 'ArrowLeft' ? -1 : 0;
    if (offset === 0) {
      return;
    }
    event.preventDefault();
    const next = (activeIndex + offset + samples.length) % samples.length;
    selectTab(next);
    tabRefs.current[next]?.focus();
  };

  return (
    <section className={clsx(primitives.section, styles.section)} aria-labelledby={`${baseId}-title`}>
      <div className={styles.glow} aria-hidden="true" />
      <div className={clsx(primitives.container, styles.layout)}>
        <div className={styles.intro}>
          <SectionHeading
            id={`${baseId}-title`}
            align="start"
            eyebrow={translate({id: 'homepage.code.eyebrow', message: 'Developer experience'})}
            title={translate({id: 'homepage.code.title', message: 'An API that feels like Rust'})}
            lead={translate({
              id: 'homepage.code.lead',
              message: 'Builders, typed results and explicit lifecycles. No hidden threads, no surprises.',
            })}
          />

          <Reveal delay={100}>
            <ul className={styles.callouts} onPointerLeave={() => setHovered(null)}>
              {sample.callouts.map((callout, index) => (
                <li key={callout.title}>
                  <button
                    type="button"
                    className={clsx(styles.callout, focusIndex === index && styles.calloutActive)}
                    aria-pressed={pinned === index}
                    onPointerEnter={() => setHovered(index)}
                    onFocus={() => setHovered(index)}
                    onBlur={() => setHovered(null)}
                    onClick={() => setPinned((current) => (current === index ? null : index))}>
                    <span className={styles.calloutIndex}>{index + 1}</span>
                    <span>
                      <strong>{callout.title}</strong>
                      <span className={styles.calloutText}>{callout.text}</span>
                    </span>
                  </button>
                </li>
              ))}
            </ul>

            <div className={styles.links}>
              <Link className={primitives.textLink} to="/docs/producer/overview">
                {translate({id: 'homepage.code.link.producer', message: 'Producer guide'})}
                <ArrowRightIcon />
              </Link>
              <Link className={primitives.textLink} to="/docs/consumer/overview">
                {translate({id: 'homepage.code.link.consumer', message: 'Consumer guide'})}
                <ArrowRightIcon />
              </Link>
              <Link className={primitives.textLink} to="/docs/reference/rust-api">
                {translate({id: 'homepage.code.link.api', message: 'Rust API reference'})}
                <ArrowRightIcon />
              </Link>
            </div>
          </Reveal>
        </div>

        <Reveal className={styles.windowWrap} delay={160}>
          <div ref={windowRef} className={clsx(styles.window, inView && styles.windowVisible)}>
            <div className={styles.windowBar}>
              <div
                className={styles.tabs}
                role="tablist"
                aria-label={translate({id: 'homepage.code.tabs.label', message: 'Code samples'})}>
                {samples.map((item, index) => (
                  <button
                    key={item.id}
                    ref={(element) => {
                      tabRefs.current[index] = element;
                    }}
                    type="button"
                    role="tab"
                    id={`${baseId}-tab-${item.id}`}
                    className={clsx(styles.tab, index === activeIndex && styles.tabActive)}
                    aria-selected={index === activeIndex}
                    aria-controls={`${baseId}-panel`}
                    tabIndex={index === activeIndex ? 0 : -1}
                    onClick={() => selectTab(index)}
                    onKeyDown={handleTabKeyDown}>
                    {item.file}
                  </button>
                ))}
              </div>
              <CopyButton text={sample.code} />
            </div>

            <div
              id={`${baseId}-panel`}
              className={styles.panel}
              role="tabpanel"
              aria-labelledby={`${baseId}-tab-${sample.id}`}
              tabIndex={0}>
              <Highlight theme={CODE_THEME} code={sample.code} language={sample.language}>
                {({tokens, getLineProps, getTokenProps}) => (
                  // Keyed so the line-by-line entrance replays when the tab changes.
                  <pre key={sample.id} className={clsx(styles.code, focus && styles.codeFocused)}>
                    {tokens.map((line, index) => {
                      const lineNumber = index + 1;
                      const focused = focus !== null && lineNumber >= focus[0] && lineNumber <= focus[1];
                      return (
                        <span
                          key={lineNumber}
                          {...getLineProps({line})}
                          className={clsx(styles.line, focused && styles.lineFocused)}
                          style={{'--line': index} as CSSProperties}>
                          <span className={styles.lineNumber} aria-hidden="true">
                            {lineNumber}
                          </span>
                          <span className={styles.lineContent}>
                            {line.map((token, tokenIndex) => (
                              <span key={tokenIndex} {...getTokenProps({token})} />
                            ))}
                          </span>
                        </span>
                      );
                    })}
                  </pre>
                )}
              </Highlight>
            </div>
          </div>
        </Reveal>
      </div>
    </section>
  );
}
