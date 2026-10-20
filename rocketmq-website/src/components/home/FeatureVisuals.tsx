import React, {useId, type CSSProperties} from 'react';
import clsx from 'clsx';
import styles from './FeatureVisuals.module.css';

/*
 * Decorative illustrations for the FeatureBento cards. Each one inherits its
 * colour from the `--accent-rgb` channel triplet set by the surrounding card.
 */

/** The borrow checker rejecting a use-after-move, replayed on a loop. */
export function SafetyVisual({verdict}: {verdict: string}): React.JSX.Element {
  return (
    <div className={styles.snippet}>
      <div className={styles.snippetBar}>
        <i />
        <i />
        <i />
        <span>producer.rs</span>
      </div>
      <pre className={styles.snippetBody}>
        <span className={styles.snippetLine}>
          <span className={styles.kw}>let</span> msg = Message::<span className={styles.fn}>builder</span>()
        </span>
        <span className={styles.snippetLine}>
          {'    '}.<span className={styles.fn}>topic</span>(<span className={styles.str}>"orders"</span>)
        </span>
        <span className={styles.snippetLine}>
          {'    '}.<span className={styles.fn}>build</span>()?;
        </span>
        <span className={styles.snippetLine}>
          producer.<span className={styles.fn}>send</span>(msg).<span className={styles.kw}>await</span>?;{' '}
          <span className={styles.cm}>{'// msg moved'}</span>
        </span>
        <span className={styles.snippetLine}>
          <span className={styles.mac}>println!</span>(<span className={styles.str}>"{'{}'}"</span>,{' '}
          <span className={styles.moved}>msg</span>.<span className={styles.fn}>topic</span>());
        </span>
        <span className={clsx(styles.snippetLine, styles.diagnostic)}>
          <b>error[E0382]</b>: borrow of moved value: `msg`
        </span>
      </pre>
      <div className={styles.verdict}>
        <span>✓</span>
        {verdict}
      </div>
    </div>
  );
}

const PIPELINE_STAGES = ['accept', 'decode', 'process', 'respond'];

/** Requests streaming through the stages of a non-blocking pipeline. */
export function AsyncVisual(): React.JSX.Element {
  return (
    <div className={styles.lanes}>
      {PIPELINE_STAGES.map((stage, index) => (
        <div key={stage} className={styles.lane} style={{'--lane': index} as CSSProperties}>
          <span>{stage}</span>
          <span className={styles.laneTrack}>
            <i />
            <i />
            <i />
          </span>
        </div>
      ))}
    </div>
  );
}

/** Java and Rust endpoints exchanging traffic in both directions. */
export function ProtocolVisual(): React.JSX.Element {
  return (
    <div className={styles.interop}>
      <div className={styles.endpoints}>
        <span className={styles.endpoint}>
          <b>Java</b>
          <small>client · broker</small>
        </span>
        <span className={styles.wire}>
          <i />
          <i />
          <i />
          <i />
        </span>
        <span className={clsx(styles.endpoint, styles.endpointRust)}>
          <b>Rust</b>
          <small>client · broker</small>
        </span>
      </div>
      <div className={styles.tags}>
        <span>Remoting</span>
        <span>gRPC v2</span>
      </div>
    </div>
  );
}

const STORAGE_LAYERS: Array<{name: string; segments: number}> = [
  {name: 'CommitLog', segments: 10},
  {name: 'ConsumeQueue', segments: 8},
  {name: 'Index', segments: 6},
];

/** Store files filling segment by segment, above the available backends. */
export function StorageVisual(): React.JSX.Element {
  return (
    <div className={styles.storage}>
      {STORAGE_LAYERS.map((layer, layerIndex) => (
        <div key={layer.name} className={styles.layer} style={{'--layer': layerIndex} as CSSProperties}>
          <span>{layer.name}</span>
          <span className={styles.segments}>
            {Array.from({length: layer.segments}, (_, index) => (
              <i key={index} style={{'--segment': index} as CSSProperties} />
            ))}
          </span>
        </div>
      ))}
      <div className={styles.tags}>
        <span>Local files</span>
        <span>RocksDB</span>
        <span>Tiered</span>
      </div>
    </div>
  );
}

/** Message-pattern chips lighting up in turn, above the consumption models. */
export function PatternsVisual({patterns}: {patterns: string[]}): React.JSX.Element {
  return (
    <div className={styles.patterns}>
      {patterns.map((pattern, index) => (
        <span key={pattern} className={styles.pattern} style={{'--chip': index} as CSSProperties}>
          {pattern}
        </span>
      ))}
      <div className={styles.tags}>
        <span>Push</span>
        <span>LitePull</span>
        <span>POP</span>
      </div>
    </div>
  );
}

const QUORUM_MEMBERS = [
  {x: 120, y: 34},
  {x: 184, y: 118},
  {x: 56, y: 118},
];

/** Three controller nodes passing leadership around, with heartbeats on every edge. */
export function QuorumVisual(): React.JSX.Element {
  return (
    <svg className={styles.quorum} viewBox="0 0 240 150">
      <path className={styles.quorumEdge} d="M120 34 56 118" />
      <path className={styles.quorumEdge} d="M120 34 184 118" />
      <path className={styles.quorumEdge} d="M56 118H184" />
      {QUORUM_MEMBERS.map((member, index) => (
        <g key={index} className={styles.quorumNode} style={{'--member': index} as CSSProperties}>
          <circle className={styles.quorumRing} cx={member.x} cy={member.y} r="21" />
          <circle className={styles.quorumCore} cx={member.x} cy={member.y} r="13" />
          <text x={member.x} y={member.y + 4}>
            {index + 1}
          </text>
        </g>
      ))}
    </svg>
  );
}

const SECURITY_CHECKS = ['AuthN', 'ACL', 'TLS'];

/** A request clearing authentication, authorization and transport security in sequence. */
export function SecurityVisual(): React.JSX.Element {
  return (
    <div className={styles.checks}>
      <span className={styles.checkRail}>
        <i />
      </span>
      {SECURITY_CHECKS.map((check, index) => (
        <span key={check} className={styles.check} style={{'--check': index} as CSSProperties}>
          <span className={styles.checkMark}>✓</span>
          {check}
        </span>
      ))}
    </div>
  );
}

/** A sparkline drawing itself, above the three telemetry signals. */
export function ObservabilityVisual(): React.JSX.Element {
  const gradientId = `${useId().replace(/:/g, '')}-area`;
  const line = 'M0 78 28 66 56 72 84 44 112 54 140 30 168 40 196 18 224 28 240 14';

  return (
    <div className={styles.telemetry}>
      <svg className={styles.chart} viewBox="0 0 240 96">
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stopColor="currentColor" stopOpacity="0.32" />
            <stop offset="1" stopColor="currentColor" stopOpacity="0" />
          </linearGradient>
        </defs>
        <path className={styles.chartArea} d={`${line}V96H0Z`} fill={`url(#${gradientId})`} />
        {/* pathLength normalises the dash animation to a single unit. */}
        <path className={styles.chartLine} d={line} pathLength="1" />
      </svg>
      <div className={styles.tags}>
        <span>metrics</span>
        <span>traces</span>
        <span>logs</span>
      </div>
    </div>
  );
}
