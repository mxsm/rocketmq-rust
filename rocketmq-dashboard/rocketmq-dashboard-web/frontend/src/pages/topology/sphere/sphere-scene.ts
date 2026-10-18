import {
  AdditiveBlending,
  AmbientLight,
  BackSide,
  BufferGeometry,
  CanvasTexture,
  Color,
  DirectionalLight,
  DoubleSide,
  FrontSide,
  Group,
  LineBasicMaterial,
  LineLoop,
  LineSegments,
  Mesh,
  MeshBasicMaterial,
  MeshLambertMaterial,
  PerspectiveCamera,
  Raycaster,
  RingGeometry,
  Scene,
  ShaderMaterial,
  SphereGeometry,
  Sprite,
  SpriteMaterial,
  SRGBColorSpace,
  TubeGeometry,
  Vector2,
  Vector3,
  WebGLRenderer,
  type InterleavedBuffer,
  type InterleavedBufferAttribute,
  type Object3D,
  type Side
} from 'three';
import { Line2 } from 'three/examples/jsm/lines/Line2.js';
import { LineGeometry } from 'three/examples/jsm/lines/LineGeometry.js';
import { LineMaterial } from 'three/examples/jsm/lines/LineMaterial.js';
import { MAX_PARTICLES, travelSeconds } from '../topology-layout';
import { formatCount } from '../topology-model';
import { Bridge, FunctionCurve, surfaceCurve } from './sphere-geometry';
import { placeLabels, type LabelObstacle, type LabelPlacement, type LabelRequest } from './sphere-labels';
import {
  capAngle,
  CONTROLLER_RADIUS,
  NAMESERVER_RADIUS,
  nodeDescription,
  nodeMetric,
  nodeTitle,
  nodeTone,
  surfacePoint,
  type ClusterMode,
  type ControllerNode,
  type SphereKey,
  type SphereLink,
  type SphereLinkKind,
  type SphereModel,
  type SphereNode,
  type SphereNodeKind,
  type Vec3
} from './sphere-model';

const DEG = Math.PI / 180;
/** The broker sphere turns once a minute, tilted toward its north. */
const BROKER_TILT = 14 * DEG;
const BROKER_TURN_SECONDS = 60;
/**
 * The NameServer sphere sits upper left and sways around its front instead of turning. On wider stages
 * it moves out toward `NAMESERVER_WIDE` and grows, so the two spheres fill the width between them.
 */
const NAMESERVER_CENTRE = new Vector3(-1.46, 0.98, 0);
const NAMESERVER_WIDE = { centre: new Vector3(-3.2, 0.35, 0), scale: 1.5 };
/** The Controller sphere mirrors it, lower right, and is drawn only when brokers name Controllers. */
const CONTROLLER_CENTRE = new Vector3(1.46, -0.98, 0);
const CONTROLLER_WIDE = { centre: new Vector3(3.2, -0.35, 0), scale: 1.5 };
const SMALL_TILT = 8 * DEG;
const SWAY = 18 * DEG;
/** The two small spheres sway at different speeds, so they never move in step. */
const SWAY_SECONDS: Record<SphereKey, number> = { brokers: 0, nameservers: 17, controllers: 21 };
/**
 * Room around each sphere the camera keeps in view, in broker-sphere radii: beside the broker sphere
 * for the labels of its edge nodes, above and below it for the hemisphere titles, on the title side
 * of a small sphere for its title (above the NameServers, below the Controllers).
 */
const MARGIN = { brokerSide: 0.35, brokerTitle: 0.3, smallSide: 0.18, smallBelow: 0.1, smallTitle: 0.26 };
/** The busiest cluster starts this far left of the front, so its neighbour is in view too. */
const FRONT_OFFSET = 30;
const FLIGHT_MS = 900;
const ROUTE_TRAVEL_S = 1.6;
const ROUTE_WINDOW_S = 2.4;
/** Segments of each line between two spheres, rewritten every frame. */
const BRIDGE_STEPS = 64;
/** Tilt of the broker sphere after flying to a node of each kind; the small spheres turn on their own. */
const FOCUS_TILT: Record<SphereNodeKind, number> = { master: 10, slave: 10, producer: 20, consumer: -12, nameserver: 0, controller: 0 };
const NODE_SIZE: Record<SphereNodeKind, number> = { master: 0.04, slave: 0.025, nameserver: 0.032, controller: 0.03, producer: 0.022, consumer: 0.022 };
/** Badge of a healthy cluster's title. */
const MODE_TEXT: Record<ClusterMode, string> = { single: 'Single master', 'master-slave': 'Master/Slave', controller: 'Controller mode' };

/** A stable fraction per id, so particles start spread along their paths. */
function phaseOf(id: string) {
  let hash = 0;
  for (const character of id) hash = (hash * 31 + character.charCodeAt(0)) >>> 0;
  return (hash % 1000) / 1000;
}

interface Palette {
  primary: string;
  consume: string;
  nameserver: string;
  route: string;
  controller: string;
  heartbeat: string;
  success: string;
  warning: string;
  danger: string;
  subtle: string;
  equator: string;
  body: string;
  glow: string;
  grid: string;
}

function readPalette(element: HTMLElement): Palette {
  const style = getComputedStyle(element);
  const read = (name: string, fallback: string) => style.getPropertyValue(name).trim() || fallback;
  return {
    primary: read('--primary', '#00b4d8'),
    consume: read('--accent-cyan', '#00c4c9'),
    nameserver: read('--accent-blue', '#00d5ff'),
    // Its own hue: the producer, consumer and NameServer blues are too close to tell the lines apart.
    route: read('--accent-violet', '#b15cff'),
    // The control plane in a neutral silver, apart from every message path and from warnings.
    controller: read('--accent-silver', '#cfd8e6'),
    heartbeat: read('--accent-pink', '#f72585'),
    success: read('--success', '#00e64d'),
    warning: read('--warning', '#ffc300'),
    danger: read('--destructive', '#ff2e6e'),
    subtle: read('--subtle-foreground', '#8b98a5'),
    equator: read('--border-strong', '#2b343e'),
    body: read('--sphere-body', '#0c1820'),
    glow: read('--sphere-glow', '#02070a'),
    grid: read('--sphere-grid', '#1d2b35')
  };
}

/**
 * Message links are tubes on the broker sphere. Lines to and from the NameServers are dashed and
 * wider, in pixels, so they stand out against the tubes they cross.
 */
const LINK_STYLE: Record<SphereLinkKind, { color: keyof Palette; radius: number; opacity: number; width: number }> = {
  produce: { color: 'primary', radius: 0.0045, opacity: 0.5, width: 1.4 },
  consume: { color: 'consume', radius: 0.0045, opacity: 0.5, width: 1.4 },
  replication: { color: 'success', radius: 0.0075, opacity: 0.8, width: 1.4 },
  heartbeat: { color: 'heartbeat', radius: 0, opacity: 0.85, width: 2 },
  route: { color: 'route', radius: 0, opacity: 0.7, width: 1.7 },
  election: { color: 'controller', radius: 0, opacity: 0.75, width: 1.7 },
  raft: { color: 'controller', radius: 0.0045, opacity: 0.85, width: 1.4 }
};
/** Speed of the dashes on NameServer lines, in broker-sphere radii per second. */
const DASH_SPEED = 0.12;

// Glows use the per-vertex view direction, so a sphere away from the screen centre gets an even rim.
const GLOW_VERTEX = `varying vec3 vNormal;
varying vec3 vView;
void main() {
  vec4 mv = modelViewMatrix * vec4(position, 1.0);
  vNormal = normalize(normalMatrix * normal);
  vView = normalize(-mv.xyz);
  gl_Position = projectionMatrix * mv;
}`;
const HALO_FRAGMENT = `uniform vec3 glowColor;
uniform float strength;
varying vec3 vNormal;
varying vec3 vView;
void main() {
  float facing = clamp(-dot(normalize(vNormal), normalize(vView)) / 0.5, 0.0, 1.0);
  gl_FragColor = vec4(glowColor, pow(facing, 1.6) * strength);
  #include <colorspace_fragment>
}`;
const RIM_FRAGMENT = `uniform vec3 glowColor;
uniform float strength;
varying vec3 vNormal;
varying vec3 vView;
void main() {
  float rim = pow(1.0 - clamp(dot(normalize(vNormal), normalize(vView)), 0.0, 1.0), 3.0);
  gl_FragColor = vec4(glowColor, rim * strength);
  #include <colorspace_fragment>
}`;

function glowMaterial(fragmentShader: string, color: string, strength: number, side: Side) {
  return new ShaderMaterial({
    uniforms: { glowColor: { value: new Color(color) }, strength: { value: strength } },
    vertexShader: GLOW_VERTEX,
    fragmentShader,
    side,
    blending: AdditiveBlending,
    transparent: true,
    depthWrite: false
  });
}

function glowTexture() {
  const size = 64;
  const canvas = document.createElement('canvas');
  canvas.width = size;
  canvas.height = size;
  const context = canvas.getContext('2d');
  if (context) {
    const gradient = context.createRadialGradient(size / 2, size / 2, 0, size / 2, size / 2, size / 2);
    gradient.addColorStop(0, 'rgba(255,255,255,1)');
    gradient.addColorStop(0.25, 'rgba(255,255,255,0.5)');
    gradient.addColorStop(1, 'rgba(255,255,255,0)');
    context.fillStyle = gradient;
    context.fillRect(0, 0, size, size);
  }
  const texture = new CanvasTexture(canvas);
  texture.colorSpace = SRGBColorSpace;
  return texture;
}

const toVector = (point: Vec3) => new Vector3(point[0], point[1], point[2]);
const Z_AXIS = new Vector3(0, 0, 1);

interface Disposable {
  dispose(): void;
}

interface Flight {
  fromSpin: number;
  toSpin: number;
  fromTilt: number;
  toTilt: number;
  start: number;
}

interface Rig {
  key: SphereKey;
  group: Group;
  pivot: Group;
  body: Mesh;
  radius: number;
  centre: Vector3;
  defaults: { spin: number; tilt: number };
  view: { spin: number; tilt: number; phase: number; flight: Flight | null };
}

interface LabelView {
  element: HTMLElement;
  width: number;
  height: number;
  shown: boolean | null;
  transform: string;
  opacity: string;
}

interface NodeView {
  node: SphereNode;
  rig: Rig;
  local: Vector3;
  normal: Vector3;
  world: Vector3;
  material: MeshBasicMaterial;
  halo: SpriteMaterial;
  decorations: MeshBasicMaterial[];
  pulse: Mesh<RingGeometry, MeshBasicMaterial> | null;
  label: LabelView;
  title: HTMLElement | null;
  metric: HTMLElement;
  priority: number;
}

interface BridgeLine {
  bridge: Bridge;
  from: NodeView;
  to: NodeView;
  segments: InterleavedBuffer;
  distances: InterleavedBuffer;
}

interface LinkView {
  link: SphereLink;
  material: MeshBasicMaterial | LineMaterial;
  baseOpacity: number;
  length: number;
  pointAt: (t: number, target: Vector3) => Vector3;
  bridge: BridgeLine | null;
}

type Timing = { kind: 'flow'; period: number } | { kind: 'pulse'; every: number; travel: number; delay: number };

interface Particle {
  link: LinkView;
  head: Mesh;
  halo: Sprite;
  tails: Mesh[];
  offset: number;
  timing: Timing;
  dim: boolean;
}

interface RouteFlight {
  link: LinkView;
  head: Mesh;
  halo: Sprite;
  delay: number;
}

type TitleKind = 'cluster' | 'nameservers' | 'controllers' | 'producers' | 'consumers';

interface TitleView {
  kind: TitleKind;
  label: LabelView;
  heading: HTMLElement;
  metric: HTMLElement;
  /** Point on the broker sphere a cluster title sits on. */
  anchor: Vector3 | null;
  clusterName: string | null;
}

interface ParticleKit {
  head: MeshBasicMaterial;
  tail: MeshBasicMaterial;
  halo: SpriteMaterial;
}

interface Content {
  roots: Object3D[];
  /** Per-sphere groups holding this model's objects, turning with their sphere. */
  layers: Record<SphereKey, Group>;
  nameServers: boolean;
  controllers: boolean;
  disposables: Disposable[];
  nodes: Map<string, NodeView>;
  links: LinkView[];
  particles: Particle[];
  flights: RouteFlight[];
  titles: TitleView[];
  hitTargets: Mesh[];
  selection: Mesh<RingGeometry, MeshBasicMaterial>;
}

interface Drag {
  x: number;
  y: number;
  rig: Rig;
  spin: number;
  tilt: number;
  moved: boolean;
  pointerId: number;
}

export interface SphereSceneHandlers {
  /** Called when the selection changes: a click on the stage or a label, Escape, a reset, or a refresh that removed the node. */
  onSelect: (id: string | null) => void;
}

export interface SphereSceneElements {
  /** Positioned container the scene's canvas fills; pointer input is read from it. */
  stage: HTMLElement;
  /** Empty container the scene fills with label buttons. */
  labels: HTMLElement;
  /** Buttons drawn over the stage, which labels keep clear of. */
  controls: HTMLElement | null;
}

const plural = (count: number, one: string, many: string) => `${formatCount(count)} ${count === 1 ? one : many}`;

function basePriority(node: SphereNode) {
  switch (node.kind) {
    case 'master':
      return 90;
    case 'nameserver':
    case 'controller':
      return 85;
    case 'consumer':
      return nodeTone(node) === 'warning' ? 75 : 60;
    case 'slave':
      return 30;
    default:
      return 50;
  }
}

/**
 * The three.js scene behind the sphere view: the broker sphere with its clusters and client groups,
 * the NameServer sphere, the Controller sphere when brokers name Controllers, the links between them,
 * particles, and the HTML labels over the canvas.
 *
 * # Errors
 *
 * The constructor throws when the browser cannot create a WebGL context.
 */
export class TopologySphereScene {
  private readonly elements: SphereSceneElements;
  private readonly handlers: SphereSceneHandlers;
  /**
   * Each scene owns its canvas. A WebGL context lost on dispose cannot be revived, so a scene
   * created again for the same stage, as React does when it remounts, needs a canvas of its own.
   */
  private readonly canvas: HTMLCanvasElement;
  private readonly renderer: WebGLRenderer;
  private readonly scene = new Scene();
  private readonly camera = new PerspectiveCamera(30, 1, 0.1, 100);
  private readonly palette: Palette;
  private readonly disposables: Disposable[] = [];
  private readonly glow: CanvasTexture;
  private readonly bodyMaterial: MeshLambertMaterial;
  private readonly rigs: Record<SphereKey, Rig>;
  private readonly raycaster = new Raycaster();
  private readonly pointer = new Vector2();
  private readonly lineMaterials = new Set<LineMaterial>();
  private readonly resizeObserver: ResizeObserver | null;
  private readonly clock = { last: 0, elapsed: 0, refreshAt: Number.NEGATIVE_INFINITY };
  private readonly scratch = new Vector3();
  private readonly toCamera = new Vector3();
  private readonly fromCentre = new Vector3();
  private readonly anchor = new Vector3();
  private readonly lineStart = new Vector3();
  private readonly lineEnd = new Vector3();
  private controlsBox: LabelObstacle = { x: 0, y: 0, width: 0, height: 0, owner: null };
  private content: Content | null = null;
  private model: SphereModel | null = null;
  private selected: string | null = null;
  private hovered: string | null = null;
  private related = new Set<string>();
  private paused = false;
  private hidden = false;
  private reducedMotion: boolean;
  private drag: Drag | null = null;

  constructor(elements: SphereSceneElements, handlers: SphereSceneHandlers, options: { reducedMotion: boolean }) {
    this.elements = elements;
    this.handlers = handlers;
    this.reducedMotion = options.reducedMotion;
    this.canvas = document.createElement('canvas');
    this.canvas.className = 'topology-sphere-canvas';
    this.canvas.setAttribute('aria-hidden', 'true');
    try {
      this.renderer = new WebGLRenderer({ canvas: this.canvas, antialias: true, alpha: true, powerPreference: 'high-performance' });
    } catch (error) {
      this.canvas.remove();
      throw error;
    }
    elements.stage.prepend(this.canvas);
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    this.renderer.setClearColor(0x000000, 0);
    this.palette = readPalette(elements.stage);
    this.glow = this.keep(glowTexture());
    this.bodyMaterial = this.keep(new MeshLambertMaterial({ color: this.palette.body, emissive: this.palette.glow }));

    this.scene.add(new AmbientLight(0xffffff, 1.9));
    const keyLight = new DirectionalLight(0xcdefff, 2.6);
    keyLight.position.set(-3, 2.4, 4);
    this.scene.add(keyLight);

    this.rigs = {
      brokers: this.buildRig('brokers', new Vector3(0, 0, 0), 1, this.palette.primary, BROKER_TILT),
      nameservers: this.buildRig('nameservers', NAMESERVER_CENTRE, NAMESERVER_RADIUS, this.palette.nameserver, SMALL_TILT),
      controllers: this.buildRig('controllers', CONTROLLER_CENTRE, CONTROLLER_RADIUS, this.palette.controller, SMALL_TILT)
    };
    this.rigs.controllers.group.visible = false;
    this.rigs.brokers.pivot.add(this.loop((lon) => toVector(surfacePoint(0, lon, 1.003)), 120, this.keep(new LineBasicMaterial({ color: this.palette.equator }))));

    const { stage } = elements;
    stage.addEventListener('pointerdown', this.onPointerDown);
    stage.addEventListener('pointermove', this.onPointerMove);
    stage.addEventListener('pointerup', this.onPointerUp);
    stage.addEventListener('pointercancel', this.onPointerUp);
    stage.addEventListener('pointerleave', this.onPointerLeave);
    document.addEventListener('keydown', this.onKeyDown);
    this.resizeObserver = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(() => this.resize());
    this.resizeObserver?.observe(stage);
    document.fonts?.ready.then(() => this.measureLabels()).catch(() => undefined);
    this.resize();
    this.renderer.setAnimationLoop(this.frame);
  }

  /** Draws `model`. The same structure only updates readings, keeping the view and the selection. */
  setModel(model: SphereModel): void {
    const previous = this.model;
    this.model = model;
    if (previous && this.content && previous.structure === model.structure) {
      this.updateReadings(model);
      return;
    }
    if (!previous) {
      const spin = (-FRONT_OFFSET - model.frontLon) * DEG;
      this.rigs.brokers.defaults.spin = spin;
      this.rigs.brokers.view.spin = spin;
    }
    this.rebuild(model);
    if (this.selected && !this.content?.nodes.has(this.selected)) this.select(null);
    else this.applyFocus();
  }

  /** Freezes rotation and particles; dragging and selecting still work. */
  setPaused(paused: boolean): void {
    this.paused = paused;
  }

  /** Stops drawing while the page is hidden. */
  setHidden(hidden: boolean): void {
    if (hidden === this.hidden) return;
    this.hidden = hidden;
    this.clock.last = 0;
    this.renderer.setAnimationLoop(hidden ? null : this.frame);
  }

  setReducedMotion(reducedMotion: boolean): void {
    this.reducedMotion = reducedMotion;
  }

  /** Sends one round of route answers from the NameServers to the client groups. */
  markRefresh(): void {
    this.clock.refreshAt = this.clock.elapsed;
  }

  /** Selects a node, turning its sphere so the node faces the viewer, or clears the selection. */
  select(id: string | null): void {
    const next = id && this.content?.nodes.has(id) ? id : null;
    this.selected = next;
    if (next) this.flyToNode(next);
    this.applyFocus();
    this.handlers.onSelect(next);
  }

  /** Clears the selection and turns every sphere back to where it started. */
  reset(): void {
    this.hovered = null;
    for (const rig of Object.values(this.rigs)) this.flyTo(rig, rig.defaults.spin, rig.defaults.tilt);
    this.select(null);
  }

  dispose(): void {
    this.renderer.setAnimationLoop(null);
    const { stage } = this.elements;
    stage.removeEventListener('pointerdown', this.onPointerDown);
    stage.removeEventListener('pointermove', this.onPointerMove);
    stage.removeEventListener('pointerup', this.onPointerUp);
    stage.removeEventListener('pointercancel', this.onPointerUp);
    stage.removeEventListener('pointerleave', this.onPointerLeave);
    document.removeEventListener('keydown', this.onKeyDown);
    this.resizeObserver?.disconnect();
    this.clearContent();
    for (const item of this.disposables) item.dispose();
    this.renderer.dispose();
    // Frees the context now instead of at garbage collection; browsers cap live WebGL contexts.
    this.renderer.forceContextLoss();
    this.canvas.remove();
  }

  private keep<T extends Disposable>(item: T, list: Disposable[] = this.disposables): T {
    list.push(item);
    return item;
  }

  private buildRig(key: SphereKey, centre: Vector3, radius: number, tint: string, tilt: number): Rig {
    const brokers = key === 'brokers';
    const group = new Group();
    group.position.copy(centre);
    this.scene.add(group);
    const pivot = new Group();
    group.add(pivot);
    const body = new Mesh(this.keep(new SphereGeometry(radius, brokers ? 96 : 64, brokers ? 64 : 40)), this.bodyMaterial);
    body.userData.sphere = key;
    pivot.add(body);
    pivot.add(this.graticule(radius, brokers ? 30 : 45, brokers ? [-60, -30, 30, 60] : [-45, 45]));
    group.add(new Mesh(this.keep(new SphereGeometry(radius * 1.16, 64, 48)), this.keep(glowMaterial(HALO_FRAGMENT, tint, brokers ? 0.36 : 0.55, BackSide))));
    group.add(new Mesh(this.keep(new SphereGeometry(radius * 1.003, 96, 64)), this.keep(glowMaterial(RIM_FRAGMENT, tint, brokers ? 0.32 : 0.55, FrontSide))));
    return { key, group, pivot, body, radius, centre: group.position, defaults: { spin: 0, tilt }, view: { spin: 0, tilt, phase: 0, flight: null } };
  }

  private graticule(radius: number, step: number, parallels: number[]) {
    const points: Vector3[] = [];
    const r = radius * 1.002;
    for (let lon = 0; lon < 360; lon += step) {
      for (let lat = -88; lat < 88; lat += 4) points.push(toVector(surfacePoint(lat, lon, r)), toVector(surfacePoint(lat + 4, lon, r)));
    }
    for (const lat of parallels) {
      for (let lon = 0; lon < 360; lon += 4) points.push(toVector(surfacePoint(lat, lon, r)), toVector(surfacePoint(lat, lon + 4, r)));
    }
    return new LineSegments(this.keep(new BufferGeometry().setFromPoints(points)), this.keep(new LineBasicMaterial({ color: this.palette.grid })));
  }

  private loop(pointAt: (degrees: number) => Vector3, steps: number, material: LineBasicMaterial, list: Disposable[] = this.disposables) {
    const points: Vector3[] = [];
    for (let index = 0; index < steps; index += 1) points.push(pointAt((index / steps) * 360));
    return new LineLoop(this.keep(new BufferGeometry().setFromPoints(points), list), material);
  }

  /* ---------- Content built from the model ---------- */

  private clearContent() {
    const content = this.content;
    if (!content) return;
    for (const root of content.roots) root.removeFromParent();
    for (const item of content.disposables) item.dispose();
    for (const link of content.links) if (link.material instanceof LineMaterial) this.lineMaterials.delete(link.material);
    this.elements.labels.replaceChildren();
    this.content = null;
  }

  private rebuild(model: SphereModel) {
    this.clearContent();
    const disposables: Disposable[] = [];
    const keep = <T extends Disposable>(item: T) => this.keep(item, disposables);
    const roots: Object3D[] = [];
    const layer = (parent: Object3D) => {
      const group = new Group();
      parent.add(group);
      roots.push(group);
      return group;
    };
    const layers: Record<SphereKey, Group> = {
      brokers: layer(this.rigs.brokers.pivot),
      nameservers: layer(this.rigs.nameservers.pivot),
      controllers: layer(this.rigs.controllers.pivot)
    };
    const world = layer(this.scene);
    const palette = this.palette;

    for (const cluster of model.clusters) {
      const curve = new FunctionCurve((t, target) => {
        const point = surfacePoint(0, cluster.from + (cluster.to - cluster.from) * t, 1.004);
        return target.set(point[0], point[1], point[2]);
      });
      const color = cluster.healthy ? (cluster.mode === 'single' ? palette.primary : palette.success) : palette.danger;
      layers.brokers.add(new Mesh(keep(new TubeGeometry(curve, 48, 0.013, 6, false)), keep(new MeshBasicMaterial({ color, transparent: true, opacity: 0.36, depthWrite: false }))));
    }
    // The nodes of a small sphere sit on one circle around its front; the circle ties them together.
    const ring = (sphere: 'nameservers' | 'controllers', radius: number, color: string) => {
      const count = model.nodes.filter((node) => node.sphere === sphere).length;
      if (count < 2) return;
      const alpha = capAngle(count);
      layers[sphere].add(this.loop(
        (degrees) => new Vector3(Math.sin(alpha) * Math.cos(degrees * DEG), Math.sin(alpha) * Math.sin(degrees * DEG), Math.cos(alpha)).multiplyScalar(radius * 1.006),
        96,
        keep(new LineBasicMaterial({ color, transparent: true, opacity: 0.5 })),
        disposables
      ));
    };
    ring('nameservers', NAMESERVER_RADIUS, palette.nameserver);
    ring('controllers', CONTROLLER_RADIUS, palette.controller);

    const nodes = new Map<string, NodeView>();
    const hitTargets: Mesh[] = [];
    const headGeometry = keep(new SphereGeometry(0.016, 12, 8));
    const tailGeometry = keep(new SphereGeometry(0.011, 10, 6));
    for (const node of model.nodes) nodes.set(node.id, this.buildNode(node, layers[node.sphere], keep));
    for (const view of nodes.values()) {
      const hit = new Mesh(keep(new SphereGeometry(Math.max(NODE_SIZE[view.node.kind] * 2.2, 0.06), 10, 8)), keep(new MeshBasicMaterial({ visible: false })));
      hit.position.copy(view.local);
      hit.userData.nodeId = view.node.id;
      layers[view.node.sphere].add(hit);
      hitTargets.push(hit);
    }

    const links: LinkView[] = [];
    for (const link of model.links) {
      const from = nodes.get(link.from);
      const to = nodes.get(link.to);
      if (!from || !to) continue;
      links.push(from.rig === to.rig ? this.buildSurfaceLink(link, from, to, layers[from.node.sphere], keep) : this.buildBridgeLink(link, from, to, world, keep));
    }

    const kits = {
      produce: this.particleKit(palette.primary, keep),
      consume: this.particleKit(palette.consume, keep),
      replication: this.particleKit(palette.success, keep),
      heartbeat: this.particleKit(palette.heartbeat, keep),
      route: this.particleKit(palette.route, keep),
      control: this.particleKit(palette.controller, keep)
    };
    const particles: Particle[] = [];
    const addParticle = (link: LinkView, kit: ParticleKit, offset: number, timing: Timing) => {
      const parent = link.bridge ? world : layers[nodes.get(link.link.from)!.node.sphere];
      const head = new Mesh(headGeometry, kit.head);
      const tails = [0, 1, 2].map((index) => {
        const tail = new Mesh(tailGeometry, kit.tail);
        tail.scale.setScalar(1 - index * 0.25);
        parent.add(tail);
        return tail;
      });
      const halo = new Sprite(kit.halo);
      halo.scale.setScalar(0.11);
      parent.add(head, halo);
      particles.push({ link, head, halo, tails, offset, timing, dim: false });
    };
    // One particle per busy message link, the busiest first, up to the same cap as the flat map.
    const flowing = links
      .filter((view) => (view.link.kind === 'produce' || view.link.kind === 'consume' || view.link.kind === 'replication') && !view.link.idle && view.link.tps > 0)
      .sort((left, right) => right.link.tps - left.link.tps || left.link.id.localeCompare(right.link.id))
      .slice(0, MAX_PARTICLES);
    for (const view of flowing) {
      addParticle(view, kits[view.link.kind as 'produce' | 'consume' | 'replication'], phaseOf(view.link.id), { kind: 'flow', period: this.period(view) });
    }
    // Heartbeat timing is illustrative: it shows that brokers register, not when they did.
    for (const view of links) {
      if (view.link.kind !== 'heartbeat' || view.link.unavailable) continue;
      addParticle(view, kits.heartbeat, 0, { kind: 'pulse', every: 3, travel: 1.4, delay: phaseOf(view.link.from) * 2.5 + phaseOf(view.link.to) * 0.5 });
    }
    // Brokers report to the leading Controller, which keeps its followers' logs in step; both are
    // shown as a beat, like the heartbeats.
    for (const view of links) {
      if (view.link.unavailable) continue;
      if (view.link.kind === 'election') {
        addParticle(view, kits.control, 0, { kind: 'pulse', every: 2, travel: 1.2, delay: phaseOf(view.link.from) * 0.8 });
      } else if (view.link.kind === 'raft') {
        addParticle(view, kits.control, 0, { kind: 'pulse', every: 1.5, travel: 0.6, delay: phaseOf(view.link.to) * 0.5 });
      }
    }
    // On every refresh one answer travels each route line, from the NameServer to the client group.
    const flights: RouteFlight[] = [];
    for (const view of links) {
      if (view.link.kind !== 'route') continue;
      const head = new Mesh(headGeometry, kits.route.head);
      const halo = new Sprite(kits.route.halo);
      halo.scale.setScalar(0.1);
      head.visible = false;
      halo.visible = false;
      world.add(head, halo);
      flights.push({ link: view, head, halo, delay: phaseOf(view.link.to) * 0.5 });
    }

    const selection = new Mesh(keep(new RingGeometry(0.075, 0.084, 56)), keep(new MeshBasicMaterial({ color: palette.primary, transparent: true, opacity: 0.95, side: DoubleSide, depthWrite: false })));
    selection.visible = false;
    layers.brokers.add(selection);

    const titles = this.buildTitles(model);
    const nameServers = model.nodes.some((node) => node.sphere === 'nameservers');
    const controllers = model.nodes.some((node) => node.sphere === 'controllers');
    this.rigs.nameservers.group.visible = nameServers;
    this.rigs.controllers.group.visible = controllers;
    this.content = { roots, layers, disposables, nodes, links, particles, flights, titles, hitTargets, selection, nameServers, controllers };
    // Framing depends on which spheres are drawn; resizing also measures the new labels.
    this.resize();
  }

  private buildNode(node: SphereNode, parent: Group, keep: <T extends Disposable>(item: T) => T): NodeView {
    const rig = this.rigs[node.sphere];
    const local = toVector(node.position);
    const normal = local.clone().normalize();
    const color = this.nodeColor(node);
    const size = NODE_SIZE[node.kind];
    const material = keep(new MeshBasicMaterial({ color, transparent: true, opacity: 1 }));
    const mesh = new Mesh(keep(new SphereGeometry(size, 20, 14)), material);
    mesh.position.copy(local);
    const halo = keep(new SpriteMaterial({ map: this.glow, color, transparent: true, opacity: 0.55, blending: AdditiveBlending, depthWrite: false }));
    const sprite = new Sprite(halo);
    sprite.scale.setScalar(size * 5.2);
    sprite.position.copy(local);
    parent.add(mesh, sprite);
    const ring = (inner: number, outer: number, ringColor: string, opacity: number) => {
      const ringMaterial = keep(new MeshBasicMaterial({ color: ringColor, transparent: true, opacity, side: DoubleSide, depthWrite: false }));
      const mark = new Mesh(keep(new RingGeometry(inner, outer, 48)), ringMaterial);
      mark.position.copy(local);
      mark.quaternion.setFromUnitVectors(Z_AXIS, normal);
      parent.add(mark);
      return mark;
    };
    const decorations: MeshBasicMaterial[] = [];
    if (node.kind === 'master') decorations.push(ring(0.054, 0.06, color, 0.8).material);
    // A second, silver ring: the Controllers elect this master.
    if (node.kind === 'master' && node.group.election === 'controller') decorations.push(ring(0.068, 0.073, this.palette.controller, 0.85).material);
    if (node.kind === 'nameserver' && node.nameServer.current) decorations.push(ring(0.048, 0.055, this.palette.primary, 0.85).material);
    if (node.kind === 'controller' && node.controller.leader) decorations.push(ring(0.046, 0.054, this.palette.controller, 0.9).material);
    // Consumers carry a ring so they read apart from producers, whose palette colour is close.
    if (node.kind === 'consumer') decorations.push(ring(0.034, 0.04, color, 0.9).material);
    const tone = nodeTone(node);
    const alarming =
      ((node.kind === 'consumer' || node.kind === 'slave') && tone === 'warning') ||
      (node.kind === 'master' && tone === 'danger') ||
      (node.kind === 'controller' && (tone === 'danger' || tone === 'warning'));
    const pulse = alarming ? ring(0.04, 0.046, color, 0.7) : null;

    const element = document.createElement('button');
    element.type = 'button';
    element.className = 'topology-sphere-label';
    element.dataset.kind = node.kind;
    element.dataset.tone = tone;
    element.setAttribute('aria-pressed', 'false');
    const titleText = nodeTitle(node);
    let title: HTMLElement | null = null;
    if (titleText) {
      title = document.createElement('span');
      title.className = 'topology-sphere-label-name';
      title.textContent = titleText;
      element.append(title);
    }
    const metric = document.createElement('span');
    metric.className = 'topology-sphere-label-metric';
    metric.textContent = nodeMetric(node);
    element.append(metric);
    element.setAttribute('aria-label', nodeDescription(node));
    element.addEventListener('click', (event) => {
      event.stopPropagation();
      this.select(this.selected === node.id ? null : node.id);
    });
    element.addEventListener('pointerenter', () => this.setHover(node.id));
    element.addEventListener('pointerleave', () => this.setHover(null, node.id));
    element.addEventListener('focus', () => this.setHover(node.id));
    element.addEventListener('blur', () => this.setHover(null, node.id));
    this.elements.labels.append(element);
    return {
      node,
      rig,
      local,
      normal,
      world: new Vector3(),
      material,
      halo,
      decorations,
      pulse,
      label: { element, width: 0, height: 0, shown: null, transform: '', opacity: '' },
      title,
      metric,
      priority: basePriority(node)
    };
  }

  private nodeColor(node: SphereNode) {
    const tone = nodeTone(node);
    if (tone === 'danger') return this.palette.danger;
    if (tone === 'warning') return this.palette.warning;
    if (tone === 'idle' || tone === 'unknown') return this.palette.subtle;
    switch (node.kind) {
      case 'producer':
        return this.palette.primary;
      case 'consumer':
        return this.palette.consume;
      case 'nameserver':
        return tone === 'current' ? this.palette.primary : this.palette.success;
      case 'controller':
        return this.palette.controller;
      default:
        return this.palette.success;
    }
  }

  private dashedMaterial(color: string, opacity: number, width: number, keep: <T extends Disposable>(item: T) => T) {
    const material = keep(new LineMaterial({ color, linewidth: width, dashed: true, dashSize: 0.035, gapSize: 0.03, transparent: true, opacity, depthWrite: false }));
    material.resolution.set(this.elements.stage.clientWidth || 1, this.elements.stage.clientHeight || 1);
    this.lineMaterials.add(material);
    return material;
  }

  private buildSurfaceLink(link: SphereLink, from: NodeView, to: NodeView, parent: Group, keep: <T extends Disposable>(item: T) => T): LinkView {
    const style = LINK_STYLE[link.kind];
    const curve = surfaceCurve(from.local, to.local, link.kind === 'replication' ? 0.03 : undefined);
    const pointAt = (t: number, target: Vector3) => curve.getPoint(t, target);
    const length = curve.getLength();
    if (link.idle) {
      const geometry = keep(new LineGeometry());
      geometry.setPositions(curve.getPoints(72).flatMap((point) => [point.x, point.y, point.z]));
      const material = this.dashedMaterial(this.palette.subtle, style.opacity, style.width, keep);
      const line = new Line2(geometry, material);
      line.computeLineDistances();
      parent.add(line);
      return { link, material, baseOpacity: style.opacity, length, pointAt, bridge: null };
    }
    const color = link.outOfSync ? this.palette.warning : this.palette[style.color];
    const material = keep(new MeshBasicMaterial({ color, transparent: true, opacity: style.opacity, depthWrite: false }));
    parent.add(new Mesh(keep(new TubeGeometry(curve, 72, style.radius, 5, false)), material));
    return { link, material, baseOpacity: style.opacity, length, pointAt, bridge: null };
  }

  // Lines between spheres are rewritten in place every frame: no new buffers, only new numbers.
  private buildBridgeLink(link: SphereLink, from: NodeView, to: NodeView, parent: Group, keep: <T extends Disposable>(item: T) => T): LinkView {
    const style = LINK_STYLE[link.kind];
    const opacity = link.unavailable ? 0.35 : style.opacity;
    const geometry = keep(new LineGeometry());
    geometry.setPositions(new Float32Array((BRIDGE_STEPS + 1) * 3));
    const material = this.dashedMaterial(link.unavailable ? this.palette.danger : this.palette[style.color], opacity, style.width, keep);
    const line = new Line2(geometry, material);
    line.computeLineDistances();
    line.frustumCulled = false;
    parent.add(line);
    const bridge = new Bridge();
    const segments = (geometry.attributes.instanceStart as InterleavedBufferAttribute).data;
    const distances = (geometry.attributes.instanceDistanceStart as InterleavedBufferAttribute).data;
    return {
      link,
      material,
      baseOpacity: opacity,
      length: 0,
      pointAt: (t, target) => bridge.pointAt(t, target),
      bridge: { bridge, from, to, segments, distances }
    };
  }

  private particleKit(color: string, keep: <T extends Disposable>(item: T) => T): ParticleKit {
    return {
      head: keep(new MeshBasicMaterial({ color: new Color(color).lerp(new Color(0xffffff), 0.45), transparent: true, depthWrite: false })),
      tail: keep(new MeshBasicMaterial({ color, transparent: true, opacity: 0.5, depthWrite: false })),
      halo: keep(new SpriteMaterial({ map: this.glow, color, transparent: true, opacity: 0.6, blending: AdditiveBlending, depthWrite: false }))
    };
  }

  private period(view: LinkView) {
    return travelSeconds(view.link.tps) * (0.6 + view.length * 0.32);
  }

  private buildTitles(model: SphereModel): TitleView[] {
    const titles: TitleView[] = [];
    const add = (kind: TitleKind, anchor: Vector3 | null, clusterName: string | null) => {
      const element = document.createElement('div');
      element.className = 'topology-sphere-title';
      element.dataset.kind = kind;
      const heading = document.createElement('span');
      heading.className = kind === 'cluster' ? 'topology-sphere-title-name' : 'topology-overline';
      const metric = document.createElement('span');
      metric.className = 'topology-sphere-title-metric';
      element.append(heading, metric);
      this.elements.labels.prepend(element);
      const view: TitleView = { kind, label: { element, width: 0, height: 0, shown: null, transform: '', opacity: '' }, heading, metric, anchor, clusterName };
      titles.push(view);
      return view;
    };
    if (model.nodes.some((node) => node.kind === 'nameserver')) add('nameservers', null, null);
    if (model.nodes.some((node) => node.kind === 'controller')) add('controllers', null, null);
    add('producers', null, null);
    add('consumers', null, null);
    for (const cluster of model.clusters) add('cluster', toVector(surfacePoint(18, cluster.centre, 1)), cluster.clusterName);
    this.writeTitles(model, titles);
    return titles;
  }

  private writeTitles(model: SphereModel, titles: readonly TitleView[]) {
    const servers = model.nodes.filter((node) => node.kind === 'nameserver');
    const checked = servers.filter((node) => node.kind === 'nameserver' && node.nameServer.status !== 'unknown');
    const available = checked.filter((node) => node.kind === 'nameserver' && node.nameServer.status === 'available').length;
    const controllers = model.nodes.filter((node): node is ControllerNode => node.kind === 'controller');
    const leader = controllers.find((node) => node.controller.leader === true);
    for (const title of titles) {
      if (title.kind === 'nameservers') {
        title.heading.textContent = 'NameServers';
        title.metric.textContent = checked.length > 0 ? `${formatCount(available)} of ${formatCount(servers.length)} available` : `${formatCount(servers.length)} configured`;
      } else if (title.kind === 'controllers') {
        title.heading.textContent = 'Controllers · Raft';
        title.metric.textContent = `${leader ? `leader ${leader.controller.address}` : 'no leader known'} · ${plural(controllers.length, 'peer', 'peers')}`;
      } else if (title.kind === 'producers') {
        title.heading.textContent = 'Producer groups · northern half';
        title.metric.textContent = `${formatCount(model.groups.producers)} of ${plural(model.groups.producersInScope, 'group', 'groups')}`;
      } else if (title.kind === 'consumers') {
        title.heading.textContent = 'Consumer groups · southern half';
        title.metric.textContent = `${formatCount(model.groups.consumers)} of ${plural(model.groups.consumersInScope, 'group', 'groups')}`;
      } else {
        const cluster = model.clusters.find((item) => item.clusterName === title.clusterName);
        if (!cluster) continue;
        const badge = document.createElement('span');
        badge.className = 'topology-sphere-badge';
        badge.dataset.tone = cluster.healthy ? cluster.mode : 'degraded';
        badge.textContent = cluster.healthy ? MODE_TEXT[cluster.mode] : 'Degraded';
        title.heading.replaceChildren(cluster.clusterName, badge);
        title.metric.textContent = `${plural(cluster.groups, 'group', 'groups')} · ${plural(cluster.instances, 'broker', 'brokers')}`;
      }
    }
  }

  /** Same nodes and links: refresh the texts and particle speeds without rebuilding the scene. */
  private updateReadings(model: SphereModel) {
    const content = this.content;
    if (!content) return;
    for (const node of model.nodes) {
      const view = content.nodes.get(node.id);
      if (!view) continue;
      view.node = node;
      if (view.title) view.title.textContent = nodeTitle(node);
      view.metric.textContent = nodeMetric(node);
      view.label.element.setAttribute('aria-label', nodeDescription(node));
    }
    const links = new Map(model.links.map((link) => [link.id, link]));
    for (const view of content.links) view.link = links.get(view.link.id) ?? view.link;
    for (const particle of content.particles) {
      if (particle.timing.kind !== 'flow') continue;
      const period = this.period(particle.link);
      // Keep each particle where it is while its speed changes.
      const time = this.clock.elapsed;
      particle.offset = (((time / particle.timing.period + particle.offset - time / period) % 1) + 1) % 1;
      particle.timing = { kind: 'flow', period };
    }
    this.writeTitles(model, content.titles);
    this.measureLabels();
  }

  /* ---------- Focus ---------- */

  private relatedTo(id: string | null) {
    const ids = new Set<string>();
    if (!id || !this.model) return ids;
    ids.add(id);
    for (const link of this.model.links) {
      if (link.from === id || link.to === id) {
        ids.add(link.from);
        ids.add(link.to);
      }
    }
    return ids;
  }

  private applyFocus() {
    const content = this.content;
    if (!content) return;
    const focus = this.selected ?? this.hovered;
    this.related = this.relatedTo(focus);
    for (const view of content.nodes.values()) {
      const dim = Boolean(focus) && !this.related.has(view.node.id);
      view.material.opacity = dim ? 0.22 : 1;
      view.halo.opacity = dim ? 0.08 : 0.55;
      for (const decoration of view.decorations) decoration.opacity = dim ? 0.15 : 0.85;
      view.label.element.setAttribute('aria-pressed', String(this.selected === view.node.id));
    }
    for (const view of content.links) {
      const on = !focus || view.link.from === focus || view.link.to === focus;
      view.material.opacity = focus ? (on ? Math.min(1, view.baseOpacity + 0.35) : 0.06) : view.baseOpacity;
    }
    for (const particle of content.particles) particle.dim = Boolean(focus) && particle.link.link.from !== focus && particle.link.link.to !== focus;
    const selected = this.selected ? content.nodes.get(this.selected) : undefined;
    content.selection.visible = Boolean(selected);
    if (selected) {
      content.layers[selected.node.sphere].add(content.selection);
      content.selection.position.copy(selected.local);
      content.selection.quaternion.setFromUnitVectors(Z_AXIS, selected.normal);
    }
  }

  private setHover(id: string | null, leaving?: string) {
    if (id === null && leaving && this.hovered !== leaving) return;
    if (this.hovered === id) return;
    this.hovered = id;
    this.applyFocus();
  }

  private flyToNode(id: string) {
    const view = this.content?.nodes.get(id);
    if (!view) return;
    const { node } = view;
    if (node.sphere === 'brokers') {
      this.flyTo(view.rig, -node.lon * DEG, FOCUS_TILT[node.kind] * DEG);
      return;
    }
    // Turn the small sphere so the node points straight at the viewer.
    const { normal } = view;
    this.flyTo(view.rig, -Math.atan2(normal.x, normal.z), Math.atan2(normal.y, Math.hypot(normal.x, normal.z)));
  }

  /** Folds a small sphere's current sway into its base angle, so a flight or a drag starts where it is. */
  private settle(rig: Rig) {
    if (rig.key === 'brokers') return;
    rig.view.spin += SWAY * Math.sin(rig.view.phase);
    rig.view.phase = 0;
  }

  private flyTo(rig: Rig, spin: number, tilt: number) {
    this.settle(rig);
    const { view } = rig;
    const delta = Math.atan2(Math.sin(spin - view.spin), Math.cos(spin - view.spin));
    view.flight = { fromSpin: view.spin, toSpin: view.spin + delta, fromTilt: view.tilt, toTilt: tilt, start: performance.now() };
  }

  /* ---------- Input ---------- */

  private pick(event: PointerEvent): { node: string | null; sphere: SphereKey | null } {
    const box = this.canvas.getBoundingClientRect();
    this.pointer.set(((event.clientX - box.left) / box.width) * 2 - 1, -((event.clientY - box.top) / box.height) * 2 + 1);
    this.raycaster.setFromCamera(this.pointer, this.camera);
    const spheres = Object.values(this.rigs)
      .filter((rig) => rig.group.visible)
      .map((rig) => rig.body);
    const targets: Object3D[] = [...spheres, ...(this.content?.hitTargets ?? [])];
    const first = this.raycaster.intersectObjects(targets, false)[0];
    if (!first) return { node: null, sphere: null };
    const nodeId = first.object.userData.nodeId as string | undefined;
    if (nodeId) return { node: nodeId, sphere: this.content?.nodes.get(nodeId)?.node.sphere ?? null };
    return { node: null, sphere: (first.object.userData.sphere as SphereKey | undefined) ?? null };
  }

  private readonly onPointerDown = (event: PointerEvent) => {
    if ((event.target as Element | null)?.closest('button')) return;
    const rig = this.rigs[this.pick(event).sphere ?? 'brokers'];
    this.settle(rig);
    this.drag = { x: event.clientX, y: event.clientY, rig, spin: rig.view.spin, tilt: rig.view.tilt, moved: false, pointerId: event.pointerId };
  };

  private readonly onPointerMove = (event: PointerEvent) => {
    const drag = this.drag;
    const { stage } = this.elements;
    if (drag) {
      const dx = event.clientX - drag.x;
      const dy = event.clientY - drag.y;
      if (!drag.moved && Math.abs(dx) + Math.abs(dy) > 5) {
        drag.moved = true;
        drag.rig.view.flight = null;
        stage.setPointerCapture?.(drag.pointerId);
        stage.dataset.dragging = 'true';
      }
      if (drag.moved) {
        const brokers = drag.rig.key === 'brokers';
        const perPixel = (Math.PI * 1.1) / (Math.max(stage.clientWidth, 1) * (brokers ? 1 : 0.35));
        drag.rig.view.spin = drag.spin + dx * perPixel;
        const limit = (brokers ? 40 : 80) * DEG;
        drag.rig.view.tilt = Math.min(limit, Math.max(brokers ? -35 * DEG : -limit, drag.tilt + dy * perPixel));
        return;
      }
    }
    if (!(event.target as Element | null)?.closest('button')) {
      const id = this.pick(event).node;
      stage.style.cursor = id ? 'pointer' : '';
      if (id !== this.hovered) this.setHover(id);
    }
  };

  private readonly onPointerUp = (event: PointerEvent) => {
    const drag = this.drag;
    this.drag = null;
    delete this.elements.stage.dataset.dragging;
    if (drag && !drag.moved && event.type === 'pointerup' && !(event.target as Element | null)?.closest('button')) {
      const id = this.pick(event).node;
      this.select(id && this.selected === id ? null : id);
    }
  };

  private readonly onPointerLeave = () => {
    if (this.drag && !this.drag.moved) this.drag = null;
    if (this.hovered && !(document.activeElement instanceof HTMLElement && this.elements.labels.contains(document.activeElement))) this.setHover(null);
  };

  private readonly onKeyDown = (event: KeyboardEvent) => {
    if (event.key === 'Escape' && this.selected && !event.defaultPrevented) this.select(null);
  };

  /* ---------- Frame ---------- */

  private resize() {
    const { stage } = this.elements;
    const width = stage.clientWidth;
    const height = stage.clientHeight;
    if (!width || !height) return;
    this.camera.aspect = width / height;
    this.spread(this.camera.aspect);
    // Frame the spheres that are drawn, so a missing one leaves no empty side.
    const bounds = this.viewBounds();
    const centreX = (bounds.left + bounds.right) / 2;
    const centreY = (bounds.bottom + bounds.top) / 2;
    const tangent = Math.tan((this.camera.fov / 2) * DEG);
    const distance = Math.max((bounds.top - bounds.bottom) / 2 / tangent, (bounds.right - bounds.left) / 2 / (tangent * this.camera.aspect));
    this.camera.position.set(centreX, centreY, distance);
    this.camera.lookAt(centreX, centreY, 0);
    this.camera.updateProjectionMatrix();
    this.camera.updateMatrixWorld();
    // The small spheres sit off-centre; turning their fronts to the camera keeps their nodes facing the viewer.
    this.rigs.nameservers.group.lookAt(this.camera.position);
    this.rigs.controllers.group.lookAt(this.camera.position);
    for (const material of this.lineMaterials) material.resolution.set(width, height);
    this.renderer.setSize(width, height, false);
    this.measureLabels();
  }

  /**
   * Places the small spheres for a stage of `aspect` (width / height): as far toward
   * `NAMESERVER_WIDE` and `CONTROLLER_WIDE` as it takes for the framed spheres to be as wide as the
   * stage. Beyond that the spheres stay centred with room on both sides.
   */
  private spread(aspect: number) {
    const moves = [
      { rig: this.rigs.nameservers, near: NAMESERVER_CENTRE, wide: NAMESERVER_WIDE, radius: NAMESERVER_RADIUS },
      { rig: this.rigs.controllers, near: CONTROLLER_CENTRE, wide: CONTROLLER_WIDE, radius: CONTROLLER_RADIUS }
    ];
    const place = (amount: number) => {
      for (const { rig, near, wide, radius } of moves) {
        const scale = 1 + (wide.scale - 1) * amount;
        rig.group.position.lerpVectors(near, wide.centre, amount);
        rig.group.scale.setScalar(scale);
        rig.radius = radius * scale;
      }
    };
    // The framed bounds widen steadily with the spread, so halving the interval finds the fit.
    let low = 0;
    let high = 1;
    for (let step = 0; step < 12; step += 1) {
      const amount = (low + high) / 2;
      place(amount);
      const bounds = this.viewBounds();
      if ((bounds.right - bounds.left) / (bounds.top - bounds.bottom) < aspect) low = amount;
      else high = amount;
    }
    place(low);
  }

  /** The part of the z = 0 plane the camera keeps in view: every drawn sphere with its margins. */
  private viewBounds() {
    const { radius } = this.rigs.brokers;
    const bounds = {
      left: -radius - MARGIN.brokerSide,
      right: radius + MARGIN.brokerSide,
      bottom: -radius - MARGIN.brokerTitle,
      top: radius + MARGIN.brokerTitle
    };
    // The NameServer title sits above its sphere, the Controller title below.
    const small = [
      { rig: this.rigs.nameservers, above: MARGIN.smallTitle, below: MARGIN.smallBelow },
      { rig: this.rigs.controllers, above: MARGIN.smallBelow, below: MARGIN.smallTitle }
    ];
    for (const { rig, above, below } of small) {
      if (!rig.group.visible) continue;
      bounds.left = Math.min(bounds.left, rig.centre.x - rig.radius - MARGIN.smallSide);
      bounds.right = Math.max(bounds.right, rig.centre.x + rig.radius + MARGIN.smallSide);
      bounds.bottom = Math.min(bounds.bottom, rig.centre.y - rig.radius - below);
      bounds.top = Math.max(bounds.top, rig.centre.y + rig.radius + above);
    }
    return bounds;
  }

  private measureLabels() {
    const content = this.content;
    if (content) {
      for (const view of content.nodes.values()) Object.assign(view.label, { width: view.label.element.offsetWidth, height: view.label.element.offsetHeight });
      for (const title of content.titles) Object.assign(title.label, { width: title.label.element.offsetWidth, height: title.label.element.offsetHeight });
    }
    const { controls } = this.elements;
    if (controls) {
      this.controlsBox = { x: controls.offsetLeft - 4, y: controls.offsetTop - 4, width: controls.offsetWidth + 8, height: controls.offsetHeight + 8, owner: null };
    }
  }

  private readonly frame = (now: number) => {
    const dt = this.clock.last ? Math.min(0.05, (now - this.clock.last) / 1000) : 0;
    this.clock.last = now;
    const moving = !this.paused && !this.reducedMotion;
    if (moving) this.clock.elapsed += dt;
    const auto = moving && !this.selected && !this.hovered && !this.drag;
    for (const rig of Object.values(this.rigs)) {
      const { view } = rig;
      if (view.flight) {
        const progress = this.reducedMotion ? 1 : Math.min(1, (now - view.flight.start) / FLIGHT_MS);
        const eased = progress < 0.5 ? 4 * progress ** 3 : 1 - (-2 * progress + 2) ** 3 / 2;
        view.spin = view.flight.fromSpin + (view.flight.toSpin - view.flight.fromSpin) * eased;
        view.tilt = view.flight.fromTilt + (view.flight.toTilt - view.flight.fromTilt) * eased;
        if (progress >= 1) view.flight = null;
      } else if (auto) {
        if (rig.key === 'brokers') view.spin += (dt * Math.PI * 2) / BROKER_TURN_SECONDS;
        else view.phase += (dt * Math.PI * 2) / SWAY_SECONDS[rig.key];
      }
      const sway = rig.key === 'brokers' ? 0 : SWAY * Math.sin(view.phase);
      rig.pivot.rotation.set(view.tilt, view.spin + sway, 0);
    }
    this.scene.updateMatrixWorld();
    const content = this.content;
    if (content) {
      for (const view of content.nodes.values()) view.world.copy(view.local).applyMatrix4(view.rig.pivot.matrixWorld);
      for (const view of content.links) if (view.bridge) this.writeBridge(view.bridge, view.link.offset);
      if (moving) this.flowDashes(content, dt);
      this.placeParticles(content);
      this.placePulses(content);
      if (this.elements.stage.clientWidth > 0) this.layoutLabels(content);
    }
    this.renderer.render(this.scene, this.camera);
  };

  private bridgeEnd(view: NodeView) {
    return { point: view.world, centre: view.rig.centre, radius: view.rig.radius };
  }

  private writeBridge(line: BridgeLine, offset: number) {
    line.bridge.update(this.bridgeEnd(line.from), this.bridgeEnd(line.to), offset);
    const segments = line.segments.array as Float32Array;
    const distances = line.distances.array as Float32Array;
    const start = this.lineStart;
    const end = this.lineEnd;
    let travelled = 0;
    line.bridge.pointAt(0, start);
    for (let step = 1; step <= BRIDGE_STEPS; step += 1) {
      line.bridge.pointAt(step / BRIDGE_STEPS, end);
      const at = (step - 1) * 6;
      segments[at] = start.x;
      segments[at + 1] = start.y;
      segments[at + 2] = start.z;
      segments[at + 3] = end.x;
      segments[at + 4] = end.y;
      segments[at + 5] = end.z;
      distances[(step - 1) * 2] = travelled;
      travelled += start.distanceTo(end);
      distances[(step - 1) * 2 + 1] = travelled;
      start.copy(end);
    }
    line.segments.needsUpdate = true;
    line.distances.needsUpdate = true;
  }

  private placeParticles(content: Content) {
    const time = this.clock.elapsed;
    for (const particle of content.particles) {
      const { timing } = particle;
      let t: number | null = null;
      if (!this.reducedMotion) {
        if (timing.kind === 'flow') t = (time / timing.period + particle.offset) % 1;
        else {
          const cycle = (((time - timing.delay) % timing.every) + timing.every) % timing.every;
          if (cycle <= timing.travel) t = cycle / timing.travel;
        }
      }
      const show = t !== null;
      particle.head.visible = show;
      particle.halo.visible = show && !particle.dim;
      for (const tail of particle.tails) tail.visible = false;
      if (t === null) continue;
      particle.link.pointAt(t, particle.head.position);
      particle.halo.position.copy(particle.head.position);
      particle.head.scale.setScalar(particle.dim ? 0.5 : 1);
      if (particle.dim) continue;
      particle.tails.forEach((tail, index) => {
        const back = (t as number) - (index + 1) * 0.034;
        if (back <= 0) return;
        tail.visible = true;
        particle.link.pointAt(back, tail.position);
      });
    }
  }

  private placePulses(content: Content) {
    const time = this.clock.elapsed;
    const phase = this.reducedMotion ? 0.3 : (time % 1.6) / 1.6;
    for (const view of content.nodes.values()) {
      if (!view.pulse) continue;
      view.pulse.scale.setScalar(1 + phase * 1.6);
      view.pulse.material.opacity = (1 - phase) * 0.7;
    }
    const since = time - this.clock.refreshAt;
    const flying = since >= 0 && since < ROUTE_WINDOW_S && !this.reducedMotion;
    for (const flight of content.flights) {
      const t = (since - flight.delay) / ROUTE_TRAVEL_S;
      const show = flying && t >= 0 && t <= 1;
      flight.head.visible = show;
      flight.halo.visible = show;
      if (!show) continue;
      flight.link.pointAt(t, flight.head.position);
      flight.halo.position.copy(flight.head.position);
    }
  }

  /** Dashes of the lines to and from the NameServers run in the direction of their link. */
  private flowDashes(content: Content, dt: number) {
    for (const view of content.links) {
      if (!view.bridge || view.link.unavailable || !(view.material instanceof LineMaterial)) continue;
      view.material.dashOffset -= dt * DASH_SPEED;
    }
  }

  /** Whether the sphere at `centre` hides `point` from the camera. */
  private occluded(point: Vector3, centre: Vector3, radius: number) {
    const ray = this.toCamera.copy(point).sub(this.camera.position);
    const distance = ray.length();
    ray.divideScalar(distance);
    const offset = this.fromCentre.copy(this.camera.position).sub(centre);
    const b = offset.dot(ray);
    const discriminant = b * b - (offset.lengthSq() - radius * radius);
    if (discriminant < 0) return false;
    const enter = -b - Math.sqrt(discriminant);
    return enter > 0 && enter < distance - 0.02 * radius;
  }

  private facing(point: Vector3, centre: Vector3) {
    this.toCamera.copy(this.camera.position).sub(point).normalize();
    return this.fromCentre.copy(point).sub(centre).normalize().dot(this.toCamera);
  }

  private project(point: Vector3, width: number, height: number) {
    this.scratch.copy(point).project(this.camera);
    return { x: ((this.scratch.x + 1) / 2) * width, y: ((1 - this.scratch.y) / 2) * height };
  }

  private disc(rig: Rig, width: number, height: number) {
    const centre = this.project(rig.centre, width, height);
    const top = this.project(this.fromCentre.copy(rig.centre).setY(rig.centre.y + rig.radius), width, height).y;
    const bottom = this.project(this.fromCentre.copy(rig.centre).setY(rig.centre.y - rig.radius), width, height).y;
    return { x: centre.x, y: centre.y, top, bottom };
  }

  private layoutLabels(content: Content) {
    const { stage } = this.elements;
    const width = stage.clientWidth;
    const height = stage.clientHeight;
    const focus = this.selected ?? this.hovered;
    const brokers = this.disc(this.rigs.brokers, width, height);
    const servers = this.disc(this.rigs.nameservers, width, height);
    const controllers = this.disc(this.rigs.controllers, width, height);
    // A small sphere keeps its disc clear of other labels; its own labels may overlap it.
    const keepClear = (disc: { x: number; y: number; top: number; bottom: number }, owner: SphereKey): LabelObstacle => {
      const reach = (disc.bottom - disc.top) / 2 + 6;
      return { x: disc.x - reach, y: disc.y - reach, width: reach * 2, height: reach * 2, owner };
    };
    const obstacles: LabelObstacle[] = [this.controlsBox];
    if (content.nameServers) obstacles.push(keepClear(servers, 'nameservers'));
    if (content.controllers) obstacles.push(keepClear(controllers, 'controllers'));
    const requests: LabelRequest[] = [];
    const fades = new Map<string, number>();
    const views = new Map<string, LabelView>();
    const request = (id: string, label: LabelView, priority: number, x: number, y: number, placement: LabelPlacement, centreX: number, owner: string | null, fade: number) => {
      requests.push({ id, priority, x, y, width: label.width, height: label.height, placement, centreX, owner });
      fades.set(id, fade);
      views.set(id, label);
    };

    for (const title of content.titles) {
      const id = `title:${title.kind}:${title.clusterName ?? ''}`;
      const dimmed = focus ? 0.55 : 1;
      if (title.kind === 'cluster' && title.anchor) {
        const anchor = this.anchor.copy(title.anchor).applyMatrix4(this.rigs.brokers.pivot.matrixWorld);
        const facing = this.facing(anchor, this.rigs.brokers.centre);
        if (facing < 0.3) {
          this.hide(title.label);
          continue;
        }
        const position = this.project(anchor, width, height);
        request(id, title.label, 95, position.x, position.y, 'centre', brokers.x, null, Math.min(1, (facing - 0.3) / 0.25) * dimmed);
      } else if (title.kind === 'nameservers') {
        request(id, title.label, 95, servers.x, servers.top, 'above', servers.x, 'nameservers', dimmed);
      } else if (title.kind === 'controllers') {
        request(id, title.label, 95, controllers.x, controllers.bottom, 'below', controllers.x, 'controllers', dimmed);
      } else {
        request(id, title.label, 95, brokers.x, title.kind === 'producers' ? brokers.top : brokers.bottom, title.kind === 'producers' ? 'above' : 'below', brokers.x, null, dimmed);
      }
    }
    for (const view of content.nodes.values()) {
      const { rig } = view;
      const facing = this.facing(view.world, rig.centre);
      const inFocus = Boolean(focus) && this.related.has(view.node.id);
      let fade = Math.min(1, (facing - 0.18) / 0.22);
      if (view.priority >= 70) fade = Math.max(fade, 0.7);
      if (facing < 0.18 || fade < 0.3 || this.occluded(view.world, rig.centre, rig.radius)) {
        this.hide(view.label);
        continue;
      }
      let priority = view.priority + (inFocus ? 200 : 0);
      if (view.node.id === this.hovered) priority = 900;
      if (view.node.id === this.selected) priority = 1000;
      const position = this.project(view.world, width, height);
      const centreX = { brokers: brokers.x, nameservers: servers.x, controllers: controllers.x }[rig.key];
      request(
        view.node.id,
        view.label,
        priority,
        position.x,
        position.y,
        view.node.kind === 'slave' ? 'near' : 'side',
        centreX,
        rig.key === 'brokers' ? null : rig.key,
        fade * (focus && !inFocus ? 0.35 : 1)
      );
    }

    const placed = new Map(placeLabels(requests, obstacles, { width, height }).map((label) => [label.id, label]));
    for (const [id, label] of views) {
      const position = placed.get(id);
      if (!position) this.hide(label);
      else this.show(label, position.x, position.y, fades.get(id) ?? 1);
    }
  }

  private hide(label: LabelView) {
    if (label.shown === false) return;
    label.element.style.visibility = 'hidden';
    label.shown = false;
  }

  private show(label: LabelView, x: number, y: number, opacity: number) {
    const transform = `translate(${x.toFixed(1)}px, ${y.toFixed(1)}px)`;
    const value = String(Math.round(opacity * 100) / 100);
    if (label.shown !== true) {
      label.element.style.visibility = 'visible';
      label.shown = true;
    }
    if (label.transform !== transform) {
      label.element.style.transform = transform;
      label.transform = transform;
    }
    if (label.opacity !== value) {
      label.element.style.opacity = value;
      label.opacity = value;
    }
  }
}
