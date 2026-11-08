import { Curve, Vector3 } from 'three';

const Y_AXIS = new Vector3(0, 1, 0);
const X_AXIS = new Vector3(1, 0, 0);
const Z_AXIS = new Vector3(0, 0, 1);
const turnAxis = new Vector3();
const scratch = new Vector3();

/** Great-circle interpolation between unit vectors; nearly opposite ends turn around a stable axis. */
export function slerpUnit(a: Vector3, b: Vector3, angle: number, t: number, target: Vector3): Vector3 {
  if (angle < 1e-4) return target.copy(a);
  const sine = Math.sin(angle);
  if (sine < 1e-3) {
    turnAxis.crossVectors(a, Math.abs(a.y) < 0.9 ? Y_AXIS : X_AXIS).normalize();
    return target.copy(a).applyAxisAngle(turnAxis, angle * t);
  }
  const k0 = Math.sin((1 - t) * angle) / sine;
  const k1 = Math.sin(t * angle) / sine;
  return target.copy(a).multiplyScalar(k0).addScaledVector(b, k1);
}

export class FunctionCurve extends Curve<Vector3> {
  private readonly fn: (t: number, target: Vector3) => Vector3;

  constructor(fn: (t: number, target: Vector3) => Vector3) {
    super();
    this.fn = fn;
  }

  override getPoint(t: number, target = new Vector3()): Vector3 {
    return this.fn(t, target);
  }
}

/**
 * A link between two points of one sphere: the great circle between them, lifted off the surface
 * the more the further apart they are.
 */
export function surfaceCurve(a: Vector3, b: Vector3, lift?: number): FunctionCurve {
  const ra = a.length();
  const rb = b.length();
  const ua = a.clone().normalize();
  const ub = b.clone().normalize();
  const angle = ua.angleTo(ub);
  const rise = lift ?? ((ra + rb) / 2) * (0.05 + 0.16 * (angle / Math.PI));
  return new FunctionCurve((t, target) => slerpUnit(ua, ub, angle, t, target).multiplyScalar(ra + (rb - ra) * t + rise * Math.sin(Math.PI * t)));
}

/** Distance from a sphere's centre to its port: close above the surface, so paths around it stay near it. */
function portDistance(radius: number) {
  return radius + Math.max(0.12, radius * 0.12);
}

/** One end of a bridge: a point on a sphere, in world coordinates. */
export interface BridgeEnd {
  point: Vector3;
  centre: Vector3;
  radius: number;
}

/**
 * Path between points on two different spheres. It climbs around the first sphere to a port facing
 * the second, crosses the gap, then wraps around the second sphere to the target. Each leg keeps its
 * distance from its own sphere's centre, so the path never cuts through a sphere. `offset` moves the
 * ports sideways, which fans the paths of one bundle apart.
 */
export class Bridge {
  private readonly side = new Vector3();
  private readonly portA = new Vector3();
  private readonly portB = new Vector3();
  private readonly ua = new Vector3();
  private readonly upa = new Vector3();
  private readonly upb = new Vector3();
  private readonly ub = new Vector3();
  private readonly centreA = new Vector3();
  private readonly centreB = new Vector3();
  private ra = 1;
  private rpa = 1;
  private rpb = 1;
  private rb = 1;
  private angleA = 0;
  private angleB = 0;
  private bulgeA = 0;
  private bulgeB = 0;
  private bow = 0;
  private k1 = 0.4;
  private k2 = 0.6;

  update(from: BridgeEnd, to: BridgeEnd, offset: number): this {
    this.centreA.copy(from.centre);
    this.centreB.copy(to.centre);
    const direction = scratch.copy(to.centre).sub(from.centre).normalize();
    this.side.crossVectors(direction, Z_AXIS).normalize();
    this.portA.copy(from.centre).addScaledVector(direction, portDistance(from.radius)).addScaledVector(this.side, offset);
    this.portB.copy(to.centre).addScaledVector(direction, -portDistance(to.radius)).addScaledVector(this.side, offset);
    this.ra = this.ua.copy(from.point).sub(from.centre).length();
    this.ua.divideScalar(this.ra || 1);
    this.rpa = this.upa.copy(this.portA).sub(from.centre).length();
    this.upa.divideScalar(this.rpa);
    this.rpb = this.upb.copy(this.portB).sub(to.centre).length();
    this.upb.divideScalar(this.rpb);
    this.rb = this.ub.copy(to.point).sub(to.centre).length();
    this.ub.divideScalar(this.rb || 1);
    this.angleA = this.ua.angleTo(this.upa);
    this.angleB = this.upb.angleTo(this.ub);
    this.bulgeA = from.radius * (0.02 + 0.1 * (this.angleA / Math.PI));
    this.bulgeB = to.radius * (0.02 + 0.1 * (this.angleB / Math.PI));
    const legA = this.angleA * 0.75 * ((this.ra + this.rpa) / 2) + Math.abs(this.rpa - this.ra) + this.bulgeA;
    const gap = this.portA.distanceTo(this.portB);
    const legB = this.angleB * 0.75 * ((this.rpb + this.rb) / 2) + Math.abs(this.rb - this.rpb) + this.bulgeB;
    const total = legA + gap + legB || 1;
    this.k1 = legA / total;
    this.k2 = (legA + gap) / total;
    this.bow = Math.min(0.1, gap * 0.12);
    return this;
  }

  pointAt(t: number, target: Vector3): Vector3 {
    if (t <= this.k1) {
      // Around the first sphere; the last quarter rises straight out to the port.
      const s = this.k1 > 0 ? t / this.k1 : 1;
      const turn = Math.min(1, s / 0.75);
      slerpUnit(this.ua, this.upa, this.angleA, turn, target);
      return target.multiplyScalar(this.ra + (this.rpa - this.ra) * s + this.bulgeA * Math.sin(Math.PI * turn)).add(this.centreA);
    }
    if (t <= this.k2) {
      const s = (t - this.k1) / (this.k2 - this.k1);
      return target.copy(this.portA).lerp(this.portB, s).addScaledVector(this.side, this.bow * Math.sin(Math.PI * s));
    }
    // Around the second sphere; the first quarter drops straight in from the port.
    const s = (t - this.k2) / (1 - this.k2);
    const turn = Math.max(0, (s - 0.25) / 0.75);
    slerpUnit(this.upb, this.ub, this.angleB, turn, target);
    return target.multiplyScalar(this.rpb + (this.rb - this.rpb) * s + this.bulgeB * Math.sin(Math.PI * turn)).add(this.centreB);
  }
}
