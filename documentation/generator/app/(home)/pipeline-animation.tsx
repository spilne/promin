"use client";

import { useEffect, useRef } from "react";

// ---------------------------------------------------------------------------
// DAG node layout
// ---------------------------------------------------------------------------

type IconDrawFn = (ctx: CanvasRenderingContext2D, x: number, y: number, s: number, color: string) => void;

interface Node {
  id: string;
  label: string;
  x: number;
  y: number;
  color: string;
  icon: IconDrawFn;
}

interface Edge {
  from: string;
  to: string;
}

// ---------------------------------------------------------------------------
// Vector icons — drawn on canvas, no emoji
// ---------------------------------------------------------------------------

const iconBolt: IconDrawFn = (ctx, x, y, s, color) => {
  ctx.beginPath();
  ctx.moveTo(x + 2 * s, y - 6 * s);
  ctx.lineTo(x - 1 * s, y + 1 * s);
  ctx.lineTo(x + 1 * s, y + 1 * s);
  ctx.lineTo(x - 2 * s, y + 6 * s);
  ctx.lineTo(x + 1 * s, y - 1 * s);
  ctx.lineTo(x - 1 * s, y - 1 * s);
  ctx.closePath();
  ctx.fillStyle = color;
  ctx.fill();
};

const iconEnvelope: IconDrawFn = (ctx, x, y, s, color) => {
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.2 * s;
  ctx.beginPath();
  ctx.roundRect(x - 5 * s, y - 3.5 * s, 10 * s, 7 * s, 1 * s);
  ctx.stroke();
  ctx.beginPath();
  ctx.moveTo(x - 5 * s, y - 3.5 * s);
  ctx.lineTo(x, y + 1 * s);
  ctx.lineTo(x + 5 * s, y - 3.5 * s);
  ctx.stroke();
};

const iconCylinder: IconDrawFn = (ctx, x, y, s, color) => {
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.2 * s;
  ctx.beginPath();
  ctx.ellipse(x, y - 4 * s, 5 * s, 2 * s, 0, 0, Math.PI * 2);
  ctx.stroke();
  ctx.beginPath();
  ctx.moveTo(x - 5 * s, y - 4 * s);
  ctx.lineTo(x - 5 * s, y + 3 * s);
  ctx.stroke();
  ctx.beginPath();
  ctx.moveTo(x + 5 * s, y - 4 * s);
  ctx.lineTo(x + 5 * s, y + 3 * s);
  ctx.stroke();
  ctx.beginPath();
  ctx.ellipse(x, y + 3 * s, 5 * s, 2 * s, 0, 0, Math.PI);
  ctx.stroke();
};

const iconArrowsRight: IconDrawFn = (ctx, x, y, s, color) => {
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5 * s;
  ctx.lineCap = "round";
  for (const dy of [-3, 0, 3]) {
    ctx.beginPath();
    ctx.moveTo(x - 5 * s, (y + dy * s));
    ctx.lineTo(x + 3 * s, (y + dy * s));
    ctx.lineTo(x + 1 * s, (y + dy * s) - 2 * s);
    ctx.moveTo(x + 3 * s, (y + dy * s));
    ctx.lineTo(x + 1 * s, (y + dy * s) + 2 * s);
    ctx.stroke();
  }
};

const iconLoop: IconDrawFn = (ctx, x, y, s, color) => {
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5 * s;
  ctx.lineCap = "round";
  ctx.beginPath();
  ctx.arc(x, y, 5 * s, -0.5, Math.PI * 1.7);
  ctx.stroke();
  // Arrowhead
  const ax = x + 5 * s * Math.cos(-0.5);
  const ay = y + 5 * s * Math.sin(-0.5);
  ctx.beginPath();
  ctx.moveTo(ax + 2 * s, ay - 1 * s);
  ctx.lineTo(ax, ay);
  ctx.lineTo(ax + 2 * s, ay + 2 * s);
  ctx.stroke();
};

const iconGrid: IconDrawFn = (ctx, x, y, s, color) => {
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.2 * s;
  const g = 3.5 * s;
  for (let r = -1; r <= 1; r++) {
    for (let c = -1; c <= 1; c++) {
      ctx.strokeRect(x + c * g - g / 2, y + r * g - g / 2, g, g);
    }
  }
};

const iconDiamond: IconDrawFn = (ctx, x, y, s, color) => {
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5 * s;
  ctx.beginPath();
  ctx.moveTo(x, y - 6 * s);
  ctx.lineTo(x + 5 * s, y);
  ctx.lineTo(x, y + 6 * s);
  ctx.lineTo(x - 5 * s, y);
  ctx.closePath();
  ctx.stroke();
  ctx.beginPath();
  ctx.moveTo(x - 3 * s, y - 2 * s);
  ctx.lineTo(x + 3 * s, y - 2 * s);
  ctx.stroke();
};

const iconDisc: IconDrawFn = (ctx, x, y, s, color) => {
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.2 * s;
  ctx.beginPath();
  ctx.arc(x, y, 5 * s, 0, Math.PI * 2);
  ctx.stroke();
  ctx.beginPath();
  ctx.arc(x, y, 2 * s, 0, Math.PI * 2);
  ctx.stroke();
  ctx.beginPath();
  ctx.moveTo(x - 5 * s, y);
  ctx.lineTo(x - 2 * s, y);
  ctx.moveTo(x + 2 * s, y);
  ctx.lineTo(x + 5 * s, y);
  ctx.stroke();
};

const iconWave: IconDrawFn = (ctx, x, y, s, color) => {
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5 * s;
  ctx.lineCap = "round";
  for (const dy of [-3, 0, 3]) {
    ctx.beginPath();
    ctx.moveTo(x - 6 * s, y + dy * s);
    ctx.bezierCurveTo(
      x - 3 * s, y + (dy - 2) * s,
      x, y + (dy + 2) * s,
      x + 3 * s, y + dy * s,
    );
    ctx.lineTo(x + 6 * s, y + dy * s);
    ctx.stroke();
  }
};

const iconStateMachine: IconDrawFn = (ctx, x, y, s, color) => {
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.3 * s;
  // Three connected states (circles with arrows)
  const r = 2.5 * s;
  ctx.beginPath();
  ctx.arc(x - 4 * s, y - 3 * s, r, 0, Math.PI * 2);
  ctx.stroke();
  ctx.beginPath();
  ctx.arc(x + 4 * s, y - 3 * s, r, 0, Math.PI * 2);
  ctx.stroke();
  ctx.beginPath();
  ctx.arc(x, y + 4 * s, r, 0, Math.PI * 2);
  ctx.stroke();
  // Arrows between
  ctx.beginPath();
  ctx.moveTo(x - 1.5 * s, y - 3 * s);
  ctx.lineTo(x + 1.5 * s, y - 3 * s);
  ctx.stroke();
  ctx.beginPath();
  ctx.moveTo(x + 2.5 * s, y - 0.5 * s);
  ctx.lineTo(x + 1.5 * s, y + 1.5 * s);
  ctx.stroke();
  ctx.beginPath();
  ctx.moveTo(x - 1.5 * s, y + 1.5 * s);
  ctx.lineTo(x - 2.5 * s, y - 0.5 * s);
  ctx.stroke();
};

const NODES: Node[] = [
  // Sources (left column)
  { id: "src1", label: "API", x: 70, y: 100, color: "#6366f1", icon: iconBolt },
  { id: "src2", label: "Kafka", x: 70, y: 250, color: "#8b5cf6", icon: iconEnvelope },
  { id: "src3", label: "DB", x: 70, y: 400, color: "#a78bfa", icon: iconCylinder },
  // Processing (middle column)
  { id: "pipe", label: "Pipeline", x: 280, y: 100, color: "#3b82f6", icon: iconArrowsRight },
  { id: "wf", label: "Workflow", x: 280, y: 250, color: "#0ea5e9", icon: iconLoop },
  { id: "sm", label: "State Machine", x: 280, y: 400, color: "#14b8a6", icon: iconStateMachine },
  // Analytics (center-right)
  { id: "df", label: "DataFrame", x: 500, y: 250, color: "#06b6d4", icon: iconGrid },
  // Outputs (right column)
  { id: "out1", label: "Redis", x: 720, y: 100, color: "#f43f5e", icon: iconDiamond },
  { id: "out2", label: "Postgres", x: 720, y: 250, color: "#10b981", icon: iconDisc },
  { id: "out3", label: "Stream", x: 720, y: 400, color: "#f59e0b", icon: iconWave },
];

const EDGES: Edge[] = [
  { from: "src1", to: "pipe" },
  { from: "src2", to: "pipe" },
  { from: "src2", to: "wf" },
  { from: "src3", to: "wf" },
  { from: "src3", to: "sm" },
  { from: "pipe", to: "df" },
  { from: "wf", to: "df" },
  { from: "sm", to: "out2" },
  { from: "pipe", to: "out1" },
  { from: "df", to: "out2" },
  { from: "df", to: "out3" },
  { from: "wf", to: "out2" },
];

// ---------------------------------------------------------------------------
// Particle system
// ---------------------------------------------------------------------------

interface Particle {
  edgeIdx: number;
  t: number; // 0..1 along edge
  speed: number;
  size: number;
  hue: number;
}

function getNode(id: string): Node {
  return NODES.find((n) => n.id === id)!;
}

function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t;
}

// Curved path control point (slight arc)
function bezierPoint(
  x0: number,
  y0: number,
  x1: number,
  y1: number,
  t: number,
): [number, number] {
  const cx = (x0 + x1) / 2;
  const cy = (y0 + y1) / 2 - 30;
  const u = 1 - t;
  const x = u * u * x0 + 2 * u * t * cx + t * t * x1;
  const y = u * u * y0 + 2 * u * t * cy + t * t * y1;
  return [x, y];
}

// ---------------------------------------------------------------------------
// Canvas animation component
// ---------------------------------------------------------------------------

export function PipelineAnimation() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const particlesRef = useRef<Particle[]>([]);
  const frameRef = useRef(0);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d")!;
    const dpr = window.devicePixelRatio || 1;

    function resize() {
      const rect = canvas!.getBoundingClientRect();
      canvas!.width = rect.width * dpr;
      canvas!.height = rect.height * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }
    resize();
    window.addEventListener("resize", resize);

    // Spawn initial particles
    for (let i = 0; i < 25; i++) {
      particlesRef.current.push({
        edgeIdx: Math.floor(Math.random() * EDGES.length),
        t: Math.random(),
        speed: 0.003 + Math.random() * 0.004,
        size: 2 + Math.random() * 3,
        hue: 200 + Math.random() * 60,
      });
    }

    let animId: number;
    const isDark = () =>
      window.matchMedia("(prefers-color-scheme: dark)").matches ||
      document.documentElement.classList.contains("dark");

    function draw() {
      const w = canvas!.getBoundingClientRect().width;
      const h = canvas!.getBoundingClientRect().height;
      const scaleX = w / 800;
      const scaleY = h / 500;
      const dark = isDark();

      ctx.clearRect(0, 0, w, h);

      // Draw edges
      for (const edge of EDGES) {
        const from = getNode(edge.from);
        const to = getNode(edge.to);
        const x0 = from.x * scaleX;
        const y0 = from.y * scaleY;
        const x1 = to.x * scaleX;
        const y1 = to.y * scaleY;
        const cx = (x0 + x1) / 2;
        const cy = (y0 + y1) / 2 - 30 * scaleY;

        ctx.beginPath();
        ctx.moveTo(x0, y0);
        ctx.quadraticCurveTo(cx, cy, x1, y1);
        ctx.strokeStyle = dark
          ? "rgba(100, 120, 180, 0.15)"
          : "rgba(100, 120, 180, 0.12)";
        ctx.lineWidth = 1.5;
        ctx.stroke();
      }

      // Draw particles
      const particles = particlesRef.current;
      for (const p of particles) {
        const edge = EDGES[p.edgeIdx]!;
        const from = getNode(edge.from);
        const to = getNode(edge.to);
        const [px, py] = bezierPoint(
          from.x * scaleX,
          from.y * scaleY,
          to.x * scaleX,
          to.y * scaleY,
          p.t,
        );

        // Glow
        const gradient = ctx.createRadialGradient(px, py, 0, px, py, p.size * 4);
        gradient.addColorStop(
          0,
          `hsla(${p.hue}, 80%, ${dark ? 70 : 55}%, 0.6)`,
        );
        gradient.addColorStop(1, `hsla(${p.hue}, 80%, 60%, 0)`);
        ctx.beginPath();
        ctx.arc(px, py, p.size * 4, 0, Math.PI * 2);
        ctx.fillStyle = gradient;
        ctx.fill();

        // Core
        ctx.beginPath();
        ctx.arc(px, py, p.size, 0, Math.PI * 2);
        ctx.fillStyle = `hsla(${p.hue}, 85%, ${dark ? 75 : 55}%, 0.9)`;
        ctx.fill();

        // Advance
        p.t += p.speed;
        if (p.t >= 1) {
          p.t = 0;
          p.edgeIdx = Math.floor(Math.random() * EDGES.length);
          p.speed = 0.003 + Math.random() * 0.004;
          p.hue = 200 + Math.random() * 60;
        }
      }

      // Draw nodes
      for (const node of NODES) {
        const nx = node.x * scaleX;
        const ny = node.y * scaleY;
        const r = 28 * Math.min(scaleX, scaleY);

        // Node circle
        ctx.beginPath();
        ctx.arc(nx, ny, r, 0, Math.PI * 2);
        ctx.fillStyle = dark
          ? "rgba(20, 24, 40, 0.85)"
          : "rgba(255, 255, 255, 0.9)";
        ctx.fill();
        ctx.strokeStyle = node.color + (dark ? "80" : "60");
        ctx.lineWidth = 2;
        ctx.stroke();

        // Subtle pulse glow on node
        const pulse = 0.3 + 0.15 * Math.sin(frameRef.current * 0.02 + NODES.indexOf(node));
        ctx.beginPath();
        ctx.arc(nx, ny, r + 4, 0, Math.PI * 2);
        ctx.strokeStyle = node.color + Math.round(pulse * 255).toString(16).padStart(2, "0");
        ctx.lineWidth = 1;
        ctx.stroke();

        // Icon (centered in circle)
        const iconScale = Math.min(scaleX, scaleY) * 2;
        ctx.save();
        node.icon(ctx, nx, ny, iconScale, node.color);
        ctx.restore();

        // Label (below circle)
        ctx.font = `500 ${10 * Math.min(scaleX, scaleY)}px system-ui, sans-serif`;
        ctx.textAlign = "center";
        ctx.textBaseline = "top";
        ctx.fillStyle = dark
          ? "rgba(200, 210, 230, 0.8)"
          : "rgba(40, 50, 80, 0.7)";
        ctx.fillText(node.label, nx, ny + r + 6 * Math.min(scaleX, scaleY));
      }

      frameRef.current++;
      animId = requestAnimationFrame(draw);
    }

    draw();

    return () => {
      cancelAnimationFrame(animId);
      window.removeEventListener("resize", resize);
    };
  }, []);

  return (
    <canvas
      ref={canvasRef}
      style={{
        width: "100%",
        maxWidth: "800px",
        height: "500px",
        borderRadius: "1rem",
      }}
    />
  );
}
