"use client";

import { useEffect, useRef } from "react";

// ---------------------------------------------------------------------------
// DAG node layout
// ---------------------------------------------------------------------------

interface Node {
  id: string;
  label: string;
  x: number;
  y: number;
  color: string;
  icon: string;
}

interface Edge {
  from: string;
  to: string;
}

const NODES: Node[] = [
  { id: "src1", label: "API", x: 60, y: 80, color: "#6366f1", icon: "⚡" },
  { id: "src2", label: "Kafka", x: 60, y: 200, color: "#8b5cf6", icon: "📨" },
  { id: "src3", label: "DB", x: 60, y: 320, color: "#a78bfa", icon: "🗄" },
  { id: "pipe", label: "Pipeline", x: 280, y: 140, color: "#3b82f6", icon: "⛓" },
  { id: "wf", label: "Workflow", x: 280, y: 280, color: "#0ea5e9", icon: "🔄" },
  { id: "df", label: "DataFrame", x: 500, y: 200, color: "#06b6d4", icon: "📊" },
  { id: "out1", label: "Redis", x: 720, y: 100, color: "#f43f5e", icon: "⚙" },
  { id: "out2", label: "Postgres", x: 720, y: 240, color: "#10b981", icon: "💾" },
  { id: "out3", label: "Stream", x: 720, y: 360, color: "#f59e0b", icon: "📡" },
];

const EDGES: Edge[] = [
  { from: "src1", to: "pipe" },
  { from: "src2", to: "pipe" },
  { from: "src2", to: "wf" },
  { from: "src3", to: "wf" },
  { from: "pipe", to: "df" },
  { from: "wf", to: "df" },
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
      const scaleY = h / 440;
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

        // Icon
        ctx.font = `${14 * Math.min(scaleX, scaleY)}px sans-serif`;
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(node.icon, nx, ny - 4 * scaleY);

        // Label
        ctx.font = `500 ${10 * Math.min(scaleX, scaleY)}px system-ui, sans-serif`;
        ctx.fillStyle = dark
          ? "rgba(200, 210, 230, 0.8)"
          : "rgba(40, 50, 80, 0.7)";
        ctx.fillText(node.label, nx, ny + 14 * scaleY);
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
        height: "440px",
        borderRadius: "1rem",
      }}
    />
  );
}
