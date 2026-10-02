// Tiny dependency-free SVG charts for the admin console.
import type { ReactNode } from 'react'

export function Spark({ data, color = 'var(--accent)', height = 44, max, min = 0 }: {
  data: number[]; color?: string; height?: number; max?: number; min?: number
}) {
  const w = 240
  const hi = max ?? Math.max(1, ...data)
  const span = Math.max(hi - min, 1e-9)
  const pts = data.map((v, i) => [(i / Math.max(data.length - 1, 1)) * w, height - 3 - ((v - min) / span) * (height - 6)])
  const line = pts.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(' ')
  return (
    <svg className="spark" viewBox={`0 0 ${w} ${height}`} preserveAspectRatio="none" role="img" aria-hidden>
      {pts.length > 1 && <polygon points={`0,${height} ${line} ${w},${height}`} fill={color} opacity={0.15} />}
      {pts.length > 1 && <polyline points={line} fill="none" stroke={color} strokeWidth={1.8} vectorEffect="non-scaling-stroke" />}
    </svg>
  )
}

export type Row = { t: number; total: number; [group: string]: number }

export function StackedTraffic({ rows, groups }: { rows: Row[]; groups: { key: string; color: string; label: string }[] }) {
  const w = 600, h = 150
  const max = Math.max(10, ...rows.map((r) => groups.reduce((a, g) => a + (r[g.key] || 0), 0)))
  const bw = w / Math.max(rows.length, 1)
  return (
    <div>
      <svg className="traffic" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" role="img" aria-label="Requests per second by type">
        {[0.25, 0.5, 0.75].map((f) => <line key={f} x1={0} x2={w} y1={h * f} y2={h * f} stroke="var(--border)" strokeDasharray="3 4" />)}
        {rows.map((r, i) => {
          let y = h
          return groups.map((g) => {
            const v = r[g.key] || 0
            const bh = (v / max) * (h - 4)
            y -= bh
            return v > 0 ? <rect key={`${i}-${g.key}`} x={i * bw + 0.5} y={y} width={Math.max(bw - 1, 1)} height={bh} fill={g.color} /> : null
          })
        })}
      </svg>
      <div className="axis"><span>60s ago</span><span>peak {max.toLocaleString()} req/s</span><span>now</span></div>
      <div className="legend-row">{groups.map((g) => <span key={g.key}><i style={{ background: g.color }} />{g.label}</span>)}</div>
    </div>
  )
}

export function Ring({ value, total, children }: { value: number; total: number; children?: ReactNode }) {
  const r = 38, c = 2 * Math.PI * r, f = total ? Math.min(1, value / total) : 0
  return (
    <div className="ring">
      <svg viewBox="0 0 100 100" aria-hidden>
        <circle cx="50" cy="50" r={r} fill="none" stroke="var(--border)" strokeWidth="9" />
        <circle cx="50" cy="50" r={r} fill="none" stroke="var(--sold)" strokeWidth="9" strokeLinecap="round"
          strokeDasharray={`${c * f} ${c}`} transform="rotate(-90 50 50)" style={{ transition: 'stroke-dasharray .8s ease' }} />
      </svg>
      <div className="ring-label">{children}</div>
    </div>
  )
}
