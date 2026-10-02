import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Ring, Spark, StackedTraffic, type Row } from './charts'
import './admin.css'

type Ev = { t: number; kind: string; msg: string }
type Snap = {
  ts: number; chaos_until: number | null
  queued: number | null; active: number | null; opened_at: number | null
  counters: Record<string, number>; events: Ev[]; sim: Record<string, string>
  redis: { up: boolean; error?: string; rtt_ms?: number; ops_per_sec?: number; clients?: number; blocked?: number; memory_mb?: number; hit_ratio?: number | null; keys?: number; total_commands?: number }
  pg: { up: boolean; connections?: number; active?: number; tps?: number; sales?: number; oversold?: number }
  seats: { states: string; sold: number; held: number; available: number; total: number }
  rps: Row[]
  latency: { p50: number; p99: number; ewma: number; n: number }
  api: { loop_lag_ms: number; pid: number }
  gate: { batch: number; max_active: number; adaptive: boolean; ewma_ms: number; target_ms: number }
}
type Hist = Record<'ops' | 'rtt' | 'p99' | 'lag' | 'queued' | 'sold' | 'batch' | 'throttle' | 'tps', number[]>
const KEEP = 90
const GROUPS = [
  { key: 'booking', color: '#5b9cff', label: 'booking' },
  { key: 'queue', color: '#3ddc97', label: 'queue join/poll' },
  { key: 'sse', color: '#a78bfa', label: 'SSE streams' },
  { key: 'other', color: '#7f8ea8', label: 'other' },
  { key: 'degraded', color: '#ff5d6c', label: '503 (Redis down)' },
]
const n = (v: number | null | undefined) => (v == null ? '—' : v.toLocaleString())

function Panel({ title, status, children }: { title: string; status?: 'ok' | 'bad' | 'warn'; children: React.ReactNode }) {
  return (
    <section className="panel">
      <header><h2>{title}</h2>{status && <span className={`dot ${status}`} />}</header>
      {children}
    </section>
  )
}

function Stat({ label, value, unit, tone }: { label: string; value: React.ReactNode; unit?: string; tone?: string }) {
  return <div className="stat"><span>{label}</span><b className={tone}>{value}<small>{unit}</small></b></div>
}

function SeatGrid({ states }: { states: string }) {
  const prev = useRef('')
  const ver = useRef<number[]>([])
  if (states !== prev.current) {
    for (let i = 0; i < states.length; i++) if (prev.current && states[i] !== prev.current[i]) ver.current[i] = (ver.current[i] || 0) + 1
    prev.current = states
  }
  return (
    <div className="heat">
      {[...states].map((c, i) => <i key={`${i}:${ver.current[i] || 0}`} className={`cell ${c} ${ver.current[i] ? 'flash' : ''}`} title={`seat ${i}`} />)}
    </div>
  )
}

export default function Admin() {
  const [key, setKey] = useState(() => sessionStorage.getItem('adminKey') ?? 'dev-admin')
  const [snap, setSnap] = useState<Snap | null>(null)
  const [conn, setConn] = useState<'connecting' | 'live' | 'lost'>('connecting')
  const [hist, setHist] = useState<Hist>({ ops: [], rtt: [], p99: [], lag: [], queued: [], sold: [], batch: [], throttle: [], tps: [] })
  const [humans, setHumans] = useState(2000)
  const [bots, setBots] = useState(300)
  const [msg, setMsg] = useState<string | null>(null)
  const prev = useRef<Snap | null>(null)
  const soldTrail = useRef<{ t: number; v: number }[]>([])

  useEffect(() => {
    sessionStorage.setItem('adminKey', key)
    const es = new EventSource(`/api/admin/stream?key=${encodeURIComponent(key)}`)
    es.onopen = () => setConn('live')
    es.onerror = () => setConn('lost')
    es.onmessage = (e) => {
      const s: Snap = JSON.parse(e.data)
      const p = prev.current
      const dt = p ? Math.max(s.ts - p.ts, 0.001) : 1
      const throttleRate = p ? Math.max(0, ((s.counters.throttled ?? 0) - (p.counters.throttled ?? 0)) / dt) : 0
      soldTrail.current = [...soldTrail.current, { t: s.ts, v: s.seats.sold }].filter((x) => x.t > s.ts - 30)
      prev.current = s
      setSnap(s)
      setConn('live')
      const push = (a: number[], v: number) => [...a, v].slice(-KEEP)
      setHist((h) => ({
        ops: push(h.ops, s.redis.ops_per_sec ?? 0), rtt: push(h.rtt, s.redis.rtt_ms ?? 0),
        p99: push(h.p99, s.latency.p99), lag: push(h.lag, s.api.loop_lag_ms),
        queued: push(h.queued, s.queued ?? 0), sold: push(h.sold, s.seats.sold),
        batch: push(h.batch, s.gate.batch), throttle: push(h.throttle, throttleRate), tps: push(h.tps, s.pg.tps ?? 0),
      }))
    }
    return () => es.close()
  }, [key])

  const call = useCallback(async (path: string, body?: object) => {
    setMsg(null)
    const r = await fetch(`/api/admin/${path}`, {
      method: 'POST', headers: { 'x-admin-key': key, 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined,
    })
    const j = await r.json().catch(() => ({}))
    if (!r.ok) setMsg(j.detail ?? `Error ${r.status}`)
    else if (j.ok === false) setMsg(j.error)
  }, [key])

  const sellRate = useMemo(() => {
    const t = soldTrail.current
    if (t.length < 2) return 0
    const first = t[0], last = t[t.length - 1]
    return ((last.v - first.v) / Math.max(last.t - first.t, 1)) * 60
  }, [snap])

  if (!snap) {
    return <main className="admin"><p className="muted">{conn === 'lost' ? 'Cannot reach the API (is it running, and is the admin key right?)' : 'Connecting…'}</p>
      <input value={key} onChange={(e) => setKey(e.target.value)} aria-label="Admin key" /></main>
  }

  const c = snap.counters
  const sim = snap.sim
  const redisDown = !snap.redis.up
  const chaosLeft = snap.chaos_until ? Math.max(0, Math.ceil(snap.chaos_until - Date.now() / 1000)) : 0
  const latest = snap.rps[snap.rps.length - 1]
  const degraded = snap.rps.slice(-5).reduce((a, r) => a + (r.degraded || 0), 0)
  const soldOutEta = sellRate > 0.5 ? Math.ceil(snap.seats.available / (sellRate / 60)) : null
  const botsBlocked = Number(sim.bot_throttled || 0) + Number(sim.bot_rejected || 0)
  const funnel = [
    { label: 'Joined queue', v: c.joined ?? 0, color: '#3ddc97' },
    { label: 'Admitted', v: c.admitted ?? 0, color: '#5b9cff' },
    { label: 'Seat held', v: c.held ?? 0, color: '#ffb547' },
    { label: 'Booked', v: c.sold ?? 0, color: '#ff5d6c' },
  ]
  const fmax = Math.max(1, ...funnel.map((f) => f.v))
  const stale = Date.now() / 1000 - snap.ts > 5

  return (
    <main className="admin">
      <header className="top">
        <div><h1>Ticket Drop · Control Room</h1><span className="muted">live from Redis, Postgres &amp; the API process</span></div>
        <div className="top-right">
          <span className={`pill ${conn === 'live' && !stale ? 'ok' : 'bad'}`}><i />{conn === 'live' && !stale ? 'LIVE' : 'STREAM LOST'}</span>
          <span className={`pill ${snap.pg.oversold === 0 ? 'ok' : 'bad'}`} title="sales minus distinct seats, straight from Postgres">
            {snap.pg.up ? (snap.pg.oversold === 0 ? '✓ 0 oversold' : `✗ ${snap.pg.oversold} OVERSOLD`) : 'DB down'}
          </span>
          <input className="keyin" type="password" value={key} onChange={(e) => setKey(e.target.value)} aria-label="Admin key" />
        </div>
      </header>

      {(redisDown || chaosLeft > 0) && (
        <div className="banner" role="alert">
          <b>Redis is {redisDown ? 'unresponsive' : 'about to recover'}.</b> The API is failing closed: new holds and queue joins get a fast 503 + Retry-After,
          nobody can be sold a seat twice (Postgres unique constraint), admission is paused.
          {chaosLeft > 0 && <> Chaos ends in <b>{chaosLeft}s</b>.</>} 503s in the last 5s: <b>{degraded}</b>.
        </div>
      )}
      {msg && <div className="banner warn" role="alert">{msg}</div>}

      <div className="kpis">
        <div className="kpi"><span>In queue</span><b>{n(snap.queued)}</b><Spark data={hist.queued} color="#3ddc97" /></div>
        <div className="kpi"><span>Admitted &amp; shopping</span><b>{n(snap.active)}<small> / {snap.gate.max_active}</small></b>
          <div className="bar"><i style={{ width: `${Math.min(100, ((snap.active ?? 0) / snap.gate.max_active) * 100)}%` }} /></div></div>
        <div className="kpi"><span>Seats held right now</span><b className="held">{snap.seats.held}</b><small className="muted">holds expire after 5 min</small></div>
        <div className="kpi sold">
          <Ring value={snap.seats.sold} total={snap.seats.total}>
            <b key={snap.seats.sold} className="pop">{snap.seats.sold}</b><small>/ {snap.seats.total}</small>
          </Ring>
          <div><span>Tickets booked</span><small className="muted">{sellRate.toFixed(1)}/min{soldOutEta ? ` · sold out in ~${soldOutEta}s` : ''}</small></div>
        </div>
      </div>

      <div className="grid2">
        <Panel title="Traffic hitting the API">
          <div className="big"><b>{n(latest?.total)}</b> req/s</div>
          <StackedTraffic rows={snap.rps} groups={GROUPS} />
        </Panel>
        <Panel title="Funnel &amp; defences">
          {funnel.map((f) => (
            <div className="frow" key={f.label}><span>{f.label}</span><div className="fbar"><i style={{ width: `${(f.v / fmax) * 100}%`, background: f.color }} /></div><b>{f.v.toLocaleString()}</b></div>
          ))}
          <div className="defence">
            <Stat label="Throttled (429)" value={n(c.throttled ?? 0)} tone="warn" />
            <Stat label="Failed proof-of-work" value={n(c.rejected ?? 0)} tone="warn" />
            <Stat label="Hit ticket cap" value={n(c.capped ?? 0)} />
            <Stat label="Throttle rate" value={hist.throttle[hist.throttle.length - 1]?.toFixed(0) ?? 0} unit="/s" />
          </div>
        </Panel>
      </div>

      <div className="grid4">
        <Panel title="Redis" status={redisDown ? 'bad' : (snap.redis.rtt_ms ?? 0) > 20 ? 'warn' : 'ok'}>
          <div className="big"><b>{n(snap.redis.ops_per_sec)}</b> ops/s</div>
          <Spark data={hist.ops} color="#ff7a59" />
          <div className="stats">
            <Stat label="RTT" value={snap.redis.rtt_ms ?? '—'} unit="ms" />
            <Stat label="Clients" value={n(snap.redis.clients)} />
            <Stat label="Memory" value={snap.redis.memory_mb ?? '—'} unit="MB" />
            <Stat label="Keys" value={n(snap.redis.keys)} />
          </div>
        </Panel>
        <Panel title="Postgres" status={snap.pg.up ? 'ok' : 'bad'}>
          <div className="big"><b>{snap.pg.tps ?? '—'}</b> tx/s</div>
          <Spark data={hist.tps} color="#38bdf8" />
          <div className="stats">
            <Stat label="Connections" value={n(snap.pg.connections)} />
            <Stat label="Active" value={n(snap.pg.active)} />
            <Stat label="Rows in sales" value={n(snap.pg.sales)} />
          </div>
        </Panel>
        <Panel title="API process" status={snap.api.loop_lag_ms > 100 ? 'bad' : snap.api.loop_lag_ms > 25 ? 'warn' : 'ok'}>
          <div className="big"><b>{snap.latency.p99}</b> ms p99 booking</div>
          <Spark data={hist.p99} color="#a78bfa" />
          <div className="stats">
            <Stat label="p50" value={snap.latency.p50} unit="ms" />
            <Stat label="Event-loop lag" value={snap.api.loop_lag_ms} unit="ms" tone={snap.api.loop_lag_ms > 25 ? 'warn' : ''} />
            <Stat label="Samples (10s)" value={n(snap.latency.n)} />
          </div>
        </Panel>
        <Panel title="Admission gate" status={snap.gate.ewma_ms > snap.gate.target_ms ? 'warn' : 'ok'}>
          <div className="big"><b>{snap.gate.batch}</b> admitted / sec</div>
          <Spark data={hist.batch} color="#3ddc97" min={0} max={100} />
          <div className="stats">
            <Stat label="Booking EWMA" value={snap.gate.ewma_ms} unit={`/${snap.gate.target_ms}ms`} />
            <Stat label="Mode" value={snap.gate.adaptive ? 'adaptive' : 'manual'} />
          </div>
        </Panel>
      </div>

      <div className="grid2 seats-row">
        <Panel title={`Seat map · ${snap.seats.available} free · ${snap.seats.held} held · ${snap.seats.sold} sold`}>
          <SeatGrid states={snap.seats.states} />
        </Panel>
        <Panel title="Live ticker">
          <ul className="ticker">
            {snap.events.map((e, i) => (
              <li key={`${e.t}-${i}`} className={e.kind}><time>{new Date(e.t * 1000).toLocaleTimeString()}</time><span>{e.msg}</span></li>
            ))}
            {snap.events.length === 0 && <li className="muted">No events yet. Open the drop and run a simulation.</li>}
          </ul>
        </Panel>
      </div>

      <div className="grid2">
        <Panel title="Bot radar">
          <div className="radar">
            <div><b className="good">{n(Number(sim.admitted || 0))}</b><span>humans admitted</span></div>
            <div><b className="good">{n(Number(sim.bought || 0))}</b><span>humans booked</span></div>
            <div><b className="bad">{n(botsBlocked)}</b><span>bot requests blocked</span></div>
            <div><b className="bad">{n(Number(sim.bot_other || 0))}</b><span>bot requests that got through</span></div>
          </div>
          <p className="muted small">
            {sim.running === '1' ? `Simulation running: ${sim.humans_total} humans, ${sim.bots_total} bots, ${sim.pow_solved} proofs-of-work solved, ${sim.joined} joined.`
              : sim.humans_total ? 'Simulation finished.' : 'No simulation running.'}
          </p>
        </Panel>

        <Panel title="Controls">
          <div className="controls">
            <div className="row">
              <button onClick={() => call('drop/open')}>Open the drop</button>
              <button className="danger" onClick={() => call('reset')}>Reset everything</button>
            </div>
            <label>Humans: <b>{humans.toLocaleString()}</b>
              <input type="range" min={100} max={10000} step={100} value={humans} onChange={(e) => setHumans(+e.target.value)} /></label>
            <label>Bots: <b>{bots.toLocaleString()}</b>
              <input type="range" min={0} max={2000} step={50} value={bots} onChange={(e) => setBots(+e.target.value)} /></label>
            <div className="row">
              <button onClick={() => call('simulate', { humans, bots })} disabled={sim.running === '1'}>Run simulation</button>
              <button className="ghost" onClick={() => call('simulate/stop')}>Stop</button>
            </div>
            <hr />
            <label>Gate · max active shoppers: <b>{snap.gate.max_active}</b>
              <input type="range" min={10} max={500} step={10} value={snap.gate.max_active} onChange={(e) => call('gate', { max_active: +e.target.value })} /></label>
            <label className="check"><input type="checkbox" checked={snap.gate.adaptive} onChange={(e) => call('gate', { adaptive: e.target.checked })} /> adaptive batch size (backpressure)</label>
            {!snap.gate.adaptive && (
              <label>Manual batch: <b>{snap.gate.batch}</b>/s
                <input type="range" min={1} max={200} value={snap.gate.batch} onChange={(e) => call('gate', { batch: +e.target.value })} /></label>
            )}
            <hr />
            <div className="row"><span className="muted">Chaos:</span>
              <button className="danger" onClick={() => call('chaos/redis-pause', { seconds: 5 })}>Freeze Redis 5s</button>
              <button className="danger" onClick={() => call('chaos/redis-pause', { seconds: 15 })}>15s</button>
            </div>
          </div>
        </Panel>
      </div>
    </main>
  )
}
