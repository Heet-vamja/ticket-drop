import { useCallback, useEffect, useRef, useState } from 'react'
import { solvePow } from './pow'
import './App.css'

type Phase = 'name' | 'solving' | 'queued' | 'admitted' | 'soldout' | 'error'
type Queued = { position: number; ahead: number; total: number; eta_seconds: number | null }

const HOLD_SECONDS = 300
const fmt = (s: number) => (s < 60 ? `${s}s` : `${Math.floor(s / 60)}m ${String(s % 60).padStart(2, '0')}s`)
const tokenExp = (t: string): number => JSON.parse(atob(t.split('.')[1])).exp * 1000

async function message(res: Response): Promise<string> {
  if (res.status === 429) return 'Too many requests, slow down for a moment.'
  if (res.status === 503) return 'The system is recovering, please retry in a couple of seconds.'
  try { return (await res.json()).detail ?? `Error ${res.status}` } catch { return `Error ${res.status}` }
}

export default function Shop() {
  const [user, setUser] = useState('')
  const [phase, setPhase] = useState<Phase>('name')
  const [error, setError] = useState<string | null>(null)
  const [queue, setQueue] = useState<Queued | null>(null)
  const [token, setToken] = useState<string | null>(null)
  const [seats, setSeats] = useState('')
  const [mine, setMine] = useState<Record<number, number>>({}) // seat -> hold expiry (ms)
  const [bought, setBought] = useState<number[]>([])
  const [now, setNow] = useState(Date.now())
  const [notice, setNotice] = useState<string | null>(null)
  const userRef = useRef('')

  const join = async () => {
    setError(null)
    setPhase('solving')
    try {
      const ch = await fetch(`/api/queue/challenge?user=${encodeURIComponent(user)}`)
      if (!ch.ok) throw new Error(await message(ch))
      const { nonce, bits } = await ch.json()
      const solution = await solvePow(nonce, user, bits)
      const res = await fetch(`/api/queue/join?user=${encodeURIComponent(user)}&solution=${solution}`, { method: 'POST' })
      if (res.status === 410) { setPhase('soldout'); return }
      if (!res.ok) throw new Error(res.status === 409 ? 'The drop has not opened yet.' : await message(res))
      userRef.current = user
      setQueue(await res.json())
      setPhase('queued')
    } catch (e) {
      setError((e as Error).message)
      setPhase('name')
    }
  }

  // Live position / ETA over SSE; the browser reconnects by itself if the stream drops.
  useEffect(() => {
    if (phase !== 'queued') return
    const es = new EventSource(`/api/queue/events?user=${encodeURIComponent(userRef.current)}`)
    es.onmessage = (e) => {
      const data = JSON.parse(e.data)
      if (data.status === 'admitted') {
        setToken(data.token)
        setPhase('admitted')
        es.close()
      } else setQueue(data)
    }
    es.addEventListener('soldout', () => { setPhase('soldout'); es.close() })
    es.addEventListener('gone', () => { setError('You left the queue. Join again to get a place.'); setPhase('name'); es.close() })
    return () => es.close()
  }, [phase])

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 500)
    return () => clearInterval(id)
  }, [])

  const auth = useCallback(() => ({ Authorization: `Bearer ${token}` }), [token])

  useEffect(() => {
    if (phase !== 'admitted') return
    let live = true
    const load = async () => {
      const r = await fetch('/api/shop/seats', { headers: auth() })
      if (live && r.ok) setSeats((await r.json()).seats)
    }
    load()
    const id = setInterval(load, 2000)
    return () => { live = false; clearInterval(id) }
  }, [phase, auth])

  const act = async (path: string, seat: number, ok: () => void) => {
    setNotice(null)
    const r = await fetch(`/api/shop/${path}/${seat}`, { method: 'POST', headers: auth() })
    if (r.ok) ok()
    else setNotice(r.status === 409 ? 'Someone just took that seat.' : await message(r))
    const s = await fetch('/api/shop/seats', { headers: auth() })
    if (s.ok) setSeats((await s.json()).seats)
  }

  const hold = (seat: number) => act('hold', seat, () => setMine((m) => ({ ...m, [seat]: Date.now() + HOLD_SECONDS * 1000 })))
  const pay = (seat: number) => act('confirm', seat, () => {
    setMine((m) => { const { [seat]: _gone, ...rest } = m; return rest })
    setBought((b) => [...b, seat])
  })
  const release = (seat: number) => act('release', seat, () => setMine((m) => { const { [seat]: _gone, ...rest } = m; return rest }))

  if (phase !== 'admitted') {
    return (
      <main className="shop">
        <h1>Final · Ticket Drop</h1>
        {(phase === 'name' || phase === 'solving' || phase === 'error') && (
          <form onSubmit={(e) => { e.preventDefault(); if (user.trim() && phase !== 'solving') join() }}>
            <input value={user} onChange={(e) => setUser(e.target.value)} placeholder="Your name" aria-label="Your name" />
            <button type="submit" disabled={phase === 'solving'}>{phase === 'solving' ? 'Verifying you are human…' : 'Join the queue'}</button>
            {error && <p role="alert" className="err">{error}</p>}
          </form>
        )}
        {phase === 'soldout' && <p className="position" style={{ fontSize: '2rem' }}>Sold out. Thanks for waiting.</p>}
        {phase === 'queued' && queue && (
          <section aria-live="polite">
            <p className="position">#{queue.position.toLocaleString()}</p>
            <p>{queue.ahead.toLocaleString()} ahead of you · {queue.total.toLocaleString()} in queue</p>
            <p>Estimated wait: {queue.eta_seconds == null ? '…' : fmt(queue.eta_seconds)}</p>
            <p className="hint">Keep this tab open. Refreshing keeps your place if you rejoin with the same name.</p>
          </section>
        )}
      </main>
    )
  }

  const admissionLeft = token ? Math.max(0, Math.round((tokenExp(token) - now) / 1000)) : 0
  return (
    <main className="shop wide">
      <h1>Pick your seats</h1>
      <p className="hint">
        {admissionLeft > 0 ? `You have ${fmt(admissionLeft)} to start a hold.` : 'Admission window over: you can still pay for seats you hold.'}
        {' '}Max 4 tickets. Holds last {HOLD_SECONDS / 60} minutes.
      </p>
      {notice && <p role="alert" className="err">{notice}</p>}
      {bought.length > 0 && <p className="ok">Booked: {bought.map((s) => `#${s}`).join(', ')}</p>}
      <div className="legend">
        <span className="chip free" /> available <span className="chip held" /> held <span className="chip sold" /> sold <span className="chip mine" /> yours
      </div>
      <div className="seatmap" role="grid">
        {[...seats].map((c, i) => {
          const isMine = i in mine || bought.includes(i)
          const cls = isMine ? 'mine' : c === 'a' ? 'free' : c === 'h' ? 'held' : 'sold'
          return (
            <button key={i} className={`seat ${cls}`} disabled={cls !== 'free'} title={`Seat ${i}`} aria-label={`Seat ${i} ${cls}`} onClick={() => hold(i)} />
          )
        })}
      </div>
      {Object.entries(mine).map(([seat, exp]) => (
        <div className="holdrow" key={seat}>
          <strong>Seat #{seat}</strong> held · {fmt(Math.max(0, Math.round((exp - now) / 1000)))} left
          <button onClick={() => pay(Number(seat))}>Pay (simulated)</button>
          <button className="ghost" onClick={() => release(Number(seat))}>Release</button>
        </div>
      ))}
    </main>
  )
}
