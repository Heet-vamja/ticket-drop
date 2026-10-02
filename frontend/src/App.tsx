import { useEffect, useState } from 'react'
import './App.css'

type QueueState = {
  user: string
  position: number
  ahead: number
  total: number
  eta_seconds: number
}

function formatEta(seconds: number) {
  if (seconds < 60) return `${seconds}s`
  return `${Math.floor(seconds / 60)}m ${seconds % 60}s`
}

export default function App() {
  const [user, setUser] = useState('')
  const [joinedAs, setJoinedAs] = useState<string | null>(null)
  const [state, setState] = useState<QueueState | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function join() {
    setError(null)
    const res = await fetch(`/api/queue/join?user=${encodeURIComponent(user)}`, { method: 'POST' })
    if (!res.ok) {
      setError(res.status === 409 ? 'The drop has not opened yet.' : 'Could not join the queue.')
      return
    }
    setState(await res.json())
    setJoinedAs(user)
  }

  // Live position/ETA over SSE; the browser reconnects on its own if the stream drops.
  useEffect(() => {
    if (!joinedAs) return
    const es = new EventSource(`/api/queue/events?user=${encodeURIComponent(joinedAs)}`)
    es.onmessage = (e) => setState(JSON.parse(e.data))
    es.addEventListener('gone', () => es.close())
    return () => es.close()
  }, [joinedAs])

  return (
    <main className="waiting-room">
      <h1>Final: Ticket Drop</h1>
      {!joinedAs ? (
        <form
          onSubmit={(e) => {
            e.preventDefault()
            if (user.trim()) join()
          }}
        >
          <input
            value={user}
            onChange={(e) => setUser(e.target.value)}
            placeholder="Your name"
            aria-label="Your name"
          />
          <button type="submit">Join the queue</button>
          {error && <p role="alert">{error}</p>}
        </form>
      ) : state ? (
        <section aria-live="polite">
          <p className="position">#{state.position}</p>
          <p>
            {state.ahead} ahead of you · {state.total} in queue
          </p>
          <p>Estimated wait: {formatEta(state.eta_seconds)}</p>
          <p className="hint">Keep this tab open. You keep your place if you refresh.</p>
        </section>
      ) : (
        <p>Joining…</p>
      )}
    </main>
  )
}
