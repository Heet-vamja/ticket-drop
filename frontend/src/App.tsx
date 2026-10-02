import { useEffect, useState } from 'react'
import Admin from './Admin'
import Shop from './Shop'

export default function App() {
  const [route, setRoute] = useState(window.location.hash)
  useEffect(() => {
    const onHash = () => setRoute(window.location.hash)
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])
  return route.startsWith('#/admin') ? <Admin /> : <Shop />
}
