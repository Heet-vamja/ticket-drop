// Proof of work: find i such that sha256(`${nonce}:${user}:${i}`) has `bits` leading zero bits.
// Costs a human a fraction of a second once; costs a bot farm real CPU per account.
function leadingZeroBits(d: Uint8Array): number {
  let n = 0
  for (const byte of d) {
    if (byte === 0) { n += 8; continue }
    return n + Math.clz32(byte) - 24
  }
  return n
}

export async function solvePow(nonce: string, user: string, bits: number): Promise<string> {
  const enc = new TextEncoder()
  for (let i = 0; ; i++) {
    const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', enc.encode(`${nonce}:${user}:${i}`)))
    if (leadingZeroBits(digest) >= bits) return String(i)
    if (i % 1500 === 0) await new Promise((r) => setTimeout(r)) // keep the UI responsive
  }
}
