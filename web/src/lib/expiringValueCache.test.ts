import { describe, expect, it } from 'bun:test'

import { createExpiringValueCache } from './expiringValueCache'

describe('createExpiringValueCache', () => {
  it('expires stale entries on read', () => {
    let now = 1_000
    const cache = createExpiringValueCache<string>(500, () => now)

    cache.set('token', 'th-a1b2-secret')

    expect(cache.get('token')).toBe('th-a1b2-secret')
    expect(cache.has('token')).toBe(true)

    now = 1_500

    expect(cache.get('token')).toBeNull()
    expect(cache.has('token')).toBe(false)
  })

  it('supports overwriting and deletion', () => {
    const cache = createExpiringValueCache<string>(1_000, () => 0)

    cache.set('token', 'first')
    cache.set('token', 'second')

    expect(cache.get('token')).toBe('second')
    expect(cache.delete('token')).toBe(true)
    expect(cache.get('token')).toBeNull()
  })
})
