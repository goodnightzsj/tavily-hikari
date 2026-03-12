export interface ExpiringValueCache<T> {
  get(key: string): T | null
  has(key: string): boolean
  set(key: string, value: T): void
  delete(key: string): boolean
  clear(): void
}

interface ExpiringEntry<T> {
  value: T
  expiresAt: number
}

export function createExpiringValueCache<T>(
  ttlMs: number,
  now: () => number = Date.now,
): ExpiringValueCache<T> {
  const store = new Map<string, ExpiringEntry<T>>()

  const readEntry = (key: string): ExpiringEntry<T> | null => {
    const entry = store.get(key)
    if (!entry) return null
    if (entry.expiresAt <= now()) {
      store.delete(key)
      return null
    }
    return entry
  }

  return {
    get(key) {
      return readEntry(key)?.value ?? null
    },
    has(key) {
      return readEntry(key) != null
    },
    set(key, value) {
      store.set(key, { value, expiresAt: now() + ttlMs })
    },
    delete(key) {
      return store.delete(key)
    },
    clear() {
      store.clear()
    },
  }
}
