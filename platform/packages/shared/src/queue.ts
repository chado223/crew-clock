/**
 * Offline-safe action queue, platform-neutral so it can be tested in Node and
 * used by the phone app with AsyncStorage.
 *
 * - Every action is saved first, then sent in order (oldest first).
 * - No signal: it stays, the attempt is counted, sending stops (order kept).
 * - The server refused it for good (a conflict, e.g. the office canceled the
 *   stop): it is removed and reported so the screen can explain.
 * - Only one send loop runs at a time, so two triggers (app reopened + screen
 *   focused) never send the same action twice at once.
 * Duplicates across retries are prevented server-side by event ids and
 * idempotent functions; this keeps the phone side honest too.
 */
export interface QueueStorage {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<void>;
  remove(key: string): Promise<void>;
}

export type Queued<A> = A & { eventId: string; at: string; attempts: number };

export interface QueueOptions<A> {
  storage: QueueStorage;
  key: string;
  legacyKeys?: string[];
  send: (action: Queued<A>) => Promise<void>;
  isNetworkError: (err: unknown) => boolean;
  newId: () => string;
  now?: () => Date;
}

export interface Rejection<A> {
  action: Queued<A>;
  error: unknown;
}

export function createQueue<A extends object>(o: QueueOptions<A>) {
  let running: Promise<{ rejected: Rejection<A>[] }> | null = null;
  const now = o.now ?? (() => new Date());

  async function load(): Promise<Queued<A>[]> {
    const raw = await o.storage.get(o.key);
    let list = raw ? (JSON.parse(raw) as Queued<A>[]) : [];
    for (const k of o.legacyKeys ?? []) {
      const old = await o.storage.get(k);
      if (old) {
        list = [...(JSON.parse(old) as Queued<A>[]), ...list];
        await o.storage.set(o.key, JSON.stringify(list));
        await o.storage.remove(k);
      }
    }
    return list;
  }
  const save = (list: Queued<A>[]) => o.storage.set(o.key, JSON.stringify(list));

  async function drain(): Promise<{ rejected: Rejection<A>[] }> {
    const rejected: Rejection<A>[] = [];
    for (;;) {
      const list = await load();
      const next = list[0];
      if (!next) break;
      try {
        await o.send(next);
      } catch (err) {
        if (o.isNetworkError(err)) {
          next.attempts += 1;
          await save(list);
          break;
        }
        rejected.push({ action: next, error: err });
      }
      // Re-read before removing: actions added while sending must survive.
      const fresh = await load();
      await save(fresh.filter((q) => q.eventId !== next.eventId));
    }
    return { rejected };
  }

  /** Send what's waiting. Concurrent calls share one run. */
  function flush(): Promise<{ rejected: Rejection<A>[] }> {
    if (!running) running = drain().finally(() => { running = null; });
    return running;
  }

  /** Record an action; "sent" when the server confirmed it, "queued" when offline. Throws if refused. */
  async function perform(action: A): Promise<"sent" | "queued"> {
    const q = { ...action, eventId: o.newId(), at: now().toISOString(), attempts: 0 } as Queued<A>;
    await save([...(await load()), q]);
    if (running) await running;           // let an in-flight run finish first
    const { rejected } = await flush();
    const mine = rejected.find((r) => r.action.eventId === q.eventId);
    if (mine) throw mine.error;
    return (await load()).some((x) => x.eventId === q.eventId) ? "queued" : "sent";
  }

  return { perform, flush, pending: load };
}
