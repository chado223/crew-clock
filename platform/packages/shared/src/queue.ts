/**
 * Offline-safe action queue, platform-neutral so it can be tested in Node and
 * used by the phone app with AsyncStorage.
 *
 * - Every action is saved first, then sent in order (oldest first).
 * - No signal, a server hiccup, or an expired sign-in: it stays, the attempt
 *   is counted, sending stops (order kept) and it's retried later.
 * - The server refused it for good (a conflict, e.g. the office canceled the
 *   stop, or a punch too old to accept): it's moved to a saved problem list
 *   that stays on the phone until the person dismisses it, and reported so
 *   the office can be told. Nothing disappears silently.
 * - Each action belongs to the person who took it. On a shared phone, actions
 *   are only sent while that same person is signed in; nobody else's sign-in
 *   is ever used for them.
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

export type Queued<A> = A & { eventId: string; at: string; attempts: number; owner?: string | null };

/** "retry" = keep and try later (no signal, server hiccup, signed out); "reject" = refused for good. */
export type Outcome = "retry" | "reject";

export interface QueueOptions<A> {
  storage: QueueStorage;
  key: string;
  legacyKeys?: string[];
  send: (action: Queued<A>) => Promise<void>;
  /** Decide what a failure means. Defaults to isNetworkError → retry, else reject. */
  classify?: (err: unknown) => Outcome;
  isNetworkError?: (err: unknown) => boolean;
  /** Who is signed in now (user id), or null. Actions are stamped with it and only sent for it. */
  owner?: () => Promise<string | null>;
  /** Told about each refusal once, e.g. to let the office know. Failures here are ignored. */
  onReject?: (r: Rejection<A>) => Promise<void>;
  newId: () => string;
  now?: () => Date;
}

export interface Rejection<A> {
  action: Queued<A>;
  error: unknown;
}

export interface Problem<A> {
  action: Queued<A>;
  error: string;
  at: string;
}

const errText = (e: unknown) =>
  e instanceof Error ? e.message : String((e as { message?: unknown })?.message ?? e);

export function createQueue<A extends object>(o: QueueOptions<A>) {
  let running: Promise<{ rejected: Rejection<A>[] }> | null = null;
  const now = o.now ?? (() => new Date());
  const problemsKey = `${o.key}.problems`;
  const classify = o.classify ?? ((e: unknown) => (o.isNetworkError?.(e) ? "retry" : "reject"));
  const whoAmI = async () => (o.owner ? await o.owner() : null);

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
  async function loadProblems(): Promise<Problem<A>[]> {
    const raw = await o.storage.get(problemsKey);
    return raw ? (JSON.parse(raw) as Problem<A>[]) : [];
  }

  /** An action may be sent by this sign-in: same owner, or saved before owners were recorded. */
  const mine = (q: Queued<A>, me: string | null) => !o.owner || q.owner === undefined || q.owner === me;

  async function drain(): Promise<{ rejected: Rejection<A>[] }> {
    const rejected: Rejection<A>[] = [];
    const me = await whoAmI();
    if (o.owner && !me) return { rejected };   // signed out: hold everything
    for (;;) {
      const list = await load();
      const next = list.find((q) => mine(q, me));
      if (!next) break;
      try {
        await o.send(next);
      } catch (err) {
        if (classify(err) === "retry") {
          next.attempts += 1;
          await save(list);
          break;
        }
        const r = { action: next, error: err };
        rejected.push(r);
        await o.storage.set(problemsKey, JSON.stringify([
          ...(await loadProblems()), { action: next, error: errText(err), at: now().toISOString() },
        ]));
        await o.onReject?.(r).catch(() => {});
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
    const q = { ...action, eventId: o.newId(), at: now().toISOString(), attempts: 0, ...(o.owner ? { owner: await whoAmI() } : {}) } as Queued<A>;
    await save([...(await load()), q]);
    if (running) await running;           // let an in-flight run finish first
    const { rejected } = await flush();
    const hit = rejected.find((r) => r.action.eventId === q.eventId);
    if (hit) {
      // The person sees this refusal right now; it doesn't need to stay on the problem list.
      await o.storage.set(problemsKey, JSON.stringify((await loadProblems()).filter((p) => p.action.eventId !== q.eventId)));
      throw hit.error;
    }
    return (await load()).some((x) => x.eventId === q.eventId) ? "queued" : "sent";
  }

  /** Actions waiting for the person signed in now. */
  async function pending(): Promise<Queued<A>[]> {
    const me = await whoAmI();
    return (await load()).filter((q) => mine(q, me));
  }

  /** Actions saved by someone else who used this phone and hasn't signed back in. */
  async function heldForOthers(): Promise<number> {
    if (!o.owner) return 0;
    const me = await whoAmI();
    return (await load()).filter((q) => !mine(q, me)).length;
  }

  /** Refusals the person hasn't dismissed yet (only their own). */
  async function problems(): Promise<Problem<A>[]> {
    const me = await whoAmI();
    return (await loadProblems()).filter((p) => mine(p.action, me));
  }

  async function dismissProblem(eventId: string): Promise<void> {
    await o.storage.set(problemsKey, JSON.stringify((await loadProblems()).filter((p) => p.action.eventId !== eventId)));
  }

  return { perform, flush, pending, heldForOthers, problems, dismissProblem };
}
