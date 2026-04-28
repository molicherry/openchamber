import { createProxyMiddleware } from 'http-proxy-middleware';

import {
  applyForwardProxyResponseHeaders,
  collectForwardProxyHeaders,
  shouldForwardProxyResponseHeader,
} from '../../proxy-headers.js';
import { parseSseEventEnvelope } from '../event-stream/protocol.js';

export const waitForSseDrain = (res, signal) => new Promise((resolve) => {
  if (signal?.aborted || res.writableEnded || res.destroyed) {
    resolve();
    return;
  }

  const cleanup = () => {
    res.off?.('drain', onDone);
    res.off?.('close', onDone);
    res.off?.('error', onDone);
    signal?.removeEventListener?.('abort', onDone);
  };
  const onDone = () => {
    cleanup();
    resolve();
  };

  res.once?.('drain', onDone);
  res.once?.('close', onDone);
  res.once?.('error', onDone);
  signal?.addEventListener?.('abort', onDone, { once: true });
});

export const writeSseChunkWithBackpressure = async (res, value, signal) => {
  if (!value || value.length === 0 || signal?.aborted || res.writableEnded || res.destroyed) {
    return false;
  }

  const flushed = res.write(value);
  if (flushed !== false) {
    return true;
  }

  await waitForSseDrain(res, signal);
  return !signal?.aborted && !res.writableEnded && !res.destroyed;
};

/**
 * Tracks the trailing bytes of the SSE stream to detect event boundaries.
 * Used by the heartbeat timer to ensure SSE comments (":heartbeat\n\n")
 * are only injected between complete events — never inside a partial event.
 */
export const createSseBoundaryTracker = () => {
  const decoder = new TextDecoder();
  let tail = '';

  const normalize = (value) => value.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

  return {
    observe(value) {
      const text = typeof value === 'string'
        ? value
        : decoder.decode(value, { stream: true });
      if (text.length > 0) {
        tail = `${tail}${normalize(text)}`;
        if (tail.length > 4096) {
          tail = tail.slice(-4096);
        }
      }
      return this.isAtBoundary();
    },
    isAtBoundary() {
      return tail.length === 0 || tail.endsWith('\n\n');
    },
  };
};

// SSE proxy reliability constants
const SSE_REPLAY_LIMIT = 64;
const SSE_STALL_TIMEOUT_MS = 15_000;
const SSE_HEARTBEAT_INTERVAL_MS = 15_000;
const SSE_RECONNECT_DELAY_MS = 250;
const SSE_MAX_RECONNECT_ATTEMPTS = 5;

/**
 * Shared replay buffers keyed by upstream path (e.g. "/global/event", "/event").
 * Each entry stores raw SSE blocks with their event IDs so the proxy can
 * replay events after a browser disconnect + reconnect with Last-Event-ID.
 */
const sseReplayBuffers = new Map();

function getOrCreateReplayBuffer(upstreamPath) {
  const key = upstreamPath || '/';
  let entry = sseReplayBuffers.get(key);
  if (!entry) {
    entry = { blocks: [], lastEventId: '' };
    sseReplayBuffers.set(key, entry);
  }
  return entry;
}

/**
 * Parse a raw byte chunk into SSE event blocks.
 * Returns an array of { raw, eventId } for each complete SSE event found.
 * Incomplete trailing data is returned separately as `remainder` and must be
 * re-fed on the next call to preserve cross-chunk events.
 */
function parseSseChunk(buffer, remainder) {
  const text = buffer instanceof Uint8Array
    ? new TextDecoder().decode(buffer, { stream: true })
    : String(buffer);
  const combined = remainder + text.replace(/\r\n/g, '\n');
  const events = [];
  let idx = combined.indexOf('\n\n');
  let lastIdx = 0;

  while (idx !== -1) {
    const block = combined.slice(lastIdx, idx);
    const envelope = parseSseEventEnvelope(block);
    const eventId = typeof envelope?.eventId === 'string' && envelope.eventId.length > 0
      ? envelope.eventId
      : undefined;
    events.push({ raw: block + '\n\n', eventId });
    lastIdx = idx + 2;
    idx = combined.indexOf('\n\n', lastIdx);
  }

  const newRemainder = combined.slice(lastIdx);
  return { events, remainder: newRemainder };
}

function readLastEventIdFromRequest(req) {
  const raw = req.headers?.['last-event-id'];
  if (typeof raw === 'string' && raw.trim().length > 0) {
    return raw.trim();
  }
  return null;
}

export const registerOpenCodeProxy = (app, deps) => {
  const {
    fs,
    os,
    path,
    OPEN_CODE_READY_GRACE_MS,
    getRuntime,
    getOpenCodeAuthHeaders,
    buildOpenCodeUrl,
    ensureOpenCodeApiPrefix,
  } = deps;

  if (app.get('opencodeProxyConfigured')) {
    return;
  }

  const runtime = getRuntime();
  if (runtime.openCodePort) {
    console.log(`Setting up proxy to OpenCode on port ${runtime.openCodePort}`);
  } else {
    console.log('Setting up OpenCode API gate (OpenCode not started yet)');
  }
  app.set('opencodeProxyConfigured', true);

  const isAbortError = (error) => error?.name === 'AbortError';
  const FALLBACK_PROXY_TARGET = 'http://127.0.0.1:3902';

  const normalizeProxyTarget = (candidate) => {
    if (typeof candidate !== 'string') {
      return null;
    }

    const trimmed = candidate.trim();
    if (!trimmed) {
      return null;
    }

    return trimmed.replace(/\/+$/, '');
  };

  // Keep generic proxy requests on the same upstream base URL that health checks
  // and direct fetch helpers use. This avoids split-brain state where /health
  // succeeds against an external host but /api/* still proxies to 127.0.0.1.
  const resolveProxyTarget = () => {
    try {
      const resolved = normalizeProxyTarget(buildOpenCodeUrl('/', ''));
      if (resolved) {
        return resolved;
      }
    } catch {
    }

    const runtimeState = getRuntime();
    const externalBase = normalizeProxyTarget(runtimeState.openCodeBaseUrl);
    if (externalBase) {
      return externalBase;
    }

    if (runtimeState.openCodePort) {
      return `http://localhost:${runtimeState.openCodePort}`;
    }

    return FALLBACK_PROXY_TARGET;
  };

  /**
   * Replay buffered SSE events to the browser response.
   * Returns true if replay completed successfully, false if the connection dropped.
   */
  const replaySseEvents = async (res, upstreamPath, requestedLastEventId, signal) => {
    const buffer = getOrCreateReplayBuffer(upstreamPath);
    if (!requestedLastEventId || buffer.blocks.length === 0) {
      return true;
    }

    const startIdx = buffer.blocks.findIndex((b) => b.eventId === requestedLastEventId);
    const replayBlocks = startIdx === -1 ? [] : buffer.blocks.slice(startIdx + 1);

    if (replayBlocks.length === 0) {
      return true;
    }

    for (const block of replayBlocks) {
      const canContinue = await writeSseChunkWithBackpressure(
        res,
        Buffer.from(block.raw, 'utf-8'),
        signal,
      );
      if (!canContinue) {
        return false;
      }
    }

    return true;
  };

  /**
   * Forward an SSE stream from OpenCode to the browser with:
   * - Last-Event-ID replay on reconnect (events buffered server-side)
   * - Stall detection (15s timeout) with automatic reconnection
   * - Boundary-aware heartbeat comments (":heartbeat") to keep intermediate
   *   proxies (nginx, CDN, etc.) from timing out
   * - Serialized write queue to prevent heartbeat/data interleaving
   * - Reconnect with backoff on transient failures (up to 5 attempts)
   * - ECONNREFUSED-aware restart handling with Retry-After
   */
  const forwardSseRequest = async (req, res) => {
    const requestUrl = typeof req.originalUrl === 'string' && req.originalUrl.length > 0
      ? req.originalUrl
      : (typeof req.url === 'string' ? req.url : '');
    const upstreamPath = requestUrl.startsWith('/api') ? requestUrl.slice(4) || '/' : requestUrl;
    const buffer = getOrCreateReplayBuffer(upstreamPath);
    const requestedLastEventId = readLastEventIdFromRequest(req);

    // If the browser requested a specific Last-Event-ID, replay buffered events first.
    if (requestedLastEventId) {
      res.status(200);
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');
      res.setHeader('X-Accel-Buffering', 'no');
      if (typeof res.flushHeaders === 'function') {
        res.flushHeaders();
      }
      if (res.socket && typeof res.socket.setNoDelay === 'function') {
        res.socket.setNoDelay(true);
      }

      const replayed = await replaySseEvents(res, upstreamPath, requestedLastEventId, null);
      if (!replayed) {
        res.end();
        return;
      }
    }

    // Reconnection loop — retries on transient failures (ECONNREFUSED during restart,
    // upstream stall, etc.) with backoff up to SSE_MAX_RECONNECT_ATTEMPTS.
    let attemptCount = 0;
    let upstreamLastEventId = buffer.lastEventId;

    while (attemptCount < SSE_MAX_RECONNECT_ATTEMPTS) {
      const abortController = new AbortController();
      const closeUpstream = () => abortController.abort();
      let upstream = null;
      let reader = null;
      let heartbeatTimer = null;
      let writeQueue = Promise.resolve(true);
      const sseBoundary = createSseBoundaryTracker();
      let headersSent = attemptCount === 0 && !requestedLastEventId
        ? false
        : res.headersSent;

      if (attemptCount > 0) {
        await new Promise((resolve) => setTimeout(resolve, SSE_RECONNECT_DELAY_MS));
      }

      req.on('close', closeUpstream);

      try {
        const headers = collectForwardProxyHeaders(req.headers, getOpenCodeAuthHeaders());
        headers.accept ??= 'text/event-stream';
        headers['cache-control'] ??= 'no-cache';
        if (upstreamLastEventId) {
          headers['Last-Event-ID'] = upstreamLastEventId;
        }

        upstream = await fetch(buildOpenCodeUrl(upstreamPath, ''), {
          method: 'GET',
          headers,
          signal: abortController.signal,
        });

        if (!headersSent) {
          if (attemptCount > 0) {
            // Reconnected after transient failure — send a comment to flush any
            // stalled browser buffer before continuing.
            res.write(': reconnect\n\n');
          }
          res.status(upstream.status);
          applyForwardProxyResponseHeaders(upstream.headers, res);

          const contentType = upstream.headers.get('content-type') || 'text/event-stream';
          const isEventStream = contentType.toLowerCase().includes('text/event-stream');

          if (!upstream.body) {
            res.end(await upstream.text().catch(() => ''));
            return;
          }

          if (!isEventStream) {
            res.end(await upstream.text());
            return;
          }

          res.setHeader('Content-Type', contentType);
          res.setHeader('Cache-Control', 'no-cache');
          res.setHeader('Connection', 'keep-alive');
          res.setHeader('X-Accel-Buffering', 'no');
          if (typeof res.flushHeaders === 'function') {
            res.flushHeaders();
          }

          if (res.socket && typeof res.socket.setNoDelay === 'function') {
            res.socket.setNoDelay(true);
          }
          headersSent = true;
        }

        reader = upstream.body.getReader();

        /**
         * Serialized write helper. All SSE writes (data and heartbeats) go
         * through this queue to prevent interleaving from concurrent timers.
         */
        const enqueueSseWrite = (value) => {
          writeQueue = writeQueue
            .catch(() => false)
            .then((canContinue) => {
              if (!canContinue) {
                return false;
              }
              return writeSseChunkWithBackpressure(res, value, abortController.signal);
            });
          return writeQueue;
        };

        // Stall detection: if upstream produces no data for SSE_STALL_TIMEOUT_MS,
        // abort and reconnect.
        let stallTimer = null;

        const clearTimers = () => {
          if (stallTimer) {
            clearTimeout(stallTimer);
            stallTimer = null;
          }
          if (heartbeatTimer) {
            clearTimeout(heartbeatTimer);
            heartbeatTimer = null;
          }
        };

        const resetStallTimer = () => {
          if (stallTimer) clearTimeout(stallTimer);
          stallTimer = setTimeout(() => {
            abortController.abort();
          }, SSE_STALL_TIMEOUT_MS);
        };

        // Heartbeat: boundary-aware keep-alive comments for intermediate proxies.
        // Resets on each data chunk so it only fires during idle periods.
        // Boundary check ensures ":heartbeat\n\n" is never injected inside a
        // partial SSE event — it always lands between complete events.
        const scheduleHeartbeat = () => {
          if (heartbeatTimer) clearTimeout(heartbeatTimer);
          heartbeatTimer = setTimeout(async () => {
            if (abortController.signal.aborted || res.writableEnded || res.destroyed) {
              return;
            }
            if (!sseBoundary.isAtBoundary()) {
              scheduleHeartbeat();
              return;
            }
            const canContinue = await enqueueSseWrite(':heartbeat\n\n');
            if (canContinue && !abortController.signal.aborted) {
              scheduleHeartbeat();
            }
          }, SSE_HEARTBEAT_INTERVAL_MS);
        };

        resetStallTimer();
        scheduleHeartbeat();

        let textRemainder = '';
        while (!abortController.signal.aborted) {
          const { done, value } = await reader.read();
          if (done) {
            break;
          }

          resetStallTimer();
          scheduleHeartbeat();

          if (value && value.length > 0) {
            // Track SSE boundary state for heartbeat safety.
            sseBoundary.observe(value);

            // Parse SSE blocks from the chunk for event ID tracking and replay.
            const { events, remainder } = parseSseChunk(value, textRemainder);
            textRemainder = remainder;

            for (const event of events) {
              if (event.eventId) {
                buffer.lastEventId = event.eventId;
                upstreamLastEventId = event.eventId;
              }
              buffer.blocks.push(event);
              if (buffer.blocks.length > SSE_REPLAY_LIMIT) {
                buffer.blocks.splice(0, buffer.blocks.length - SSE_REPLAY_LIMIT);
              }
            }

            const canContinue = await enqueueSseWrite(value);
            if (!canContinue) {
              break;
            }
          }
        }

        clearTimers();
        res.end();
        return; // Normal stream completion — don't retry.

      } catch (error) {
        clearTimers();
        if (isAbortError(error)) {
          // Stall timeout or client disconnect — retry if client still connected.
          if (res.writableEnded || res.destroyed) {
            return;
          }
          attemptCount++;
          continue;
        }
        console.error('[proxy] OpenCode SSE proxy error:', error?.message ?? error);
        if (!res.headersSent) {
          const isConnRefused = error?.cause?.code === 'ECONNREFUSED' ||
            (error?.message && error.message.includes('ECONNREFUSED'));
          if (isConnRefused) {
            res.setHeader('Retry-After', '2');
            res.status(503).json({ error: 'OpenCode is restarting', restarting: true });
          } else {
            res.status(503).json({ error: 'OpenCode service unavailable' });
          }
        } else {
          res.end();
        }
        return;
      } finally {
        req.off('close', closeUpstream);
        try {
          if (reader) {
            await reader.cancel();
            reader.releaseLock();
          } else if (upstream?.body && !upstream.body.locked) {
            await upstream.body.cancel();
          }
        } catch {
        }
      }
    }
  };

  // Ensure API prefix is detected before proxying
  app.use('/api', (_req, _res, next) => {
    ensureOpenCodeApiPrefix();
    next();
  });

  // Readiness gate — return 503 while OpenCode is starting/restarting
  app.use('/api', (req, res, next) => {
    if (
      req.path.startsWith('/themes/custom') ||
      req.path.startsWith('/push') ||
      req.path.startsWith('/config/agents') ||
      req.path.startsWith('/config/opencode-resolution') ||
      req.path.startsWith('/config/settings') ||
      req.path.startsWith('/config/skills') ||
      req.path === '/config/reload' ||
      req.path === '/health'
    ) {
      return next();
    }

    const runtimeState = getRuntime();
    const waitElapsed = runtimeState.openCodeNotReadySince === 0 ? 0 : Date.now() - runtimeState.openCodeNotReadySince;
    const stillWaiting =
      (!runtimeState.isOpenCodeReady && (runtimeState.openCodeNotReadySince === 0 || waitElapsed < OPEN_CODE_READY_GRACE_MS)) ||
      runtimeState.isRestartingOpenCode ||
      !runtimeState.openCodePort;

    if (stillWaiting) {
      res.setHeader('Retry-After', '2');
      return res.status(503).json({
        error: 'OpenCode is restarting',
        restarting: true,
      });
    }

    next();
  });

  // Windows: session merge for cross-directory session listing
  if (process.platform === 'win32') {
    app.get('/api/session', async (req, res, next) => {
      const rawUrl = req.originalUrl || req.url || '';
      if (rawUrl.includes('directory=')) return next();

      try {
        const authHeaders = getOpenCodeAuthHeaders();
        const fetchOpts = {
          method: 'GET',
          headers: { Accept: 'application/json', ...authHeaders },
          signal: AbortSignal.timeout(10000),
        };
        const globalRes = await fetch(buildOpenCodeUrl('/session', ''), fetchOpts);
        const globalPayload = globalRes.ok ? await globalRes.json().catch(() => []) : [];
        const globalSessions = Array.isArray(globalPayload) ? globalPayload : [];

        const settingsPath = path.join(os.homedir(), '.config', 'openchamber', 'settings.json');
        let projectDirs = [];
        try {
          const settingsRaw = fs.readFileSync(settingsPath, 'utf8');
          const settings = JSON.parse(settingsRaw);
          projectDirs = (settings.projects || [])
            .map((project) => (typeof project?.path === 'string' ? project.path.trim() : ''))
            .filter(Boolean);
        } catch {
        }

        const seen = new Set(
          globalSessions
            .map((session) => (session && typeof session.id === 'string' ? session.id : null))
            .filter((id) => typeof id === 'string')
        );
        const extraSessions = [];
        for (const dir of projectDirs) {
          const candidates = Array.from(new Set([
            dir,
            dir.replace(/\\/g, '/'),
            dir.replace(/\//g, '\\'),
          ]));
          for (const candidateDir of candidates) {
            const encoded = encodeURIComponent(candidateDir);
            try {
              const dirRes = await fetch(buildOpenCodeUrl(`/session?directory=${encoded}`, ''), fetchOpts);
              if (dirRes.ok) {
                const dirPayload = await dirRes.json().catch(() => []);
                const dirSessions = Array.isArray(dirPayload) ? dirPayload : [];
                for (const session of dirSessions) {
                  const id = session && typeof session.id === 'string' ? session.id : null;
                  if (id && !seen.has(id)) {
                    seen.add(id);
                    extraSessions.push(session);
                  }
                }
              }
            } catch {
            }
          }
        }

        const merged = [...globalSessions, ...extraSessions];
        merged.sort((a, b) => {
          const aTime = a && typeof a.time_updated === 'number' ? a.time_updated : 0;
          const bTime = b && typeof b.time_updated === 'number' ? b.time_updated : 0;
          return bTime - aTime;
        });
        console.log(`[SessionMerge] ${globalSessions.length} global + ${extraSessions.length} extra = ${merged.length} total`);
        return res.json(merged);
      } catch (error) {
        console.log(`[SessionMerge] Error: ${error.message}, falling through`);
        next();
      }
    });
  }

  app.get('/api/global/event', forwardSseRequest);
  app.get('/api/event', forwardSseRequest);

  // Generic proxy for non-SSE OpenCode API routes.
  const apiProxy = createProxyMiddleware({
    target: resolveProxyTarget(),
    changeOrigin: true,
    pathRewrite: { '^/api': '' },
    // Dynamic target — port can change after restart
    router: () => resolveProxyTarget(),
    on: {
      proxyReq: (proxyReq) => {
        // Inject OpenCode auth headers
        const authHeaders = getOpenCodeAuthHeaders();
        if (authHeaders.Authorization) {
          proxyReq.setHeader('Authorization', authHeaders.Authorization);
        }

        // Defensive: request identity encoding from upstream OpenCode.
        // This avoids compressed-body/header mismatches in multi-proxy setups.
        proxyReq.setHeader('accept-encoding', 'identity');
      },
      proxyRes: (proxyRes) => {
        for (const key of Object.keys(proxyRes.headers || {})) {
          if (!shouldForwardProxyResponseHeader(key)) {
            delete proxyRes.headers[key];
          }
        }
      },
      error: (err, _req, res) => {
        console.error('[proxy] OpenCode proxy error:', err.message);
        if (res && !res.headersSent && typeof res.status === 'function') {
          res.status(503).json({ error: 'OpenCode service unavailable' });
        }
      },
    },
  });

  app.use('/api', apiProxy);
};
