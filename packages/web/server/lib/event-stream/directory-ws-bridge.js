import { sendMessageStreamWsEvent, sendMessageStreamWsFrame } from './protocol.js';
import { createUpstreamSseReader } from './upstream-reader.js';

function shouldTriggerUpstreamHealthCheck(upstream) {
  if (!upstream) {
    return true;
  }

  if (!upstream.body) {
    return upstream.ok || upstream.status >= 500;
  }

  return upstream.status >= 500;
}

const directoryHubs = new Map();

function getOrCreateDirectoryHub(key) {
  let hub = directoryHubs.get(key);
  if (hub) {
    return hub;
  }

  const sockets = new Set();
  let controller = null;
  let reader = null;
  let connected = false;
  let everConnected = false;

  const addSocket = (socket) => {
    sockets.add(socket);
  };

  const removeSocket = (socket) => {
    sockets.delete(socket);
    if (sockets.size === 0) {
      hub.cleanup();
      directoryHubs.delete(key);
    }
  };

  const cleanup = () => {
    if (controller && !controller.signal.aborted) {
      controller.abort();
    }
    reader?.stop();
    reader = null;
    controller = null;
    connected = false;
    everConnected = false;
  };

  hub = { addSocket, removeSocket, cleanup, get connected() { return connected; }, get everConnected() { return everConnected; }, _setConnected(v) { connected = v; }, _setEverConnected(v) { everConnected = v; }, _setReader(r) { reader = r; }, _setController(c) { controller = c; }, _getController() { return controller; }, _getSockets() { return sockets; } };
  directoryHubs.set(key, hub);
  return hub;
}

export function clearDirectoryHubs() {
  for (const hub of directoryHubs.values()) {
    hub.cleanup();
  }
  directoryHubs.clear();
}

export function acceptDirectoryMessageStreamWsConnection({
  socket,
  requestedLastEventId,
  requestedDirectory,
  buildOpenCodeUrl,
  getOpenCodeAuthHeaders,
  processForwardedEventPayload,
  wsClients,
  triggerHealthCheck,
  heartbeatIntervalMs,
  upstreamStallTimeoutMs,
  upstreamReconnectDelayMs,
  fetchImpl,
}) {
  const normalizedDir = requestedDirectory || 'global';
  const hubKey = `dir:${normalizedDir}`;
  const hub = getOrCreateDirectoryHub(hubKey);

  const controller = new AbortController();
  let upstreamConnected = false;
  let streamReady = false;

  const cleanupSocket = () => {
    if (!controller.signal.aborted) {
      controller.abort();
    }
    wsClients.delete(socket);
    hub.removeSocket(socket);
  };

  const pingInterval = setInterval(() => {
    if (socket.readyState !== 1) {
      return;
    }

    try {
      socket.ping();
    } catch {
    }
  }, heartbeatIntervalMs);

  const heartbeatInterval = setInterval(() => {
    if (!upstreamConnected) {
      return;
    }

    sendMessageStreamWsEvent(socket, { type: 'openchamber:heartbeat', timestamp: Date.now() }, { directory: 'global' });
  }, heartbeatIntervalMs);

  socket.on('close', () => {
    clearInterval(pingInterval);
    clearInterval(heartbeatInterval);
    upstreamConnected = false;
    cleanupSocket();
  });

  socket.on('error', () => {
    void 0;
  });

  hub.addSocket(socket);

  if (hub.everConnected) {
    sendMessageStreamWsFrame(socket, {
      type: 'ready',
      scope: 'directory',
    });
    streamReady = true;
    upstreamConnected = hub.connected;
    return;
  }

  const run = async () => {
    const broadcastEvent = ({ envelope, payload }) => {
      const directory = requestedDirectory || envelope?.directory || 'global';

      for (const clientSocket of hub._getSockets()) {
        sendMessageStreamWsEvent(clientSocket, payload, {
          directory,
          eventId: typeof envelope?.eventId === 'string' && envelope.eventId.length > 0 ? envelope.eventId : undefined,
        });

        processForwardedEventPayload(payload, (syntheticPayload) => {
          sendMessageStreamWsEvent(clientSocket, syntheticPayload, { directory: 'global' });
        });
      }
    };

    try {
      let buildUrlFailed = false;
      const closeAllWithError = ({ message, closeReason = message, triggerHealthCheckFor = null }) => {
        for (const clientSocket of hub._getSockets()) {
          sendMessageStreamWsFrame(clientSocket, { type: 'error', message });
          try {
            clientSocket.close(1011, closeReason);
          } catch {
          }
          wsClients.delete(clientSocket);
        }
        hub._getSockets().clear();
        if (triggerHealthCheckFor === true || (triggerHealthCheckFor && shouldTriggerUpstreamHealthCheck(triggerHealthCheckFor))) {
          triggerHealthCheck?.();
        }
        hub.cleanup();
        directoryHubs.delete(hubKey);
      };

      const reader = createUpstreamSseReader({
        initialLastEventId: requestedLastEventId,
        signal: controller.signal,
        stallTimeoutMs: upstreamStallTimeoutMs,
        reconnectDelayMs: upstreamReconnectDelayMs,
        fetchImpl,
        buildUrl: () => {
          buildUrlFailed = false;
          let targetUrl;
          try {
            targetUrl = new URL(buildOpenCodeUrl('/event', ''));
          } catch {
            buildUrlFailed = true;
            throw new Error('OpenCode service unavailable');
          }

          if (requestedDirectory) {
            targetUrl.searchParams.set('directory', requestedDirectory);
          }

          return targetUrl;
        },
        getHeaders: getOpenCodeAuthHeaders,
        onConnect() {
          if (!streamReady) {
            for (const clientSocket of hub._getSockets()) {
              sendMessageStreamWsFrame(clientSocket, {
                type: 'ready',
                scope: 'directory',
              });
            }
            streamReady = true;
          }

          upstreamConnected = true;
          hub._setConnected(true);
          hub._setEverConnected(true);
        },
        onDisconnect() {
          upstreamConnected = false;
          hub._setConnected(false);
        },
        onEvent: broadcastEvent,
        onError(error) {
          if (controller.signal.aborted) {
            return;
          }

          if (!streamReady) {
            if (error?.type === 'upstream_unavailable') {
              closeAllWithError({
                message: `OpenCode event stream unavailable (${error.status})`,
                closeReason: 'OpenCode event stream unavailable',
                triggerHealthCheckFor: error.response,
              });
              return;
            }

            closeAllWithError({
              message: buildUrlFailed ? 'OpenCode service unavailable' : 'Failed to connect to OpenCode event stream',
              closeReason: buildUrlFailed ? 'OpenCode service unavailable' : 'Failed to connect to OpenCode event stream',
              triggerHealthCheckFor: !buildUrlFailed,
            });
            return;
          }

          if (error?.type === 'stream_error') {
            console.warn('Message stream WS proxy error:', error.error);
          }
        },
      });

      hub._setReader(reader);
      hub._setController(controller);

      await reader.start();
    } catch (error) {
      if (!controller.signal.aborted) {
        console.warn('Message stream WS proxy error:', error);
        for (const clientSocket of hub._getSockets()) {
          sendMessageStreamWsFrame(clientSocket, { type: 'error', message: 'Message stream proxy error' });
          try {
            clientSocket.close(1011, 'Message stream proxy error');
          } catch {
          }
          wsClients.delete(clientSocket);
        }
        hub._getSockets().clear();
      }
    } finally {
      cleanupSocket();
      try {
        if (socket.readyState === 1 || socket.readyState === 0) {
          socket.close();
        }
      } catch {
      }
    }
  };

  void run();
}
