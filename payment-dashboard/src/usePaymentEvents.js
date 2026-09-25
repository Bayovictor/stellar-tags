import { useCallback, useEffect, useRef, useState } from 'react';

// ---------------------------------------------------------------------------
// #730 — usePaymentEvents: subscribe to the backend's SSE status stream
// ---------------------------------------------------------------------------
// Wraps the browser's native EventSource. Reconnection is handled entirely by
// the EventSource API (it retries automatically and replays missed events via
// Last-Event-ID), so this hook only has to:
//   • open/close the stream with the connection lifecycle
//   • expose connection state and the latest event to the caller
//
// Usage:
//   const { status, lastEvent, connectedAt } = usePaymentEvents({ address });
// ---------------------------------------------------------------------------

const DEFAULT_RETRY_MS = 25_000;

/**
 * @param {object} [options]
 * @param {string} [options.address] Only receive events for this Stellar address.
 * @param {string} [options.apiBase] API base URL (defaults to the shared API_BASE).
 * @param {(event: MessageEvent) => void} [options.onEvent] Called for every payment event.
 * @param {string[]} [options.events] Event names to listen for
 *   (default: ['payment.created', 'payment.received']).
 * @returns {{ status: 'idle'|'connecting'|'open'|'error'|'closed', lastEvent: object|null, connectedAt: number|null }}
 */
export const usePaymentEvents = ({
  address = null,
  apiBase,
  onEvent,
  events = ['payment.created', 'payment.received'],
} = {}) => {
  const resolvedBase = apiBase ?? (
    typeof import.meta !== 'undefined' && import.meta.env?.VITE_API_BASE
      ? import.meta.env.VITE_API_BASE
      : 'https://stellar-tags.onrender.com'
  );

  const [status, setStatus] = useState('idle');
  const [lastEvent, setLastEvent] = useState(null);
  const [connectedAt, setConnectedAt] = useState(null);

  // Keep the callback in a ref so a re-render never tears the stream down.
  const onEventRef = useRef(onEvent);
  useEffect(() => {
    onEventRef.current = onEvent;
  }, [onEvent]);

  const handleEvent = useCallback((messageEvent) => {
    let data = null;
    try {
      data = JSON.parse(messageEvent.data);
    } catch {
      data = { raw: messageEvent.data };
    }
    const enriched = { event: messageEvent.type, id: messageEvent.lastEventId, data };
    setLastEvent(enriched);
    onEventRef.current?.(enriched);
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined' || typeof EventSource === 'undefined') {
      return undefined; // Non-browser environment; nothing to subscribe to.
    }

    const params = new URLSearchParams();
    if (address) params.set('address', address);
    const url = `${resolvedBase}/api/v1/events/status${params.size ? `?${params}` : ''}`;

    setStatus('connecting');
    const source = new EventSource(url);

    source.onopen = () => {
      setStatus('open');
      setConnectedAt(Date.now());
    };

    source.onerror = () => {
      // readyState CONNECTING means the browser is already retrying natively;
      // CLOSED means the server told it to stop (e.g. deploy in progress).
      setStatus(
        source.readyState === EventSource.CLOSED ? 'closed' : 'connecting',
      );
    };

    for (const eventName of events) {
      source.addEventListener(eventName, handleEvent);
    }
    source.addEventListener('connected', handleEvent);

    return () => {
      source.close();
      setStatus('closed');
      setConnectedAt(null);
    };
  }, [address, resolvedBase, handleEvent, events]);

  return { status, lastEvent, connectedAt, retryMs: DEFAULT_RETRY_MS };
};

export default usePaymentEvents;
