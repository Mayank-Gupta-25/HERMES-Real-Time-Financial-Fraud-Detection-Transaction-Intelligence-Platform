import { useCallback, useEffect, useRef, useState } from "react";

const DEFAULT_WS_URL =
  (typeof import.meta !== "undefined" &&
    import.meta.env &&
    import.meta.env.VITE_WS_ALERTS_URL) ||
  "ws://localhost:8003/ws/alerts";

/**
 * Auto-reconnecting WebSocket hook for real-time `FRAUD_ALERT` notifications (`MOD-07-FN-02`).
 * Connects to `ws://localhost:8003/ws/alerts` with exponential backoff and deduplicates by `alert_id`.
 */
export function useWebSocket(wsUrl = DEFAULT_WS_URL, initialAlerts = []) {
  const [status, setStatus] = useState("CONNECTING");
  const [alerts, setAlerts] = useState(initialAlerts);
  const [latestToast, setLatestToast] = useState(null);
  const seenIdsRef = useRef(new Set(initialAlerts.map((a) => a.alert_id || a.event_id)));
  const retryCountRef = useRef(0);
  const wsRef = useRef(null);

  const pushIncomingAlert = useCallback((incoming) => {
    if (!incoming || typeof incoming !== "object") return;
    const key = incoming.alert_id || incoming.event_id;
    if (key && seenIdsRef.current.has(key)) return;
    if (key) seenIdsRef.current.add(key);

    setAlerts((prev) => [incoming, ...prev].slice(0, 100));
    setLatestToast(incoming);
  }, []);

  useEffect(() => {
    if (typeof WebSocket === "undefined") {
      setStatus("RECONNECTING");
      return undefined;
    }

    let unmounted = false;
    let reconnectTimer = null;

    function connect() {
      if (unmounted) return;
      try {
        const ws = new WebSocket(wsUrl);
        wsRef.current = ws;

        ws.onopen = () => {
          if (unmounted) return;
          retryCountRef.current = 0;
          setStatus("CONNECTED");
        };

        ws.onmessage = (event) => {
          if (unmounted) return;
          try {
            const payload = JSON.parse(event.data);
            if (payload.type === "FRAUD_ALERT" || payload.alert_id) {
              pushIncomingAlert(payload);
            }
          } catch (_err) {
            // Ignore malformed keepalive frames
          }
        };

        ws.onclose = () => {
          if (unmounted) return;
          setStatus("RECONNECTING");
          const delayMs = Math.min(1000 * Math.pow(2, retryCountRef.current), 15000);
          retryCountRef.current += 1;
          reconnectTimer = setTimeout(connect, delayMs);
        };

        ws.onerror = () => {
          if (ws && ws.readyState === WebSocket.OPEN) {
            ws.close();
          }
        };
      } catch (_err) {
        setStatus("RECONNECTING");
      }
    }

    connect();

    return () => {
      unmounted = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      if (wsRef.current) {
        try {
          wsRef.current.close();
        } catch (_e) {
          // Ignore close error on cleanup
        }
      }
    };
  }, [wsUrl, pushIncomingAlert]);

  return {
    status,
    alerts,
    setAlerts,
    latestToast,
    clearToast: () => setLatestToast(null),
    pushIncomingAlert,
  };
}

export default useWebSocket;
