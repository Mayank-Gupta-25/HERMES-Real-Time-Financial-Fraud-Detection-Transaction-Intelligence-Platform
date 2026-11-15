import { useCallback, useEffect, useState } from "react";
import {
  getAlerts,
  getAnomalies,
  getCategories,
  getGeo,
  getOverview,
  getVolume,
  MOCK_FIXTURES,
} from "../services/api.js";

/**
 * Custom hook polling live analytical metrics and rollups from `services/analytics-service` (`MOD-07-FN-01`).
 */
export function useMetrics(range = "24h", refreshIntervalMs = 15000) {
  const [overview, setOverview] = useState(MOCK_FIXTURES.overview);
  const [volume, setVolume] = useState(MOCK_FIXTURES.volume);
  const [categories, setCategories] = useState(MOCK_FIXTURES.categories);
  const [geoPoints, setGeoPoints] = useState(MOCK_FIXTURES.geo);
  const [anomalies, setAnomalies] = useState(MOCK_FIXTURES.anomalies);
  const [alerts, setAlerts] = useState(MOCK_FIXTURES.alerts);
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const [ov, vol, cat, geo, anom, al] = await Promise.all([
        getOverview(range),
        getVolume("5m", range),
        getCategories(range),
        getGeo(range),
        getAnomalies(150, range),
        getAlerts("active", 50),
      ]);
      if (ov) setOverview(ov);
      if (Array.isArray(vol)) setVolume(vol);
      if (Array.isArray(cat)) setCategories(cat);
      if (Array.isArray(geo)) setGeoPoints(geo);
      if (Array.isArray(anom)) setAnomalies(anom);
      if (Array.isArray(al)) setAlerts(al);
    } finally {
      setLoading(false);
    }
  }, [range]);

  useEffect(() => {
    refresh();
    if (!refreshIntervalMs || refreshIntervalMs <= 0) return undefined;
    const timer = setInterval(refresh, refreshIntervalMs);
    return () => clearInterval(timer);
  }, [refresh, refreshIntervalMs]);

  return {
    overview,
    volume,
    categories,
    geoPoints,
    anomalies,
    alerts,
    setAlerts,
    loading,
    refresh,
  };
}

export default useMetrics;
