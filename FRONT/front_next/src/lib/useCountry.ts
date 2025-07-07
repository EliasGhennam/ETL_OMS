import { useEffect, useState } from "react";

export function useCountry() {
  const [country, setCountry] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    fetch("https://ipinfo.io/json")
      .then((res) => res.json())
      .then((data) => {
        if (!cancelled) {
          if (data && data.country) {
            setCountry(data.country);
          } else {
            setCountry(null);
          }
        }
      })
      .catch((err) => {
        if (!cancelled) setError(err);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return { country, loading, error };
} 