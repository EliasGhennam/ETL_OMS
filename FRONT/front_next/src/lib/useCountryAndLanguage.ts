import { useEffect, useState } from "react";

export type Country = "FR" | "CH" | "US" | "ES" | "DE" | "AR" | "OTHER";
export type Language = "fr" | "en" | "de" | "es" | "ar";

const COUNTRY_LANGUAGE_MAP: Record<Country, Language> = {
  FR: "fr",
  CH: "de", // par défaut allemand pour la Suisse
  US: "en",
  ES: "es",
  DE: "de",
  AR: "ar",
  OTHER: "en",
};

function getStoredCountry(): Country | null {
  if (typeof window === "undefined") return null;
  return (localStorage.getItem("country") as Country) || null;
}
function getStoredLanguage(): Language | null {
  if (typeof window === "undefined") return null;
  return (localStorage.getItem("language") as Language) || null;
}

export function useCountryAndLanguage() {
  const [country, setCountry] = useState<Country | null>(getStoredCountry());
  const [language, setLanguage] = useState<Language | null>(getStoredLanguage());
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const [showDialog, setShowDialog] = useState(false);

  useEffect(() => {
    if (typeof window !== "undefined") {
      const savedCountry = localStorage.getItem("country") as Country;
      const savedLanguage = localStorage.getItem("language") as Language;
      
      if (savedCountry) setCountry(savedCountry);
      if (savedLanguage) setLanguage(savedLanguage);
    }
  }, []);

  useEffect(() => {
    if (country && language) {
      setLoading(false);
      return;
    }
    fetch("https://ipinfo.io/json")
      .then((res) => {
        if (!res.ok) throw new Error("API error");
        return res.json();
      })
      .then((data) => {
        let detected: Country = "OTHER";
        switch (data.country) {
          case "FR": detected = "FR"; break;
          case "CH": detected = "CH"; break;
          case "US": detected = "US"; break;
          case "ES": detected = "ES"; break;
          case "DE": detected = "DE"; break;
          case "AR": detected = "AR"; break;
          default: detected = "OTHER";
        }
        setCountry(detected);
        setLanguage(COUNTRY_LANGUAGE_MAP[detected]);
        localStorage.setItem("country", detected);
        localStorage.setItem("language", COUNTRY_LANGUAGE_MAP[detected]);
        setLoading(false);
      })
      .catch((err) => {
        setError(err);
        setShowDialog(true);
        setLoading(false);
      });
  }, [country, language]);

  // Quand l'utilisateur choisit manuellement
  const manualSet = (c: Country, l: Language) => {
    setCountry(c);
    setLanguage(l);
    localStorage.setItem("country", c);
    localStorage.setItem("language", l);
    setShowDialog(false);
    window.location.reload(); // Force reload for full reactivity
  };

  return { country, language, loading, error, setCountry, setLanguage, showDialog, setShowDialog, manualSet };
} 