import React from "react";
import { Country, Language } from "@/lib/useCountryAndLanguage";
import { useLanguage } from "@/context/LanguageContext";
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "./ui/select";
import { useCountryAndLanguage } from "@/lib/useCountryAndLanguage";

const COUNTRY_LANGUAGE_MAP: Record<Country, Language> = {
  FR: "fr",
  CH: "de",
  US: "en",
  ES: "es",
  DE: "de",
  AR: "ar",
  OTHER: "en",
};

const COUNTRIES: { code: Country; label: string }[] = [
  { code: "FR", label: "France" },
  { code: "CH", label: "Schweiz" },
  { code: "US", label: "United States" },
  { code: "ES", label: "España" },
  { code: "DE", label: "Deutschland" },
  { code: "AR", label: "المملكة العربية السعودية" },
  { code: "OTHER", label: "Other" },
];

export function CountrySelector() {
  const { setLanguage } = useLanguage();
  const { country, language, loading, error } = useCountryAndLanguage();

  const handleChange = (selectedCountry: Country) => {
    localStorage.setItem("country", selectedCountry);
    const lang = COUNTRY_LANGUAGE_MAP[selectedCountry];
    setLanguage(lang);
    localStorage.setItem("language", lang);
    window.location.reload();
  };

  return (
    <Select value={country} onValueChange={handleChange}>
      <SelectTrigger className="ml-2 w-40 text-sm">
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        {COUNTRIES.map((c) => (
          <SelectItem key={c.code} value={c.code}>{c.label}</SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
} 