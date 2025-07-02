import React, { useState } from "react";
import { Country, Language } from "@/lib/useCountryAndLanguage";

const COUNTRIES: { code: Country; label: string }[] = [
  { code: "FR", label: "France" },
  { code: "CH", label: "Suisse" },
  { code: "US", label: "États-Unis" },
  { code: "ES", label: "Espagne" },
  { code: "DE", label: "Allemagne" },
  { code: "AR", label: "Arabie Saoudite" },
  { code: "OTHER", label: "Autre" },
];
const LANGUAGES: { code: Language; label: string }[] = [
  { code: "fr", label: "Français" },
  { code: "en", label: "English" },
  { code: "de", label: "Deutsch" },
  { code: "es", label: "Español" },
  { code: "ar", label: "العربية" },
];

export function CountryLanguageDialog({ show, onValidate }: { show: boolean; onValidate: (country: Country, language: Language) => void }) {
  const [country, setCountry] = useState<Country>("FR");
  const [language, setLanguage] = useState<Language>("fr");
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!country || !language) {
      setError("Veuillez sélectionner un pays et une langue.");
      return;
    }
    setError(null);
    onValidate(country, language);
  };

  if (!show) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
      <form onSubmit={handleSubmit} className="bg-white dark:bg-black p-8 rounded-xl shadow-xl border w-full max-w-md flex flex-col gap-6">
        <h2 className="text-xl font-bold text-center">Sélectionnez votre pays et votre langue</h2>
        <div className="flex flex-col gap-4">
          <label className="font-semibold">Pays :</label>
          <select value={country} onChange={e => setCountry(e.target.value as Country)} className="p-2 rounded border">
            {COUNTRIES.map(c => <option key={c.code} value={c.code}>{c.label}</option>)}
          </select>
        </div>
        <div className="flex flex-col gap-4">
          <label className="font-semibold">Langue :</label>
          <select value={language} onChange={e => setLanguage(e.target.value as Language)} className="p-2 rounded border">
            {LANGUAGES.map(l => <option key={l.code} value={l.code}>{l.label}</option>)}
          </select>
        </div>
        {error && <div className="text-red-600 text-center">{error}</div>}
        <button type="submit" className="bg-primary text-white rounded p-2 font-semibold">Valider</button>
      </form>
    </div>
  );
} 