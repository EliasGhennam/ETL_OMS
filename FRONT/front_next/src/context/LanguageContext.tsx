import React, { createContext, useContext, useState, ReactNode } from "react";
import { Language, useCountryAndLanguage } from "@/lib/useCountryAndLanguage";

interface LanguageContextType {
  language: Language;
  setLanguage: (lang: Language) => void;
}

const LanguageContext = createContext<LanguageContextType | undefined>(undefined);

export function LanguageProvider({ children }: { children: ReactNode }) {
  const { language: initialLanguage, setLanguage: setLang } = useCountryAndLanguage();
  const [language, setLanguageState] = useState<Language>(initialLanguage || "en");

  const setLanguage = (lang: Language) => {
    setLanguageState(lang);
    setLang(lang);
  };

  return (
    <LanguageContext.Provider value={{ language, setLanguage }}>
      {children}
    </LanguageContext.Provider>
  );
}

export function useLanguage() {
  const context = useContext(LanguageContext);
  if (!context) throw new Error("useLanguage must be used within a LanguageProvider");
  return context;
}

export const LanguageConsumer = LanguageContext.Consumer; 