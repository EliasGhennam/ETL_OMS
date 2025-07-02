"use client";

import Link from "next/link";
import Image from "next/image";
import { ThemeToggle } from "./ThemeToggle";
import { useState, useEffect } from "react";
import { Menu } from "lucide-react";
import { useTheme } from "next-themes";
import Cookies from "js-cookie";
import { useCountryAndLanguage } from "@/lib/useCountryAndLanguage";
import { useLanguage } from "@/context/LanguageContext";
import { CountrySelector } from "@/components/CountrySelector";
import { useTranslations } from "next-intl";

export function Header() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const { theme, systemTheme } = useTheme();
  const [mounted, setMounted] = useState(false);
  const { country } = useCountryAndLanguage();
  const { language } = useLanguage();
  const t = useTranslations("menu");

  useEffect(() => {
    setMounted(true);
  }, []);

  return (
    <header className="sticky top0 z-40 w-full select-none">
      <div className="w-full flex h-12 items-center justify-between px-4 md:px-8">
        {/* -- Section de gauche (Logo) -- */}
        <div className="flex-1 flex justify-start">
          <Link href="/" className="flex items-center space-x-2">
          <div className="relative w-36 h-10">
            <Image
              src="/HealthCheckerLogo.png"
              alt="Logo"
              fill
              className={`object-contain transition-all duration-300${mounted && ((theme === 'system' ? systemTheme : theme) === 'dark') ? ' invert brightness-200' : ''}`}
            />
          </div>
        </Link>
        </div>

        {/* -- Section centrale (Navigation) -- */}
        <nav className="hidden md:flex justify-center">
          <div className="flex gap-6 rounded-2xl bg-white/60 dark:bg-white/10 backdrop-blur-md shadow-[0_0_16px_2px_rgba(255,255,255,0.18)] px-8 py-2 border border-white/30 items-center font-semibold text-base tracking-wide" style={{boxShadow: '0 2px 24px 2px rgba(255,255,255,0.10)'}}>
            {country === 'CH' ? (
              <Link href="/insertion" className="px-3 py-1 rounded-lg transition-all hover:bg-white/80 dark:hover:bg-white/20">{t('inserer')}</Link>
            ) : (
              <Link href="/statistics" className="px-3 py-1 rounded-lg transition-all hover:bg-white/80 dark:hover:bg-white/20">{t('visualiser')}</Link>
            )}
            <Link href="/traiter" className="px-3 py-1 rounded-lg transition-all hover:bg-white/80 dark:hover:bg-white/20">{t('traiter')}</Link>
            <Link href="/prediction" className="px-3 py-1 rounded-lg transition-all hover:bg-white/80 dark:hover:bg-white/20">{t('prediction')}</Link>
            <Link href="/about" className="px-3 py-1 rounded-lg transition-all hover:bg-white/80 dark:hover:bg-white/20">{t('apropos')}</Link>
          </div>
        </nav>

        {/* -- Section de droite (Theme Toggle & Menu mobile) -- */}
        <div className="flex-1 flex justify-end items-center">
          <div className="hidden md:flex items-center">
            <ThemeToggle />
            <CountrySelector />
          </div>
          {/* Bouton menu mobile */}
          <button
            className="md:hidden flex flex-col justify-center items-center h-10 w-10"
            onClick={() => setSidebarOpen(true)}
            aria-label="Ouvrir le menu"
            type="button"
          >
            <Menu className="h-6 w-6" />
          </button>
        </div>
      </div>

      {/* Sidebar mobile */}
      {sidebarOpen && (
        <div className="fixed inset-0 z-50 flex">
          {/* Overlay sombre pour fermer */}
          <div className="fixed inset-0 bg-black/40 backdrop-blur-md" onClick={() => setSidebarOpen(false)} />
          {/* Sidebar */}
          <aside className="relative w-64 max-w-[80vw] h-full bg-white/90 dark:bg-black/80 backdrop-blur-xl shadow-2xl border-r border-white/30 flex flex-col p-6 animate-slide-in-left">
            <button
              className="absolute top-4 right-4 text-gray-900 dark:text-white text-2xl"
              onClick={() => setSidebarOpen(false)}
              aria-label="Fermer le menu"
              type="button"
            >
              ×
            </button>
            <nav className="flex flex-col gap-6 mt-8 font-semibold text-lg">
              {country === 'CH' ? (
                <Link href="/insertion" className="transition-all hover:text-blue-600 dark:hover:text-blue-400" onClick={() => setSidebarOpen(false)}>{t('inserer')}</Link>
              ) : (
                <Link href="/statistics" className="transition-all hover:text-blue-600 dark:hover:text-blue-400" onClick={() => setSidebarOpen(false)}>{t('visualiser')}</Link>
              )}
              <Link href="/traiter" className="transition-all hover:text-blue-600 dark:hover:text-blue-400" onClick={() => setSidebarOpen(false)}>{t('traiter')}</Link>
              <Link href="/prediction" className="transition-all hover:text-blue-600 dark:hover:text-blue-400" onClick={() => setSidebarOpen(false)}>{t('prediction')}</Link>
              <Link href="/about" className="transition-all hover:text-blue-600 dark:hover:text-blue-400" onClick={() => setSidebarOpen(false)}>{t('apropos')}</Link>
            </nav>
            <div className="mt-8 flex items-center gap-2">
              <ThemeToggle />
              <CountrySelector />
            </div>
          </aside>
        </div>
      )}
    </header>
  );
} 