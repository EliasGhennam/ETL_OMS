"use client";

import { Inter } from 'next/font/google'
import './globals.css'
import { ThemeProvider } from '@/components/theme-provider'
import { Header } from '@/components/Header/Header'
import { CookieConsentPopup } from '@/components/CookieConsentPopup'
import { LanguageProvider, LanguageConsumer } from "@/context/LanguageContext";
import { useCountryAndLanguage } from "@/lib/useCountryAndLanguage";
import { CountryLanguageDialog } from "@/components/CountryLanguageDialog";
import { IntlProvider } from 'next-intl';
import fr from '../locales/fr.json';
import en from '../locales/en.json';
import de from '../locales/de.json';
import es from '../locales/es.json';
import ar from '../locales/ar.json';

const inter = Inter({ subsets: ['latin'] })

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <LanguageProvider>
      <LanguageConsumer>{(context) => {
        if (!context) return null;
        const { language } = context;
        return (
          <html lang={language} suppressHydrationWarning>
      <body className={`${inter.className} min-h-screen bg-background`}>
              <IntlProvider key={language} locale={language} messages={{ fr, en, de, es, ar }[language] as Record<string, any>}>
        <ThemeProvider
          attribute="class"
                  defaultTheme="light"
          enableSystem
          disableTransitionOnChange
        >
                  <CookieConsentPopup />
          <div className="flex min-h-screen flex-col pt-8">
            <Header />
            <main className="flex-1">
              {children}
            </main>
          </div>
        </ThemeProvider>
              </IntlProvider>
      </body>
    </html>
        );
      }}</LanguageConsumer>
    </LanguageProvider>
  );
}