"use client";
import Link from "next/link";
import { Hero3D } from "@/components/Hero3D";
import { AnimatedText } from "@/components/AnimatedText";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { useState } from "react";
import { useTranslations } from "next-intl";

export default function Home() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const t = useTranslations("home");
  return (
    <div className="min-h-screen relative overflow-hidden bg-background -mt-16">
      {/* Orbes colorées dynamiques en arrière-plan */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none z-0">
        {/* Orbe 1 - Bleu/Violet */}
        <div className="absolute top-1/4 right-1/4 w-96 h-96 bg-gradient-to-r from-blue-400/40 via-purple-500/40 to-pink-500/40 rounded-full blur-3xl animate-pulse dark:from-blue-400/25 dark:via-purple-500/25 dark:to-pink-500/25"
             style={{ animationDelay: '0s', animationDuration: '4s', transform: 'translate(-50%, -50%)' }}></div>
        {/* Orbe 2 - Vert/Cyan */}
        <div className="absolute bottom-1/4 right-1/3 w-80 h-80 bg-gradient-to-r from-green-400/40 via-cyan-500/40 to-blue-500/40 rounded-full blur-3xl animate-pulse dark:from-green-400/25 dark:via-cyan-500/25 dark:to-blue-500/25"
             style={{ animationDelay: '1s', animationDuration: '6s', transform: 'translate(-50%, -50%)' }}></div>
        {/* Orbe 3 - Orange/Rose */}
        <div className="absolute top-1/2 right-1/6 w-72 h-72 bg-gradient-to-r from-orange-400/40 via-red-500/40 to-pink-500/40 rounded-full blur-3xl animate-pulse dark:from-orange-400/25 dark:via-red-500/25 dark:to-pink-500/25"
             style={{ animationDelay: '2s', animationDuration: '5s', transform: 'translate(-50%, -50%)' }}></div>
        {/* Orbe 4 - Violet/Indigo */}
        <div className="absolute bottom-1/3 right-1/5 w-64 h-64 bg-gradient-to-r from-violet-400/40 via-indigo-500/40 to-purple-500/40 rounded-full blur-3xl animate-pulse dark:from-violet-400/25 dark:via-indigo-500/25 dark:to-purple-500/25"
             style={{ animationDelay: '3s', animationDuration: '7s', transform: 'translate(-50%, -50%)' }}></div>
        {/* Orbe 5 - Rose/Magenta */}
        <div className="absolute top-1/3 right-1/8 w-56 h-56 bg-gradient-to-r from-pink-400/40 via-rose-500/40 to-magenta-500/40 rounded-full blur-3xl animate-pulse dark:from-pink-400/25 dark:via-rose-500/25 dark:to-magenta-500/25"
             style={{ animationDelay: '1.5s', animationDuration: '5.5s', transform: 'translate(-50%, -50%)' }}></div>
        {/* Orbe 6 - Jaune/Orange */}
        <div className="absolute bottom-1/5 right-1/4 w-48 h-48 bg-gradient-to-r from-yellow-400/40 via-orange-500/40 to-amber-500/40 rounded-full blur-3xl animate-pulse dark:from-yellow-400/25 dark:via-orange-500/25 dark:to-amber-500/25"
             style={{ animationDelay: '2.5s', animationDuration: '6.5s', transform: 'translate(-50%, -50%)' }}></div>
      </div>

      {/* Conteneur principal en deux colonnes : sphère à gauche, contenu à droite */}
      <div className="relative z-20 flex flex-row min-h-screen w-full">
        {/* Colonne gauche : sphère 3D, cachée sur mobile */}
        <div className="hidden md:flex items-center justify-center w-1/2 h-screen">
          <div className="w-full h-full max-w-full max-h-full opacity-70 relative flex items-center justify-center">
            <Hero3D />
          </div>
        </div>
        {/* Colonne droite : contenu principal, centré sur mobile */}
        <div className="flex items-center justify-center w-full md:w-1/2 h-screen p-0">
          <div className="w-full h-full flex items-center justify-center md:backdrop-blur-xl md:bg-white/60 md:dark:bg-black/40 md:border-l-2 md:border-gray-200 md:dark:border-white/10 md:shadow-2xl">
            <Card className="w-full h-full flex items-center justify-center bg-transparent border-none shadow-none md:bg-transparent md:border-none md:shadow-none md:w-full md:h-full">
              <CardContent className="p-4 md:p-16 text-center w-full h-full flex flex-col items-center justify-center max-w-lg mx-auto md:max-w-full">
                <h1 className="text-3xl md:text-4xl lg:text-5xl font-bold text-gray-900 dark:text-white mb-6">
                  {t("title")}
                </h1>
                <p className="text-base md:text-lg lg:text-xl text-gray-700 dark:text-gray-200 mb-8 leading-relaxed">
                  {t("subtitle")}
                </p>
                <div className="mb-12 flex flex-col items-center w-full">
                  <span className="text-base md:text-lg text-gray-700 dark:text-gray-200 mb-2 block">{t("carousel_prefix")}</span>
                  <div className="w-full flex justify-center items-center py-4">
                    <span className="block w-full">
                      <AnimatedText words={t.raw("carousel")} />
                    </span>
                  </div>
                  <span className="text-base md:text-lg text-gray-700 dark:text-gray-200 mt-2 block">{t("carousel_suffix")}</span>
                </div>
                <div className="flex flex-col sm:flex-row gap-4 justify-center">
                  <Button asChild size="lg" className="bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 dark:from-blue-500 dark:to-purple-500 dark:hover:from-blue-600 dark:hover:to-purple-600 text-white dark:text-white font-semibold transition-all duration-200 hover:scale-105 shadow-lg">
                    <Link href="/statistics">{t("cta_stats")}</Link>
                  </Button>
                  <Button asChild variant="outline" size="lg" className="border-2 border-gray-300 dark:border-white/20 text-gray-900 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-white/5 font-semibold transition-all duration-200 hover:scale-105 shadow-lg">
                    <Link href="/about">{t("cta_about")}</Link>
                  </Button>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
}