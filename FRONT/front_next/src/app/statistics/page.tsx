'use client';
import { StatisticsDashboard } from "@/components/Statistics/StatisticsDashboard";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import React from "react";
import { useCountryAndLanguage } from "@/lib/useCountryAndLanguage";
import { useLanguage } from "@/context/LanguageContext";
import { useTranslations } from "next-intl";

export default function StatisticsPage() {
  const { country, loading } = useCountryAndLanguage();
  const t = useTranslations("statistics");

  if (loading) return null;
  if (country === "CH") {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="bg-white/80 dark:bg-black/80 p-8 rounded-xl shadow-xl border text-center max-w-lg">
          <h2 className="text-2xl font-bold mb-4">{t("forbiddenTitle")}</h2>
          <p className="text-lg">{t("forbidden")}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen relative overflow-hidden bg-background -mt-16">
      <div className="absolute inset-0 overflow-hidden pointer-events-none z-0">
        <div className="absolute top-1/4 right-1/4 w-96 h-96 bg-gradient-to-r from-blue-400/40 via-purple-500/40 to-pink-500/40 rounded-full blur-3xl animate-pulse dark:from-blue-400/25 dark:via-purple-500/25 dark:to-pink-500/25"
             style={{ animationDelay: '0s', animationDuration: '4s', transform: 'translate(-50%, -50%)' }}></div>
        <div className="absolute bottom-1/4 right-1/3 w-80 h-80 bg-gradient-to-r from-green-400/40 via-cyan-500/40 to-blue-500/40 rounded-full blur-3xl animate-pulse dark:from-green-400/25 dark:via-cyan-500/25 dark:to-blue-500/25"
             style={{ animationDelay: '1s', animationDuration: '6s', transform: 'translate(-50%, -50%)' }}></div>
        <div className="absolute top-1/2 right-1/6 w-72 h-72 bg-gradient-to-r from-orange-400/40 via-red-500/40 to-pink-500/40 rounded-full blur-3xl animate-pulse dark:from-orange-400/25 dark:via-red-500/25 dark:to-pink-500/25"
             style={{ animationDelay: '2s', animationDuration: '5s', transform: 'translate(-50%, -50%)' }}></div>
        <div className="absolute bottom-1/3 right-1/5 w-64 h-64 bg-gradient-to-r from-violet-400/40 via-indigo-500/40 to-purple-500/40 rounded-full blur-3xl animate-pulse dark:from-violet-400/25 dark:via-indigo-500/25 dark:to-purple-500/25"
             style={{ animationDelay: '3s', animationDuration: '7s', transform: 'translate(-50%, -50%)' }}></div>
        <div className="absolute top-1/3 right-1/8 w-56 h-56 bg-gradient-to-r from-pink-400/40 via-rose-500/40 to-magenta-500/40 rounded-full blur-3xl animate-pulse dark:from-pink-400/25 dark:via-rose-500/25 dark:to-magenta-500/25"
             style={{ animationDelay: '1.5s', animationDuration: '5.5s', transform: 'translate(-50%, -50%)' }}></div>
        <div className="absolute bottom-1/5 right-1/4 w-48 h-48 bg-gradient-to-r from-yellow-400/40 via-orange-500/40 to-amber-500/40 rounded-full blur-3xl animate-pulse dark:from-yellow-400/25 dark:via-orange-500/25 dark:to-amber-500/25"
             style={{ animationDelay: '2.5s', animationDuration: '6.5s', transform: 'translate(-50%, -50%)' }}></div>
      </div>
      
      <div className="relative z-10 container mx-auto px-4 py-12 md:py-24">
         <Card className="w-full backdrop-blur-xl bg-white/60 dark:bg-white/10 shadow-xl border-white/30 dark:border-white/20">
            <CardHeader className="text-center">
              <CardTitle className="text-2xl md:text-3xl font-bold">{t("title")}</CardTitle>
              <CardDescription>{t("description")}</CardDescription>
            </CardHeader>
            <CardContent className="p-4 md:p-6">
              <StatisticsDashboard />
            </CardContent>
          </Card>
      </div>
    </div>
  );
} 