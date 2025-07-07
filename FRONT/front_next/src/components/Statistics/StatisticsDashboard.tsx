"use client";

import { useEffect, useState } from "react";
import { TrendingUp } from "lucide-react";
import { Area, AreaChart, CartesianGrid, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  ChartConfig,
  ChartTooltipContent,
} from "@/components/ui/chart";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useTranslations } from "next-intl";

interface DataPoint {
  date: string;
  valeur: number;
}

interface CombinedDataPoint {
  date: string;
  nouveau_mort: number;
  nouveau_cas: number;
}

// Fonction pour calculer la moyenne mobile
const calculateMovingAverage = (data: DataPoint[], windowSize: number): DataPoint[] => {
  if (windowSize <= 1 || data.length === 0) {
    return data;
  }

  return data.map((_d, index) => {
    const start = Math.max(0, index - Math.floor(windowSize / 2));
    const end = Math.min(data.length - 1, index + Math.ceil(windowSize / 2) - 1);
    
    let sum = 0;
    let count = 0;
    for (let i = start; i <= end; i++) {
      sum += data[i].valeur;
      count++;
    }
    
    return {
      date: data[index].date,
      valeur: sum / count,
    };
  });
};

const API_BASE = "http://localhost:8080/api";

export function StatisticsDashboard() {
  const [pays, setPays] = useState<{ id_pays: string; nom_pays: string }[]>([]);
  const [maladies, setMaladies] = useState<{ id_maladie: string; nom_maladie: string }[]>([]);
  const [selectedPays, setSelectedPays] = useState<string>("");
  const [selectedMaladie, setSelectedMaladie] = useState<string>("");
  const [combinedNouveauData, setCombinedNouveauData] = useState<CombinedDataPoint[]>([]);
  const [totalMortData, setTotalMortData] = useState<DataPoint[]>([]);
  const [totalCasData, setTotalCasData] = useState<DataPoint[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const t = useTranslations("statistics");

  useEffect(() => {
    fetchPays();
    fetchMaladies();
  }, []);

  useEffect(() => {
    if (selectedPays && selectedMaladie) {
      fetchAllChartData();
    }
  }, [selectedPays, selectedMaladie, fetchAllChartData]);

  // Ajout d'un useEffect pour suivre les changements de l'état des données
  useEffect(() => {
    console.log("Combined Nouveau Data state changed:", combinedNouveauData);
    if (combinedNouveauData.length > 0) {
      console.log("Combined Nouveau Data is not empty, chart should render.");
    }
  }, [combinedNouveauData]);

  useEffect(() => {
    console.log("Total Mort Data state changed:", totalMortData);
    if (totalMortData.length > 0) {
      console.log("Total Mort Data is not empty, chart should render.");
    }
  }, [totalMortData]);

  useEffect(() => {
    console.log("Total Cas Data state changed:", totalCasData);
    if (totalCasData.length > 0) {
      console.log("Total Cas Data is not empty, chart should render.");
    }
  }, [totalCasData]);

  async function fetchPays() {
    const res = await fetch(`${API_BASE}/pays`);
    const data = await res.json();
    const filtered = data.filter((p: { nom_pays: string }) => p.nom_pays !== "Inconnue");
    setPays(filtered);
    if (filtered.length > 0) setSelectedPays(filtered[0].id_pays);
  }

  async function fetchMaladies() {
    const res = await fetch(`${API_BASE}/maladies`);
    const data = await res.json();
    const filtered = data.filter((m: { nom_maladie: string }) => m.nom_maladie !== "Inconnue");
    setMaladies(filtered);
    if (filtered.length > 0) setSelectedMaladie(filtered[0].id_maladie);
  }

  async function fetchAllChartData() {
    setIsLoading(true);
    try {
      const [nouveauMortRes, nouveauCasRes, totalMortRes, totalCasRes] = await Promise.all([
        fetch(`${API_BASE}/statistiques/donnees-par-jour?paysId=${selectedPays}&maladieId=${selectedMaladie}&type=nouveau_mort`),
        fetch(`${API_BASE}/statistiques/donnees-par-jour?paysId=${selectedPays}&maladieId=${selectedMaladie}&type=nouveau_cas`),
        fetch(`${API_BASE}/statistiques/donnees-par-jour?paysId=${selectedPays}&maladieId=${selectedMaladie}&type=total_mort`),
        fetch(`${API_BASE}/statistiques/donnees-par-jour?paysId=${selectedPays}&maladieId=${selectedMaladie}&type=total_cas`),
      ]);

      const rawNouveauMortData = await nouveauMortRes.json();
      const rawNouveauCasData = await nouveauCasRes.json();
      const rawTotalMortData = await totalMortRes.json();
      const rawTotalCasData = await totalCasRes.json();

      // Process and smooth Nouveau Mort data
      const processedNouveauMortData = rawNouveauMortData.map((d: { date: string; valeur: string | number }) => ({
        date: d.date,
        valeur: Number(d.valeur),
      }));
      const dynamicWindowSizeNouveauMort = Math.max(2, Math.min(10, Math.floor(processedNouveauMortData.length * 0.05)));
      const smoothedNouveauMortData = calculateMovingAverage(processedNouveauMortData, dynamicWindowSizeNouveauMort);

      // Process and smooth Nouveau Cas data
      const processedNouveauCasData = rawNouveauCasData.map((d: { date: string; valeur: string | number }) => ({
        date: d.date,
        valeur: Number(d.valeur),
      }));
      const dynamicWindowSizeNouveauCas = Math.max(2, Math.min(10, Math.floor(processedNouveauCasData.length * 0.05)));
      const smoothedNouveauCasData = calculateMovingAverage(processedNouveauCasData, dynamicWindowSizeNouveauCas);

      // Process and smooth Total Mort data
      const processedTotalMortData = rawTotalMortData.map((d: { date: string; valeur: string | number }) => ({
        date: d.date,
        valeur: Number(d.valeur),
      }));
      const dynamicWindowSizeTotalMort = Math.max(2, Math.min(10, Math.floor(processedTotalMortData.length * 0.05)));
      const smoothedTotalMortData = calculateMovingAverage(processedTotalMortData, dynamicWindowSizeTotalMort);
      setTotalMortData(smoothedTotalMortData);

      // Process and smooth Total Cas data
      const processedTotalCasData = rawTotalCasData.map((d: { date: string; valeur: string | number }) => ({
        date: d.date,
        valeur: Number(d.valeur),
      }));
      const dynamicWindowSizeTotalCas = Math.max(2, Math.min(10, Math.floor(processedTotalCasData.length * 0.05)));
      const smoothedTotalCasData = calculateMovingAverage(processedTotalCasData, dynamicWindowSizeTotalCas);
      setTotalCasData(smoothedTotalCasData);

      // Combine Nouveau Mort and Nouveau Cas data for the main chart
      const allDates = new Set<string>();
      smoothedNouveauMortData.forEach(d => allDates.add(d.date));
      smoothedNouveauCasData.forEach(d => allDates.add(d.date));

      const combinedData: CombinedDataPoint[] = Array.from(allDates).map(date => {
        const nouveauMortEntry = smoothedNouveauMortData.find(d => d.date === date);
        const nouveauCasEntry = smoothedNouveauCasData.find(d => d.date === date);

        return {
          date: date,
          nouveau_mort: nouveauMortEntry ? nouveauMortEntry.valeur : 0,
          nouveau_cas: nouveauCasEntry ? nouveauCasEntry.valeur : 0,
        };
      }).sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());

      setCombinedNouveauData(combinedData);

    } catch (error) {
      console.error('Error fetching all chart data:', error);
    } finally {
      setIsLoading(false);
    }
  }

  const chartConfig = {
    nouveau_mort: {
      label: "Nouveaux morts",
      color: "hsl(var(--chart-1))",
    },
    nouveau_cas: {
      label: "Nouveaux cas",
      color: "hsl(var(--chart-2))",
    },
    total_mort: {
      label: "Total des morts",
      color: "hsl(var(--chart-3))",
    },
    total_cas: {
      label: "Total des cas",
      color: "hsl(var(--chart-4))",
    },
  } satisfies ChartConfig;

  return (
    <div className="container mx-auto p-4 space-y-8">
      {/* Filter Section */}
      <Card>
        <CardHeader>
          <CardTitle>{t("filter_title")}</CardTitle>
          <CardDescription>{t("filter_description")}</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">{t("country_label")}</label>
              <Select value={selectedPays} onValueChange={setSelectedPays}>
                <SelectTrigger>
                  <SelectValue placeholder={t("country_placeholder")} />
                </SelectTrigger>
                <SelectContent>
                  {pays.map((p) => (
                    <SelectItem key={p.id_pays} value={p.id_pays}>
                      {p.nom_pays}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">{t("disease_label")}</label>
              <Select value={selectedMaladie} onValueChange={setSelectedMaladie}>
                <SelectTrigger>
                  <SelectValue placeholder={t("disease_placeholder")} />
                </SelectTrigger>
                <SelectContent>
                  {maladies.map((m) => (
                    <SelectItem key={m.id_maladie} value={m.id_maladie}>
                      {m.nom_maladie}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Chart Section */}
      {isLoading ? (
        <div className="flex items-center justify-center h-[400px]">
          <div className="text-muted-foreground">{t("loading")}</div>
        </div>
      ) : combinedNouveauData.length === 0 && totalMortData.length === 0 && totalCasData.length === 0 ? (
        <div className="flex items-center justify-center h-[400px] text-muted-foreground">
          {t("no_data")}
        </div>
      ) : (
        <div className="grid gap-8">
          {/* Main Chart: Nouveau Morts and Nouveau Cas */}
          <Card className="w-full">
            <CardHeader>
              <CardTitle>{t("main_chart_title")}</CardTitle>
              <CardDescription>{t("main_chart_description")}</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="h-[400px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart
                    accessibilityLayer
                    width={700}
                    height={400}
                    data={combinedNouveauData}
                    margin={{
                      left: 12,
                      right: 12,
                    }}
                  >
                    <CartesianGrid vertical={false} />
                    <XAxis
                      dataKey="date"
                      tickLine={false}
                      axisLine={false}
                      tickMargin={8}
                      tickFormatter={(value) => value.slice(0, 7)}
                    />
                    <YAxis
                      domain={[0, 'auto']}
                      tickLine={false}
                      axisLine={false}
                      tickMargin={8}
                      tickFormatter={(value) => value.toLocaleString()}
                    />
                    <Tooltip content={<ChartTooltipContent config={chartConfig} />} />
                    <defs>
                      <linearGradient id="fillNouveauMort" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#e88f2b" stopOpacity={0.8} />
                        <stop offset="95%" stopColor="#e88f2b" stopOpacity={0.1} />
                      </linearGradient>
                      <linearGradient id="fillNouveauCas" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#00BFFF" stopOpacity={0.8} />
                        <stop offset="95%" stopColor="#00BFFF" stopOpacity={0.1} />
                      </linearGradient>
                    </defs>
                    <Area
                      dataKey="nouveau_mort"
                      type="natural"
                      fill="url(#fillNouveauMort)"
                      fillOpacity={0.4}
                      stroke="#e88f2b"
                    />
                    <Area
                      dataKey="nouveau_cas"
                      type="natural"
                      fill="url(#fillNouveauCas)"
                      fillOpacity={0.4}
                      stroke="#00BFFF"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>

          <div className="grid gap-8 md:grid-cols-2">
            {/* Total Morts Chart */}
            <Card>
              <CardHeader>
                <CardTitle>{t("total_mort_title")}</CardTitle>
                <CardDescription>{t("total_mort_description")}</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-[250px] w-full">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart
                      accessibilityLayer
                      width={350}
                      height={250}
                      data={totalMortData}
                      margin={{
                        left: 12,
                        right: 12,
                      }}
                    >
                      <CartesianGrid vertical={false} />
                      <XAxis
                        dataKey="date"
                        tickLine={false}
                        axisLine={false}
                        tickMargin={8}
                        tickFormatter={(value) => value.slice(0, 7)}
                      />
                      <YAxis
                        domain={[0, 'auto']}
                        tickLine={false}
                        axisLine={false}
                        tickMargin={8}
                        tickFormatter={(value) => value.toLocaleString()}
                      />
                      <Tooltip content={<ChartTooltipContent config={chartConfig} />} />
                      <defs>
                        <linearGradient id="fillTotalMort" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#00BFFF" stopOpacity={0.7} />
                          <stop offset="95%" stopOpacity={0.1} />
                        </linearGradient>
                      </defs>
                      <Area
                        dataKey="valeur"
                        type="natural"
                        fill="url(#fillTotalMort)"
                        fillOpacity={0.4}
                        stroke="#00BFFF"
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>

            {/* Total Cas Chart */}
            <Card>
              <CardHeader>
                <CardTitle>{t("total_cas_title")}</CardTitle>
                <CardDescription>{t("total_cas_description")}</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-[250px] w-full">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart
                      accessibilityLayer
                      width={350}
                      height={250}
                      data={totalCasData}
                      margin={{
                        left: 12,
                        right: 12,
                      }}
                    >
                      <CartesianGrid vertical={false} />
                      <XAxis
                        dataKey="date"
                        tickLine={false}
                        axisLine={false}
                        tickMargin={8}
                        tickFormatter={(value) => value.slice(0, 7)}
                      />
                      <YAxis
                        domain={[0, 'auto']}
                        tickLine={false}
                        axisLine={false}
                        tickMargin={8}
                        tickFormatter={(value) => value.toLocaleString()}
                      />
                      <Tooltip content={<ChartTooltipContent config={chartConfig} />} />
                      <defs>
                        <linearGradient id="fillTotalCas" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#00BFFF" stopOpacity={0.7} />
                          <stop offset="95%" stopOpacity={0.1} />
                        </linearGradient>
                      </defs>
                      <Area
                        dataKey="valeur"
                        type="natural"
                        fill="url(#fillTotalCas)"
                        fillOpacity={0.4}
                        stroke="#00BFFF"
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      )}

      {/* Footer */}
      <CardFooter>
        <div className="flex w-full items-start gap-2 text-sm">
          <div className="grid gap-2">
            <div className="flex items-center gap-2 leading-none font-medium">
              {t("footer_realtime")} <TrendingUp className="h-4 w-4" />
            </div>
            <div className="text-muted-foreground flex items-center gap-2 leading-none">
              {t("footer_daily_update")}
            </div>
          </div>
        </div>
      </CardFooter>
    </div>
  );
}