import { StatisticsDashboard } from '@/components/Statistics/StatisticsDashboard';

export default function StatistiquesPage() {
  return (
    <main className="min-h-screen p-4 bg-gray-950">
      <h1 className="text-3xl font-bold text-center mb-8 text-white">📊 Statistiques sanitaires par pays</h1>
      <StatisticsDashboard />
    </main>
  );
} 