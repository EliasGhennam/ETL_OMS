'use client';

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';

import { Bar } from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
);

export const options = {
  responsive: true,
  plugins: {
    legend: {
      position: 'top' as const,
    },
    title: {
      display: true,
      text: 'Statistiques ETL',
      color: 'var(--foreground)'
    },
  },
  scales: {
    y: {
      ticks: { color: 'var(--foreground)' },
      grid: { color: 'var(--border)' }
    },
    x: {
      ticks: { color: 'var(--foreground)' },
      grid: { color: 'var(--border)' }
    }
  }
};

export function StatisticsChart() {
  const data = {
    labels: ['Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin'],
    datasets: [
      {
        label: 'Données traitées',
        data: [65, 59, 80, 81, 56, 55],
        backgroundColor: 'rgba(255, 99, 132, 0.5)',
      }
    ],
  };

  return (
    <div className="w-full max-w-4xl mx-auto p-6">
      <Bar options={options} data={data} />
    </div>
  );
}