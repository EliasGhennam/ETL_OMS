import React from "react";
import Link from "next/link";

export default function AboutPage() {
  return (
    <div className="max-w-3xl mx-auto py-12 px-4 md:px-0">
      <h1 className="text-3xl md:text-4xl font-bold mb-4 text-center">À propos du projet</h1>
      <p className="text-lg text-center mb-8 text-muted-foreground">
        Plateforme ETL & IA pour la prévision pandémique : une solution complète de traitement, visualisation et prédiction des données sanitaires, basée sur une architecture microservices (Python, Java, Next.js) et orchestrée avec Docker.
      </p>
      <div className="mb-10">
        <h2 className="text-2xl font-semibold mb-2">Objectifs</h2>
        <ul className="list-disc pl-6 text-base text-muted-foreground">
          <li>Collecter, nettoyer et stocker des données sanitaires (ETL)</li>
          <li>Visualiser et explorer les statistiques épidémiologiques</li>
          <li>Prédire l'évolution des pandémies grâce à l'IA (LSTM)</li>
          <li>Offrir une interface moderne, multilingue et accessible</li>
        </ul>
      </div>
      <div>
        <h2 className="text-2xl font-semibold mb-4">L'équipe de développement</h2>
        <div className="grid md:grid-cols-2 gap-6">
          <div className="bg-white/80 dark:bg-black/40 rounded-xl p-6 shadow flex flex-col items-center">
            <span className="text-xl font-bold">Elias GHENNAM</span>
            <span className="text-sm text-muted-foreground mb-2">Développeur IA / DATA / BDD / Front</span>
            <div className="flex gap-3">
              <Link href="https://github.com/EliasGhennam" target="_blank" className="underline text-blue-600">GitHub</Link>
              <Link href="https://www.linkedin.com/in/elias-ghennam/" target="_blank" className="underline text-blue-600">LinkedIn</Link>
            </div>
          </div>
          <div className="bg-white/80 dark:bg-black/40 rounded-xl p-6 shadow flex flex-col items-center">
            <span className="text-xl font-bold">Moumine KONE</span>
            <span className="text-sm text-muted-foreground mb-2">Développeur API / Front</span>
            <div className="flex gap-3">
              <Link href="mailto:moumine.kone@ecoles-epsi.net" className="underline text-blue-600">Mail</Link>
              <Link href="https://www.linkedin.com/in/moumine-kone/" target="_blank" className="underline text-blue-600">LinkedIn</Link>
            </div>
          </div>
          <div className="bg-white/80 dark:bg-black/40 rounded-xl p-6 shadow flex flex-col items-center">
            <span className="text-xl font-bold">Adnan MAHBOUBI</span>
            <span className="text-sm text-muted-foreground mb-2">Développeur API Java / BDD / Front</span>
            <div className="flex gap-3">
              <Link href="https://github.com/A2nan" target="_blank" className="underline text-blue-600">GitHub</Link>
              <Link href="https://www.linkedin.com/in/adnan-mahboubi-25359424b/" target="_blank" className="underline text-blue-600">LinkedIn</Link>
            </div>
          </div>
          <div className="bg-white/80 dark:bg-black/40 rounded-xl p-6 shadow flex flex-col items-center">
            <span className="text-xl font-bold">Karim BIH</span>
            <span className="text-sm text-muted-foreground mb-2">Développeur Fullstack</span>
            <div className="flex gap-3">
              <Link href="mailto:karim.bih@ecoles-epsi.net" className="underline text-blue-600">Mail</Link>
              <Link href="https://www.linkedin.com/in/karim-bih-29b079260/" target="_blank" className="underline text-blue-600">LinkedIn</Link>
            </div>
          </div>
        </div>
      </div>
      <div className="mt-10 text-center text-sm text-muted-foreground">
        Projet réalisé dans le cadre de l'EPSI - 2024. <br /> Licence MIT.
      </div>
    </div>
  );
} 