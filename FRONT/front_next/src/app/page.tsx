import Link from "next/link";
import { Hero3D } from "@/components/Hero3D";
import { Button } from "@/components/ui/button";

export default function Home() {
  return (
    <div className="flex w-full min-h-screen">
      {/* Container 1: Hero3D and welcome text (Left) */}
      <div className="flex flex-col items-center justify-center space-y-8 w-1/2 p-4">
        <div className="text-center">
          <h1 className="text-4xl font-bold mb-4">Bienvenue sur notre outil de visualisation des données épidémiologiques</h1>
          <p className="text-lg text-muted-foreground">
            Visualisez et analysez vos données en temps réel !
          </p>
        </div>
        <div className="relative w-full h-[600px] overflow-hidden">
          <Hero3D />
        </div>
      </div>

      {/* Container 2: Navigation Buttons (Right) */}
      <div className="flex flex-col items-center justify-center space-y-8 w-1/2 p-4 bg-secondary/10">
        <h2 className="text-4xl font-bold mb-4">Explorez nos données</h2>
        <p className="text-lg text-muted-foreground text-center mb-6">
          Plongez au cœur des statistiques épidémiologiques mondiales.
        </p>
        <div className="flex space-x-4">
          <Button asChild size="lg">
            <Link href="/statistics">Voir les Statistiques</Link>
          </Button>
          <Button asChild variant="outline" size="lg">
            <Link href="/about">À propos</Link>
          </Button>
          {/* Add more buttons as needed */}
        </div>
      </div>
    </div>
  );
}