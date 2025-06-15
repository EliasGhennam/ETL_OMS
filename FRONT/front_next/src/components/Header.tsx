"use client";

import Link from "next/link";
import { ThemeToggle } from "./ThemeToggle";

export function Header() {
  return (
    <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container flex h-14 items-center">
        <div className="mr-4 flex">
          <Link href="/" className="mr-6 flex items-center space-x-2">
            <span className="font-bold">ETL Project</span>
          </Link>
          <nav className="flex items-center space-x-6 text-sm font-medium">
            <Link href="/statistics" className="transition-colors hover:text-foreground/80">
              Statistiques
            </Link>
            <Link href="/about" className="transition-colors hover:text-foreground/80">
              À propos
            </Link>
          </nav>
        </div>
        <div className="flex flex-1 items-center justify-end space-x-2">
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
} 