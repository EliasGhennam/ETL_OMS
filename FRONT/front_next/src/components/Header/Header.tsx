"use client";

import Link from "next/link";
import Image from "next/image";
import { ThemeToggle } from "./ThemeToggle";
import {
  Menubar,
  MenubarCheckboxItem,
  MenubarContent,
  MenubarItem,
  MenubarMenu,
  MenubarRadioGroup,
  MenubarRadioItem,
  MenubarSeparator,
  MenubarShortcut,
  MenubarSub,
  MenubarSubContent,
  MenubarSubTrigger,
  MenubarTrigger,
} from "@/components/ui/menubar";

export function Header() {
  return (
    <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="w-full flex h-14 items-center justify-between">
        
        
        <Menubar className="pl-4">
          {/* Navigation Menu */}
          <MenubarMenu>
            <MenubarTrigger>Navigation</MenubarTrigger>
            <MenubarContent>
              <MenubarItem asChild>
                <Link href="/statistics">Statistiques</Link>
              </MenubarItem>
              <MenubarItem asChild>
                <Link href="/about">À propos</Link>
              </MenubarItem>
            </MenubarContent>
          </MenubarMenu>

          {/* Example View Menu - Customize as needed */}
          <MenubarMenu>
            <MenubarTrigger>Affichage</MenubarTrigger>
            <MenubarContent>
              <MenubarCheckboxItem>Activer le mode daltonien</MenubarCheckboxItem>
            </MenubarContent>
          </MenubarMenu>
        </Menubar>

        <Link href="/" className="absolute left-1/2 -translate-x-1/2 flex items-center space-x-2">
          <div className="relative w-40 h-25">
            <Image 
              src="/logo.png" 
              alt="Logo" 
              fill
              className="object-contain"
            />
          </div>
        </Link>

        <div className="flex items-center pr-4">
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
} 