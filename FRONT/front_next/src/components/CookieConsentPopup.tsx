import React, { useEffect, useState } from "react";
import { Button } from "./ui/button";
import Cookies from "js-cookie";

const COOKIE_KEY = "cookie_consent_accepted";

export function CookieConsentPopup() {
  const [show, setShow] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (typeof window === "undefined") return;
    // Si déjà accepté (cookie), ne pas afficher
    if (Cookies.get(COOKIE_KEY) === "true") {
      setShow(false);
      setLoading(false);
      return;
    }
    // Géolocalisation par IP
    fetch("https://ipapi.co/json/")
      .then((res) => res.json())
      .then((data) => {
        if (data && data.country_code === "FR") {
          setShow(true);
        }
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const acceptCookies = () => {
    // Expire dans 12 mois
    Cookies.set(COOKIE_KEY, "true", { expires: 365 });
    setShow(false);
  };

  if (!show || loading) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center pointer-events-none">
      <div className="mb-8 w-full max-w-md rounded-lg bg-card shadow-lg border border-border p-6 pointer-events-auto flex flex-col gap-4 animate-fade-in">
        <div className="text-lg font-semibold">Consentement aux cookies</div>
        <div className="text-sm text-muted-foreground">
          Ce site utilise des cookies pour améliorer votre expérience. En continuant, vous acceptez notre <a href="/conditions" target="_blank" rel="noopener noreferrer" className="underline text-primary hover:text-primary/80">politique de confidentialité et conditions d'utilisation</a>.
        </div>
        <div className="flex justify-end">
          <Button onClick={acceptCookies} variant="default">
            J'accepte
          </Button>
        </div>
      </div>
    </div>
  );
} 