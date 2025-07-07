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
        <p className="text-sm text-muted-foreground">
          Nous utilisons des cookies pour améliorer votre expérience sur notre site. En continuant à naviguer, vous acceptez notre utilisation des cookies. Pour plus d&apos;informations, consultez notre <a href="/conditions" className="underline text-blue-600 hover:text-blue-800" target="_blank" rel="noopener noreferrer">politique de confidentialité et conditions d&apos;utilisation</a>.
        </p>
        <div className="flex justify-end">
          <Button onClick={acceptCookies} variant="default">
            J&apos;accepte
          </Button>
        </div>
      </div>
    </div>
  );
} 