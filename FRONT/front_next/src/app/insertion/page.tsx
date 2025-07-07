"use client";
import { useState } from "react";

export default function InsertionPage() {
  const [file, setFile] = useState<File | null>(null);
  const [result, setResult] = useState<{ status: string; message: string; files?: string[] } | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setFile(e.target.files[0]);
    } else {
      setFile(null);
    }
    setResult(null);
    setError(null);
  };

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!file) return;
    setLoading(true);
    setError(null);
    setResult(null);
    const formData = new FormData();
    formData.append("file", file);
    try {
      const res = await fetch("http://localhost:8000/upload", {
        method: "POST",
        body: formData,
      });
      if (!res.ok) throw new Error("Erreur lors de l'envoi du fichier");
      const data = await res.json();
      setResult(data);
    } catch (err) {
      if (err instanceof Error) {
        setError(err.message);
      } else {
        setError("Erreur inconnue");
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ maxWidth: 600, margin: "auto", padding: 32 }}>
      <h1>Insertion et traitement de données</h1>
      <form onSubmit={handleSubmit} style={{ marginBottom: 24 }}>
        <input type="file" onChange={handleFileChange} accept=".csv,.xlsx" />
        <button type="submit" disabled={loading || !file} style={{ marginLeft: 16 }}>
          {loading ? "Traitement..." : "Envoyer et traiter"}
        </button>
      </form>
      {error && <div style={{ color: "red" }}>{error}</div>}
      {result && (
        <div style={{ background: "#f6f6f6", padding: 16, borderRadius: 8 }}>
          <h3>Résultat :</h3>
          <pre>{JSON.stringify(result, null, 2)}</pre>
        </div>
      )}
    </div>
  );
} 