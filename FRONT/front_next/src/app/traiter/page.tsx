"use client";
import { useState, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import type { ChangeEvent, DragEvent, FormEvent } from "react";
import { Trash2, UploadCloud, File as FileIcon, Loader2 } from "lucide-react";
import { useTranslations } from "next-intl";

export default function TraiterPage() {
  const [files, setFiles] = useState<File[]>([]);
  const [logs, setLogs] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const eventSourceRef = useRef<EventSource | null>(null);
  const t = useTranslations("traiter");

  const handleRemoveFile = (indexToRemove: number) => {
    setFiles(prevFiles => prevFiles.filter((_, index) => index !== indexToRemove));
  };
  
  const handleDrag = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") setDragActive(true);
    else if (e.type === "dragleave") setDragActive(false);
  };

  const handleDrop = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    const droppedFiles = e.dataTransfer.files;
    if (droppedFiles && droppedFiles.length > 0) {
      setFiles(prevFiles => [...prevFiles, ...Array.from(droppedFiles)]);
      setError(null);
    }
  };

  const handleFileChange = (e: ChangeEvent<HTMLInputElement>) => {
    const selectedFiles = e.target.files;
    if (selectedFiles && selectedFiles.length > 0) {
      setFiles(prevFiles => [...prevFiles, ...Array.from(selectedFiles)]);
      setError(null);
    }
  };

  const triggerInput = () => inputRef.current?.click();

  const handleUploadAndEtl = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!files.length) return;
    setLoading(true);
    setError(null);
    setLogs("");

    const formData = new FormData();
    files.forEach(file => formData.append('files', file));
    
    try {
      const res = await fetch("http://127.0.0.1:5000/upload", { method: "POST", body: formData });
      if (!res.ok) throw new Error((await res.json()).message || "Erreur lors de l'upload");
      
      const es = new window.EventSource("http://127.0.0.1:5000/upload/progress");
      eventSourceRef.current = es;
      es.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          setLogs(data.logs.replace(/\\n/g, "\n"));
          if (data.done) {
            es.close();
            setLoading(false);
          }
        } catch {
            es.close();
            setLoading(false);
            setError("Erreur de parsing des logs.");
        }
      };
      es.onerror = () => {
        es.close();
        setLoading(false);
        setError("Erreur de connexion pour les logs en direct.");
      };
    } catch (err: any) {
      setError(err.message || "Erreur inconnue");
      setLoading(false);
    }
  };
  
  const handlePing = async (e: FormEvent<HTMLButtonElement>) => {
    e.preventDefault();
    try {
      const res = await fetch("http://127.0.0.1:5000/ping");
      const data = await res.json();
      setLogs(JSON.stringify(data, null, 2));
    } catch (err) {
      setError("API injoignable");
    }
  };

  return (
    <div className="min-h-screen relative overflow-hidden bg-background -mt-16">
      <div className="absolute inset-0 overflow-hidden pointer-events-none z-0">
        <div className="absolute top-1/4 right-1/4 w-96 h-96 bg-gradient-to-r from-blue-400/40 via-purple-500/40 to-pink-500/40 rounded-full blur-3xl animate-pulse dark:from-blue-400/25 dark:via-purple-500/25 dark:to-pink-500/25"
             style={{ animationDelay: '0s', animationDuration: '4s', transform: 'translate(-50%, -50%)' }}></div>
        <div className="absolute bottom-1/4 right-1/3 w-80 h-80 bg-gradient-to-r from-green-400/40 via-cyan-500/40 to-blue-500/40 rounded-full blur-3xl animate-pulse dark:from-green-400/25 dark:via-cyan-500/25 dark:to-blue-500/25"
             style={{ animationDelay: '1s', animationDuration: '6s', transform: 'translate(-50%, -50%)' }}></div>
        <div className="absolute top-1/2 right-1/6 w-72 h-72 bg-gradient-to-r from-orange-400/40 via-red-500/40 to-pink-500/40 rounded-full blur-3xl animate-pulse dark:from-orange-400/25 dark:via-red-500/25 dark:to-pink-500/25"
             style={{ animationDelay: '2s', animationDuration: '5s', transform: 'translate(-50%, -50%)' }}></div>
        <div className="absolute bottom-1/3 right-1/5 w-64 h-64 bg-gradient-to-r from-violet-400/40 via-indigo-500/40 to-purple-500/40 rounded-full blur-3xl animate-pulse dark:from-violet-400/25 dark:via-indigo-500/25 dark:to-purple-500/25"
             style={{ animationDelay: '3s', animationDuration: '7s', transform: 'translate(-50%, -50%)' }}></div>
        <div className="absolute top-1/3 right-1/8 w-56 h-56 bg-gradient-to-r from-pink-400/40 via-rose-500/40 to-magenta-500/40 rounded-full blur-3xl animate-pulse dark:from-pink-400/25 dark:via-rose-500/25 dark:to-magenta-500/25"
             style={{ animationDelay: '1.5s', animationDuration: '5.5s', transform: 'translate(-50%, -50%)' }}></div>
        <div className="absolute bottom-1/5 right-1/4 w-48 h-48 bg-gradient-to-r from-yellow-400/40 via-orange-500/40 to-amber-500/40 rounded-full blur-3xl animate-pulse dark:from-yellow-400/25 dark:via-orange-500/25 dark:to-amber-500/25"
             style={{ animationDelay: '2.5s', animationDuration: '6.5s', transform: 'translate(-50%, -50%)' }}></div>
      </div>

      <div className="relative z-10 max-w-2xl mx-auto flex flex-col items-center justify-center min-h-screen p-4 md:p-12">
        <form onSubmit={handleUploadAndEtl} className="w-full">
          <Card className="w-full backdrop-blur-xl bg-white/60 dark:bg-white/10 shadow-xl border-white/30 dark:border-white/20">
            <CardHeader className="text-center">
              <CardTitle className="text-2xl md:text-3xl font-bold">{t("title")}</CardTitle>
              <CardDescription>{t("description")}</CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div
                className={`group relative border-2 border-dashed rounded-lg p-8 text-center transition-colors cursor-pointer
                  ${dragActive ? "border-primary" : "border-border"}
                  ${files.length === 0 ? "hover:border-primary/60" : ""}`}
                onClick={triggerInput}
                onDragEnter={handleDrag}
                onDragLeave={handleDrag}
                onDragOver={handleDrag}
                onDrop={handleDrop}
              >
                <div className="flex flex-col items-center justify-center space-y-4">
                  <UploadCloud className={`h-12 w-12 text-muted-foreground transition-transform group-hover:scale-110 ${dragActive ? "text-primary" : ""}`} />
                  <p className="text-muted-foreground">{t("dropzone")}</p>
                </div>
                <Input {...(inputRef ? { ref: inputRef } : {})} type="file" onChange={handleFileChange} accept=".csv,.xlsx" className="hidden" multiple />
              </div>
              
              {files.length > 0 && (
                <div className="space-y-3">
                  <h4 className="text-sm font-medium text-muted-foreground">{t("pendingFiles")}</h4>
                  <ul className="space-y-2">
                    {files.map((file, index) => (
                      <li key={file.name + index} className="flex items-center justify-between bg-muted/50 p-2 rounded-md">
                        <div className="flex items-center gap-3">
                          <FileIcon className="h-5 w-5 text-muted-foreground" />
                          <span className="text-sm font-medium truncate">{file.name}</span>
                        </div>
                        <Button
                          type="button"
                          variant="ghost"
                          size="icon"
                          className="h-8 w-8 text-destructive hover:bg-destructive/10"
                          onClick={(e) => { e.stopPropagation(); handleRemoveFile(index); }}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              <div className="flex flex-col sm:flex-row gap-4">
                <Button type="submit" disabled={loading || files.length === 0} className="w-full sm:w-auto flex-grow">
                  {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                  {loading ? t("processing") : t("startProcessing")}
                </Button>
                <Button type="button" onClick={handlePing} variant="outline" className="w-full sm:w-auto">{t("pingApi")}</Button>
              </div>

              {error && (
                <div className="bg-destructive/10 text-destructive text-sm font-medium p-3 rounded-md text-center">
                  {error}
                </div>
              )}
            </CardContent>
          </Card>
        </form>

        {logs && (
          <Card className="w-full mt-8 backdrop-blur-xl bg-black/60 shadow-xl border-white/20">
            <CardHeader>
              <CardTitle className="text-lg text-green-400">{t("liveLogs")}</CardTitle>
            </CardHeader>
            <CardContent>
              <pre className="text-sm font-mono text-green-300 whitespace-pre-wrap break-all max-h-[300px] overflow-y-auto">
                {logs}
              </pre>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}