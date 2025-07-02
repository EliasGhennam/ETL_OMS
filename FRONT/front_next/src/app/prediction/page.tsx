"use client";
import { useState, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { CheckCircle2, XCircle, Loader2, UploadCloud, File as FileIcon, Trash2 } from "lucide-react";
import { useTranslations } from "next-intl";
import { Switch } from "@/components/ui/switch";
import { AreaChart, Area, CartesianGrid, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

type StepStatus = "pending" | "in-progress" | "success" | "error";

interface Step {
  id: string;
  title: string;
  description: string;
  status: StepStatus;
  action: () => Promise<void>;
  buttonText: string;
  errorMessage?: string;
}

export default function PredictionPage() {
  const [files, setFiles] = useState<File[]>([]);
  const [dragActive, setDragActive] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const [results, setResults] = useState<any>(null);
  const t = useTranslations("prediction");
  const [mode, setMode] = useState<'train' | 'predict'>("train");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const updateStepStatus = (id: string, status: StepStatus, errorMessage?: string) => {
    setSteps(prevSteps =>
      prevSteps.map(step =>
        step.id === id ? { ...step, status, errorMessage } : step
      )
    );
  };

  const handleApiCall = async (endpoint: string, stepId: string) => {
    updateStepStatus(stepId, "in-progress");
    try {
      const res = await fetch(`http://127.0.0.1:5001${endpoint}`, { method: "POST" });
      const data = await res.json();
      if (res.ok && data.status === 'success') {
        updateStepStatus(stepId, "success");
        if (stepId === 'forecast') setResults(data.details);
      } else {
        throw new Error(data.message || `Erreur à l'étape ${stepId}`);
      }
    } catch (err: any) {
      updateStepStatus(stepId, "error", err.message);
    }
  };

  const [steps, setSteps] = useState<Step[]>([
    { id: 'upload', title: t("step1Title"), description: t("step1Desc"), status: 'pending', action: async () => {}, buttonText: '' },
    { id: 'cache', title: t("step2Title"), description: t("step2Desc"), status: 'pending', action: () => handleApiCall('/etl/cache', 'cache'), buttonText: t("step2Btn") },
    { id: 'train', title: t("step3Title"), description: t("step3Desc"), status: 'pending', action: () => handleApiCall('/etl/train', 'train'), buttonText: t("step3Btn") },
    { id: 'download', title: "Télécharger le modèle IA", description: "Téléchargez le modèle IA entraîné pour l'utiliser dans la génération de prédiction.", status: 'pending', action: async () => { window.open('/models/model_lstm.pt', '_blank'); updateStepStatus('download', 'success'); }, buttonText: "Télécharger le modèle" },
  ]);

  const handleUpload = async () => {
    if (!files.length) return;
    updateStepStatus('upload', 'in-progress');
    const formData = new FormData();
    files.forEach(file => formData.append('files', file));
    try {
      const res = await fetch("http://127.0.0.1:5001/upload-data", { method: "POST", body: formData });
      const data = await res.json();
      if (res.ok && data.status === 'success') {
        updateStepStatus('upload', 'success');
      } else {
        throw new Error(data.message || "Erreur lors de l'upload");
      }
    } catch (err: any) {
      updateStepStatus('upload', 'error', err.message);
    }
  };

  // --- Fonctions de gestion des fichiers (drag & drop, etc.) ---
  const handleRemoveFile = (indexToRemove: number) => setFiles(p => p.filter((_, i) => i !== indexToRemove));
  const handleDrag = (e: React.DragEvent) => { e.preventDefault(); e.stopPropagation(); setDragActive(e.type === "dragenter" || e.type === "dragover"); };
  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault(); e.stopPropagation(); setDragActive(false);
    if (e.dataTransfer.files?.[0]) setFiles(p => [...p, ...Array.from(e.dataTransfer.files)]);
  };
  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files?.[0]) setFiles(p => [...p, ...Array.from(e.target.files as FileList)]);
  };

  const isStepDisabled = (index: number) => {
    if (index === 0) return files.length === 0;
    if (index === 1) return steps[0].status !== 'success';
    return steps[index - 1].status !== 'success';
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
      <div className="relative z-10 max-w-4xl mx-auto flex flex-col items-center justify-center min-h-screen p-4 md:p-12">
        <div className="mb-8 flex justify-center w-full pt-16">
          <label className="flex items-center gap-4 mx-auto">
            <span className="text-base font-medium text-gray-700 dark:text-gray-200">
              {mode === 'train' ? 'Mode : Entraînement du modèle IA' : 'Mode : Génération de prédiction'}
            </span>
            <Switch
              checked={mode === 'predict'}
              onCheckedChange={checked => setMode(checked ? 'predict' : 'train')}
            />
          </label>
        </div>
        {mode === 'train' ? (
          <Card className="w-full backdrop-blur-xl bg-white/60 dark:bg-white/10 shadow-xl border-white/30 dark:border-white/20">
            <CardHeader className="text-center">
              <CardTitle className="text-2xl md:text-3xl font-bold">{t("title")}</CardTitle>
              <CardDescription>{t("description")}</CardDescription>
            </CardHeader>
            <CardContent className="space-y-8 p-6">
              {steps.map((step, index) => (
                <div key={step.id} className="flex flex-col sm:flex-row items-start gap-6">
                  <div className="flex-shrink-0 flex flex-col items-center">
                    <div className={`h-12 w-12 rounded-full flex items-center justify-center border-2 ${
                      step.status === 'success' ? 'bg-green-500/20 border-green-500' : 
                      step.status === 'error' ? 'bg-red-500/20 border-red-500' : 'border-border'
                    }`}>
                      {step.status === 'in-progress' && <Loader2 className="h-6 w-6 animate-spin" />}
                      {step.status === 'success' && <CheckCircle2 className="h-6 w-6 text-green-500" />}
                      {step.status === 'error' && <XCircle className="h-6 w-6 text-red-500" />}
                      {step.status === 'pending' && <span className="text-xl font-bold">{index + 1}</span>}
                    </div>
                    {index < steps.length - 1 && <div className="w-0.5 h-16 bg-border mt-2" />}
                  </div>

                  <div className="flex-grow w-full">
                    <h3 className="text-lg font-semibold">{step.title}</h3>
                    <p className="text-muted-foreground mb-4">{step.description}</p>
                    
                    {step.id === 'upload' && (
                      <div className="space-y-4">
                         <div
                            className={`group relative border-2 border-dashed rounded-lg p-8 text-center transition-colors cursor-pointer ${dragActive ? "border-primary" : "border-border"}`}
                            onClick={() => inputRef.current?.click()} onDragEnter={handleDrag} onDragLeave={handleDrag} onDragOver={handleDrag} onDrop={handleDrop}
                          >
                            <UploadCloud className="h-10 w-10 mx-auto text-muted-foreground group-hover:text-primary transition-colors" />
                            <p className="mt-2 text-sm text-muted-foreground">{t("dropzone")}</p>
                            <Input ref={inputRef} type="file" onChange={handleFileChange} className="hidden" multiple />
                          </div>
                          {files.length > 0 && (
                            <ul className="space-y-2">
                              {files.map((file, i) => (
                                <li key={i} className="flex items-center justify-between bg-muted/50 p-2 rounded-md">
                                  <FileIcon className="h-5 w-5 text-muted-foreground" />
                                  <span className="text-sm font-medium truncate ml-2 flex-grow">{file.name}</span>
                                  <Button type="button" variant="ghost" size="icon" onClick={() => handleRemoveFile(i)}><Trash2 className="h-4 w-4 text-destructive" /></Button>
                                </li>
                              ))}
                            </ul>
                          )}
                        <Button onClick={handleUpload} disabled={files.length === 0 || step.status === 'in-progress'} className="w-full">
                          {step.status === 'in-progress' ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                          {t("uploadBtn")}
                        </Button>
                      </div>
                    )}

                    {step.id !== 'upload' && (
                      <Button onClick={step.action} disabled={isStepDisabled(index) || step.status === 'in-progress'}>
                        {step.status === 'in-progress' && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                        {step.buttonText}
                      </Button>
                    )}
                    {step.status === 'error' && <p className="text-red-500 text-sm mt-2">{step.errorMessage}</p>}
                  </div>
                </div>
              ))}
              {results && (
                  <Card className="mt-6">
                      <CardHeader><CardTitle>{t("resultsTitle")}</CardTitle></CardHeader>
                      <CardContent><pre className="bg-muted p-4 rounded-lg overflow-x-auto">{JSON.stringify(results, null, 2)}</pre></CardContent>
                  </Card>
              )}
            </CardContent>
          </Card>
        ) : (
          <Card className="w-full backdrop-blur-xl bg-white/60 dark:bg-white/10 shadow-xl border-white/30 dark:border-white/20">
            <CardHeader className="text-center">
              <CardTitle>Génération de prédiction</CardTitle>
              <CardDescription>Utilisez un modèle déjà entraîné pour générer des prédictions à partir de vos données.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-8 p-6">
              <div className="flex flex-col items-center gap-6">
                <Button onClick={async () => {
                  setLoading(true);
                  setError(null);
                  setResults(null);
                  try {
                    const res = await fetch('http://127.0.0.1:5001/etl/forecast', { method: 'POST' });
                    const data = await res.json();
                    if (res.ok && data.status === 'success') {
                      setResults(data.details);
                    } else {
                      setError(data.message || 'Erreur lors de la génération de la prédiction');
                    }
                  } catch (e: any) {
                    setError(e.message || 'Erreur lors de la génération de la prédiction');
                  } finally {
                    setLoading(false);
                  }
                }} disabled={loading}>
                  {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                  Générer la prédiction
                </Button>
                {error && <div className="text-red-500 text-sm">{error}</div>}
                {results && Array.isArray(results) && results.length > 0 && results[0].date && results[0].valeur !== undefined ? (
                  <Card className="w-full">
                    <CardHeader>
                      <CardTitle>Résultat de la prédiction</CardTitle>
                      <Button asChild variant="outline" className="mb-4 w-fit">
                        <a href="http://127.0.0.1:5001/download-prediction" target="_blank" rel="noopener noreferrer">
                          Télécharger le fichier CSV de prédiction
                        </a>
                      </Button>
                    </CardHeader>
                    <CardContent>
                      <ResponsiveContainer width="100%" height={400}>
                        <AreaChart data={results} margin={{ left: 12, right: 12 }}>
                          <CartesianGrid vertical={false} />
                          <XAxis dataKey="date" tickLine={false} axisLine={false} tickMargin={8} />
                          <YAxis domain={[0, 'auto']} tickLine={false} axisLine={false} tickMargin={8} />
                          <Tooltip />
                          <Area dataKey="valeur" name="Nouveaux cas prédits" type="natural" fill="#8884d8" fillOpacity={0.4} stroke="#8884d8" />
                        </AreaChart>
                      </ResponsiveContainer>
                    </CardContent>
                  </Card>
                ) : results ? (
                  <Card className="w-full">
                    <CardHeader><CardTitle>Résultat brut</CardTitle></CardHeader>
                    <CardContent><pre className="bg-muted p-4 rounded-lg overflow-x-auto">{JSON.stringify(results, null, 2)}</pre></CardContent>
                  </Card>
                ) : null}
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
} 