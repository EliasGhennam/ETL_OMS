"use client"

import * as React from "react"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "./tooltip"

export type ChartConfig = Record<string, { label: string; color: string }>

interface ChartContainerProps {
  children: React.ReactNode
  config: ChartConfig
  className?: string
}

export function ChartContainer({ children, config, className }: ChartContainerProps) {
  return (
    <div className={`relative w-full h-[400px] ${className}`}>
      <style jsx global>{`
        :root {
          ${Object.entries(config)
            .map(([key, value]) => `--color-${key}: ${value.color};`)
            .join("\n")}
        }
      `}</style>
      {children}
    </div>
  );
}

export function ChartTooltip({ children, ...props }: any) {
  return (
    <TooltipProvider>
      <Tooltip {...props}>
        <TooltipTrigger asChild>{children}</TooltipTrigger>
        <TooltipContent>
          <div className="grid gap-2">
            {Object.entries(props.config || {}).map(([key, value]: [string, any]) => (
              <div key={key} className="flex items-center gap-2">
                <div
                  className="h-2 w-2 rounded-full"
                  style={{ backgroundColor: value.color }}
                />
                <span className="font-medium">{value.label}</span>
              </div>
            ))}
          </div>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

export function ChartTooltipContent({ active, payload, label, config }: any) {
  console.log("ChartTooltipContent props:", { active, payload, label, config });
  if (!active || !payload?.length) {
    console.log("ChartTooltipContent: Not active or empty payload, returning null");
    return null;
  }

  return (
    <div className="rounded-lg border bg-background p-2 shadow-sm">
      <div className="grid gap-2">
        <div className="flex items-center justify-between gap-2">
          <div className="text-sm font-medium">{label}</div>
        </div>
        {payload.map((entry: any, index: number) => (
          <div key={index} className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <div
                className="h-2 w-2 rounded-full"
                style={{ backgroundColor: entry.color }}
              />
              <span className="text-sm font-medium">{entry.name}</span>
            </div>
            <span className="text-sm font-medium">{entry.value}</span>
          </div>
        ))}
      </div>
    </div>
  )
} 