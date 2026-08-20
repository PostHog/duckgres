import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { Plus, X } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { EmptyState } from "@/components/states";
import { OrgRef } from "@/components/OrgRef";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { fmtUnits } from "@/lib/format";
import {
  DEFAULT_CPU_PER_MIN,
  DEFAULT_MEM_PER_GIB_MIN,
  DEFAULT_STORAGE_PER_GIB_H,
  fmtMoney,
  orgTotals,
  parsePrice,
  scenarioCost,
  type PriceScenario,
} from "@/lib/pricing";
import type { MonthlyUsageRow } from "@/types/api";

// Pricing sensitivity: enter unit-price scenarios and see each org's monthly
// fee under each of them, side by side. Entirely client-side over the monthly
// usage rows (the same admin-only data as the table above) — nothing is sent
// anywhere; scenarios persist in localStorage so a PM's what-ifs survive a
// reload but never leave their browser.

const STORAGE_KEY = "duckgres-usage-pricing-scenarios-v1";

// A fresh scenario starts from the grounded defaults (EC2-on-demand-ish
// compute + S3 standard storage — see lib/pricing.ts) so the calculator
// prices orgs sensibly on first open; every price is editable.
function newScenario(n: number): PriceScenario {
  return {
    id: `s${Date.now()}-${n}`,
    name: n === 1 ? "Baseline" : `Scenario ${String.fromCharCode(64 + n)}`,
    cpuPerMin: DEFAULT_CPU_PER_MIN,
    memPerGiBMin: DEFAULT_MEM_PER_GIB_MIN,
    storagePerGiBH: DEFAULT_STORAGE_PER_GIB_H,
  };
}

function loadScenarios(): PriceScenario[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw) as PriceScenario[];
      if (Array.isArray(parsed) && parsed.length > 0 && parsed.every((s) => typeof s?.name === "string")) {
        return parsed;
      }
    }
  } catch {
    // corrupted storage → fall through to the default
  }
  return [newScenario(1)];
}

export function UsagePricing({ rows, labels }: { rows: MonthlyUsageRow[]; labels?: Map<string, string> }) {
  const [scenarios, setScenarios] = useState<PriceScenario[]>(loadScenarios);
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(scenarios));
    } catch {
      // storage full/unavailable — the calculator still works for the session
    }
  }, [scenarios]);

  const totals = useMemo(() => orgTotals(rows), [rows]);
  const grand = useMemo(
    () => ({
      cpuMinutes: totals.reduce((s, t) => s + t.cpuMinutes, 0),
      memGiBMinutes: totals.reduce((s, t) => s + t.memGiBMinutes, 0),
      storageGiBHours: totals.reduce((s, t) => s + t.storageGiBHours, 0),
      orgId: "",
    }),
    [totals],
  );

  const update = (id: string, patch: Partial<PriceScenario>) =>
    setScenarios((ss) => ss.map((s) => (s.id === id ? { ...s, ...patch } : s)));
  const remove = (id: string) => setScenarios((ss) => (ss.length > 1 ? ss.filter((s) => s.id !== id) : ss));
  const add = () => setScenarios((ss) => [...ss, newScenario(ss.length + 1)]);

  return (
    <Card className="mt-4">
      <CardHeader className="flex-row items-center justify-between gap-3">
        <div>
          <CardTitle>Pricing sensitivity</CardTitle>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Enter unit prices to see each org's fee for the selected month under each scenario. Baseline starts
            from EC2-on-demand-ish unit costs + S3 standard storage — edit freely. Prices stay in your browser.
          </p>
        </div>
        <Button size="sm" variant="outline" onClick={add}>
          <Plus className="h-4 w-4" /> Add scenario
        </Button>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap gap-3">
          {scenarios.map((s) => (
            <div key={s.id} className="rounded-md border border-border p-3">
              <div className="mb-2 flex items-center gap-2">
                <Input
                  value={s.name}
                  onChange={(e) => update(s.id, { name: e.target.value })}
                  className="h-7 w-32 text-xs font-medium"
                  aria-label="Scenario name"
                />
                {scenarios.length > 1 && (
                  <button
                    type="button"
                    className="text-muted-foreground hover:text-foreground"
                    aria-label={`Remove ${s.name}`}
                    onClick={() => remove(s.id)}
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                )}
              </div>
              <div className="grid grid-cols-3 gap-2">
                {(
                  [
                    ["cpuPerMin", "$/CPU-min"],
                    ["memPerGiBMin", "$/GiB·min"],
                    ["storagePerGiBH", "$/GiB·h"],
                  ] as const
                ).map(([field, label]) => (
                  <label key={field} className="flex flex-col gap-1 text-[11px] text-muted-foreground">
                    {label}
                    <Input
                      type="number"
                      min={0}
                      step="any"
                      value={s[field] === 0 ? "" : String(s[field])}
                      placeholder="0"
                      aria-label={`${s.name} ${label}`}
                      className="h-7 w-24 text-xs"
                      onChange={(e) => update(s.id, { [field]: parsePrice(e.target.value) })}
                    />
                  </label>
                ))}
              </div>
            </div>
          ))}
        </div>

        {totals.length === 0 ? (
          <EmptyState title="No usage rows" description="Pick a month with usage above to price it." />
        ) : (
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead>Org</TableHead>
                <TableHead>CPU-min</TableHead>
                <TableHead>GiB·min</TableHead>
                <TableHead>GiB·h</TableHead>
                {scenarios.map((s) => (
                  <TableHead key={s.id} className="text-right">
                    {s.name}/mo
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {totals.map((t) => (
                <TableRow key={t.orgId}>
                  <TableCell>
                    <Link to={`/orgs/${encodeURIComponent(t.orgId)}`} className="block hover:underline">
                      <OrgRef id={t.orgId} label={labels?.get(t.orgId)} copyable={false} />
                    </Link>
                  </TableCell>
                  <TableCell className="font-mono text-xs">{fmtUnits(t.cpuMinutes)}</TableCell>
                  <TableCell className="font-mono text-xs">{fmtUnits(t.memGiBMinutes)}</TableCell>
                  <TableCell className="font-mono text-xs">{fmtUnits(t.storageGiBHours)}</TableCell>
                  {scenarios.map((s) => (
                    <TableCell key={s.id} className="text-right font-mono text-xs font-medium">
                      {fmtMoney(scenarioCost(t, s))}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
              <TableRow className="border-t-2 font-semibold">
                <TableCell className="text-xs">All orgs</TableCell>
                <TableCell className="font-mono text-xs">{fmtUnits(grand.cpuMinutes)}</TableCell>
                <TableCell className="font-mono text-xs">{fmtUnits(grand.memGiBMinutes)}</TableCell>
                <TableCell className="font-mono text-xs">{fmtUnits(grand.storageGiBHours)}</TableCell>
                {scenarios.map((s) => (
                  <TableCell key={s.id} className="text-right font-mono text-xs">
                    {fmtMoney(scenarioCost(grand, s))}
                  </TableCell>
                ))}
              </TableRow>
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}
