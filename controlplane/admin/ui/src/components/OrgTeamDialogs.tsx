// Create / edit / delete dialogs for org teams (duckgres_org_teams rows),
// shared by the Org teams page and the org detail page. The dialogs encode the
// surface rules: schema_name is set once at create time and immutable
// afterwards; billing is repointed (never cleared) and carries the org's
// buffered usage with it; the org's last team cannot be deleted.
import { useState } from "react";
import { AlertTriangle } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useCreateOrgTeam, useDeleteOrgTeam, useUpdateOrgTeam } from "@/hooks/useApi";
import type { OrgTeam, OrgTeamUpdateBody } from "@/types/api";

// Tri-state backfill select value ↔ wire value.
type BackfillChoice = "unset" | "on" | "off";

function backfillChoice(v: boolean | null | undefined): BackfillChoice {
  if (v == null) return "unset";
  return v ? "on" : "off";
}

export function BackfillBadge({ value }: { value: boolean | null | undefined }) {
  if (value == null) return <span className="text-muted-foreground">—</span>;
  return value ? <Badge variant="success">on</Badge> : <Badge variant="muted">off</Badge>;
}

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function FieldRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1">
      <Label>{label}</Label>
      {children}
    </div>
  );
}

// CreateTeamDialog: org is either fixed (org detail page) or picked from
// `orgs` (the global Org teams page).
export function CreateTeamDialog({
  open,
  onClose,
  org,
  orgs,
}: {
  open: boolean;
  onClose: () => void;
  org?: string;
  orgs?: string[];
}) {
  const create = useCreateOrgTeam();
  const [orgChoice, setOrgChoice] = useState(org ?? "");
  const [teamId, setTeamId] = useState("");
  const [schemaName, setSchemaName] = useState("");
  const [enabled, setEnabled] = useState(true);
  const [backfill, setBackfill] = useState<BackfillChoice>("unset");
  const [error, setError] = useState<string | null>(null);

  const targetOrg = org ?? orgChoice;
  const teamIdOk = /^\d+$/.test(teamId.trim()) && Number(teamId.trim()) > 0;

  const close = () => {
    setOrgChoice(org ?? "");
    setTeamId("");
    setSchemaName("");
    setEnabled(true);
    setBackfill("unset");
    setError(null);
    onClose();
  };

  const submit = async () => {
    setError(null);
    try {
      await create.mutateAsync({
        org_id: targetOrg,
        team_id: Number(teamId.trim()),
        schema_name: schemaName.trim(),
        enabled,
        ...(backfill === "unset" ? {} : { backfill_enabled: backfill === "on" }),
      });
      close();
    } catch (e) {
      setError(errMsg(e));
    }
  };

  return (
    <Dialog open={open} onOpenChange={(o) => !o && close()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Add team</DialogTitle>
          <DialogDescription>
            Map a PostHog team to {org ? <span className="font-mono">{org}</span> : "an org"} and the
            warehouse schema its data lives in.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          {!org && (
            <FieldRow label="Org">
              <Select value={orgChoice} onValueChange={setOrgChoice}>
                <SelectTrigger>
                  <SelectValue placeholder="Pick an org…" />
                </SelectTrigger>
                <SelectContent>
                  {(orgs ?? []).map((o) => (
                    <SelectItem key={o} value={o}>
                      {o}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </FieldRow>
          )}
          <FieldRow label="Team id">
            <Input
              value={teamId}
              onChange={(e) => setTeamId(e.target.value)}
              placeholder="PostHog team id, e.g. 12345"
              className="font-mono text-xs"
            />
          </FieldRow>
          <FieldRow label="Schema name">
            <Input
              value={schemaName}
              onChange={(e) => setSchemaName(e.target.value)}
              placeholder="e.g. team_12345"
              className="font-mono text-xs"
            />
          </FieldRow>
          <p className="flex items-start gap-2 text-xs text-warning">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
            <span>
              The schema name cannot be changed later — it is where the team's warehouse data lives.
              Lowercase letters, digits and underscores only.
            </span>
          </p>
          <div className="flex items-center justify-between">
            <Label>Enabled</Label>
            <Switch checked={enabled} onCheckedChange={setEnabled} />
          </div>
          <FieldRow label="Backfill">
            <Select value={backfill} onValueChange={(v) => setBackfill(v as BackfillChoice)}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="unset">unset</SelectItem>
                <SelectItem value="on">enabled</SelectItem>
                <SelectItem value="off">disabled</SelectItem>
              </SelectContent>
            </Select>
          </FieldRow>
          {error && <p className="text-xs text-destructive">{error}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" size="sm" onClick={close}>
            Cancel
          </Button>
          <Button
            size="sm"
            onClick={submit}
            disabled={create.isPending || !targetOrg || !teamIdOk || schemaName.trim() === ""}
          >
            {create.isPending ? "Creating…" : "Create team"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// EditTeamDialog: enabled / backfill / billing repoint. The schema name is
// shown read-only; billing can only be pointed at this team, never cleared.
export function EditTeamDialog({ team, onClose }: { team: OrgTeam; onClose: () => void }) {
  const update = useUpdateOrgTeam();
  const isBilling = Boolean(team.is_billing_team);
  const [enabled, setEnabled] = useState(team.enabled);
  const [backfill, setBackfill] = useState<BackfillChoice>(backfillChoice(team.backfill_enabled));
  const [makeBilling, setMakeBilling] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async () => {
    setError(null);
    const body: OrgTeamUpdateBody = {};
    if (enabled !== team.enabled) body.enabled = enabled;
    if (backfill !== backfillChoice(team.backfill_enabled)) {
      body.backfill_enabled = backfill === "unset" ? null : backfill === "on";
    }
    if (makeBilling && !isBilling) body.is_billing_team = true;
    if (Object.keys(body).length === 0) {
      onClose();
      return;
    }
    try {
      await update.mutateAsync({ org: team.org_id, teamId: team.team_id, body });
      onClose();
    } catch (e) {
      setError(errMsg(e));
    }
  };

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>
            Edit team {team.team_id} in "{team.org_id}"
          </DialogTitle>
          <DialogDescription>
            Schema <span className="font-mono">{team.schema_name}</span> (immutable).
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <Label>Enabled</Label>
            <Switch checked={enabled} onCheckedChange={setEnabled} />
          </div>
          <FieldRow label="Backfill">
            <Select value={backfill} onValueChange={(v) => setBackfill(v as BackfillChoice)}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="unset">unset</SelectItem>
                <SelectItem value="on">enabled</SelectItem>
                <SelectItem value="off">disabled</SelectItem>
              </SelectContent>
            </Select>
          </FieldRow>
          {isBilling ? (
            <p className="text-xs text-muted-foreground">
              This is the org's billing team. To change it, mark another team as billing.
            </p>
          ) : (
            <div className="space-y-1.5">
              <div className="flex items-center justify-between">
                <Label>Make billing team</Label>
                <Switch checked={makeBilling} onCheckedChange={setMakeBilling} />
              </div>
              {makeBilling && (
                <p className="flex items-start gap-2 text-xs text-warning">
                  <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                  <span>
                    Repoints the org's billing to this team and moves its buffered usage buckets
                    along with it.
                  </span>
                </p>
              )}
            </div>
          )}
          {error && <p className="text-xs text-destructive">{error}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button size="sm" onClick={submit} disabled={update.isPending}>
            {update.isPending ? "Saving…" : "Save"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// DeleteTeamDialog: config-only delete with the two rules surfaced up front —
// last-team deletion is refused (delete the org instead), and deleting the
// billing team hands billing to the oldest remaining team.
export function DeleteTeamDialog({
  team,
  teamCount,
  onClose,
}: {
  team: OrgTeam;
  // How many teams the org currently has, so the last-team refusal shows
  // before the request instead of as a raw 409.
  teamCount: number;
  onClose: () => void;
}) {
  const del = useDeleteOrgTeam();
  const [error, setError] = useState<string | null>(null);
  const isLast = teamCount <= 1;
  const isBilling = Boolean(team.is_billing_team);

  const submit = async () => {
    setError(null);
    try {
      await del.mutateAsync({ org: team.org_id, teamId: team.team_id });
      onClose();
    } catch (e) {
      setError(errMsg(e));
    }
  };

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>
            Delete team {team.team_id} from "{team.org_id}"?
          </DialogTitle>
          <DialogDescription>
            Removes only the team's config row. The warehouse schema{" "}
            <span className="font-mono">{team.schema_name}</span> and its data are not touched.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          {isLast ? (
            <p className="flex items-start gap-2 text-xs text-destructive">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <span>
                This is the org's last team and cannot be deleted. The only way to remove it is
                deleting the org.
              </span>
            </p>
          ) : (
            isBilling && (
              <p className="flex items-start gap-2 text-xs text-warning">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                <span>
                  This is the billing team. The org's oldest remaining team automatically becomes
                  billing, and buffered usage buckets move to it.
                </span>
              </p>
            )
          )}
          {error && <p className="text-xs text-destructive">{error}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="destructive" size="sm" onClick={submit} disabled={del.isPending || isLast}>
            {del.isPending ? "Deleting…" : "Delete team"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
