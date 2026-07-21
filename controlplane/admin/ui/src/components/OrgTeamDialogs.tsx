// Create / edit / delete dialogs for org teams (duckgres_org_teams rows),
// shared by the Org teams page and the org detail page. The dialogs encode the
// surface rules: billing is repointed (never cleared) and carries the org's
// buffered usage with it; the org's last team cannot be deleted. The edit
// dialog is the operator break-glass — it can change EVERY setting, including
// the schema name (a config-only rename that moves no warehouse data) and the
// legacy table-name overrides; schema immutability remains a user-facing-flow
// rule, not an admin one.
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

// backfill_enabled always has a value (NOT NULL DEFAULT TRUE, mirroring the
// PostHog-side Django default) — a plain on/off badge.
export function BackfillBadge({ value }: { value: boolean }) {
  return value ? <Badge variant="success">on</Badge> : <Badge variant="muted">off</Badge>;
}

// EarliestEventDateCell renders PostHog's cached earliest-event date: em dash
// while the PostHog sensor hasn't resolved it yet, "none" for the 9999-12-31
// "no event history" sentinel (PostHog's NO_HISTORY_SENTINEL), the plain date
// otherwise.
export function EarliestEventDateCell({ value }: { value: string | null | undefined }) {
  if (!value) return <span className="text-muted-foreground">—</span>;
  if (value === "9999-12-31") {
    return (
      <span className="text-muted-foreground" title="9999-12-31 sentinel: the team has no event history">
        none
      </span>
    );
  }
  return <span className="font-mono text-xs tabular-nums">{value}</span>;
}

// LegacyNamesBadge: compact indicator that a row carries explicit legacy
// table-name overrides (grandfathered pre-existing teams). The actual names
// live in the hover tooltip so the tables stay narrow.
export function LegacyNamesBadge({ team }: { team: OrgTeam }) {
  const parts = [
    team.events_table_name && `events: ${team.events_table_name}`,
    team.persons_table_name && `persons: ${team.persons_table_name}`,
    team.schema_data_imports_name && `data imports: ${team.schema_data_imports_name}`,
  ].filter((p): p is string => Boolean(p));
  if (parts.length === 0) return null;
  return (
    <Badge variant="outline" className="whitespace-nowrap" title={parts.join("\n")}>
      legacy names
    </Badge>
  );
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
  const [backfill, setBackfill] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const targetOrg = org ?? orgChoice;
  const teamIdOk = /^\d+$/.test(teamId.trim()) && Number(teamId.trim()) > 0;

  const close = () => {
    setOrgChoice(org ?? "");
    setTeamId("");
    setSchemaName("");
    setEnabled(true);
    setBackfill(true);
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
        backfill_enabled: backfill,
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
          <div className="flex items-center justify-between">
            <Label>Backfill</Label>
            <Switch checked={backfill} onCheckedChange={setBackfill} />
          </div>
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

// legacyNameBody maps an edited legacy table-name input to its wire value:
// unchanged = undefined (omit, preserve), empty = null (clear back to
// "derive from schema name"), otherwise the trimmed value.
function legacyNameBody(edited: string, current: string | null | undefined): string | null | undefined {
  const trimmed = edited.trim();
  if (trimmed === (current ?? "")) return undefined;
  return trimmed === "" ? null : trimmed;
}

// EditTeamDialog: the operator break-glass — every setting is editable,
// including the schema name and the legacy table-name overrides. Billing can
// only be pointed at this team, never cleared.
export function EditTeamDialog({ team, onClose }: { team: OrgTeam; onClose: () => void }) {
  const update = useUpdateOrgTeam();
  const isBilling = Boolean(team.is_billing_team);
  const [schemaName, setSchemaName] = useState(team.schema_name);
  const [enabled, setEnabled] = useState(team.enabled);
  const [backfill, setBackfill] = useState(team.backfill_enabled);
  const [makeBilling, setMakeBilling] = useState(false);
  const [eventsName, setEventsName] = useState(team.events_table_name ?? "");
  const [personsName, setPersonsName] = useState(team.persons_table_name ?? "");
  const [importsName, setImportsName] = useState(team.schema_data_imports_name ?? "");
  const [earliestEventDate, setEarliestEventDate] = useState(team.earliest_event_date ?? "");
  const [error, setError] = useState<string | null>(null);

  const schemaChanged = schemaName.trim() !== team.schema_name;

  const submit = async () => {
    setError(null);
    const body: OrgTeamUpdateBody = {};
    if (schemaChanged) body.schema_name = schemaName.trim();
    if (enabled !== team.enabled) body.enabled = enabled;
    if (backfill !== team.backfill_enabled) body.backfill_enabled = backfill;
    if (makeBilling && !isBilling) body.is_billing_team = true;
    const events = legacyNameBody(eventsName, team.events_table_name);
    if (events !== undefined) body.events_table_name = events;
    const persons = legacyNameBody(personsName, team.persons_table_name);
    if (persons !== undefined) body.persons_table_name = persons;
    const imports = legacyNameBody(importsName, team.schema_data_imports_name);
    if (imports !== undefined) body.schema_data_imports_name = imports;
    const trimmedDate = earliestEventDate.trim();
    if (trimmedDate !== (team.earliest_event_date ?? "")) {
      // Empty input clears the cached date (wire value null): the PostHog
      // sensor then re-discovers the team's backfill range.
      body.earliest_event_date = trimmedDate === "" ? null : trimmedDate;
    }
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
            Operator repair tool: every setting is editable here, including the schema name.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <FieldRow label="Schema name">
            <Input
              value={schemaName}
              onChange={(e) => setSchemaName(e.target.value)}
              className="font-mono text-xs"
            />
          </FieldRow>
          {schemaChanged && (
            <p className="flex items-start gap-2 text-xs text-destructive">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <span>
                Changing the schema name does not move any data. Tables under the old schema are
                not renamed — this only repoints where duckgres looks for the team's data. Use it
                to repair a broken row, not to relocate a team.
              </span>
            </p>
          )}
          <div className="flex items-center justify-between">
            <Label>Enabled</Label>
            <Switch checked={enabled} onCheckedChange={setEnabled} />
          </div>
          <div className="flex items-center justify-between">
            <Label>Backfill</Label>
            <Switch checked={backfill} onCheckedChange={setBackfill} />
          </div>
          <FieldRow label="Legacy events table">
            <Input
              value={eventsName}
              onChange={(e) => setEventsName(e.target.value)}
              placeholder={`${team.schema_name}.events (derived)`}
              className="font-mono text-xs"
            />
          </FieldRow>
          <FieldRow label="Legacy persons table">
            <Input
              value={personsName}
              onChange={(e) => setPersonsName(e.target.value)}
              placeholder={`${team.schema_name}.persons (derived)`}
              className="font-mono text-xs"
            />
          </FieldRow>
          <FieldRow label="Legacy data-imports schema">
            <Input
              value={importsName}
              onChange={(e) => setImportsName(e.target.value)}
              placeholder={`${team.schema_name}_data_imports (derived)`}
              className="font-mono text-xs"
            />
          </FieldRow>
          <p className="text-xs text-muted-foreground">
            Legacy overrides for grandfathered teams. Leave a field empty to derive the name from
            the schema name.
          </p>
          <FieldRow label="Earliest event date">
            <Input
              type="date"
              value={earliestEventDate}
              onChange={(e) => setEarliestEventDate(e.target.value)}
              className="font-mono text-xs"
            />
          </FieldRow>
          <p className="text-xs text-muted-foreground">
            Cached by PostHog's backfill sensor; 9999-12-31 means the team has no event history.
            Clearing it makes the sensor re-discover the team's backfill range.
          </p>
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
          <Button size="sm" onClick={submit} disabled={update.isPending || schemaName.trim() === ""}>
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
