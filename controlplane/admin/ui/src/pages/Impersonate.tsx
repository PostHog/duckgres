import { useMemo, useState } from "react";
import { AlertTriangle, Play, ShieldAlert, UserCog } from "lucide-react";
import { PageBody, PageHeader } from "@/components/AppShell";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { EmptyState } from "@/components/states";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useIdentity } from "@/components/IdentityProvider";
import { useImpersonateQuery, useOrgs, useUsers } from "@/hooks/useApi";
import { sqlLooksLikeRead } from "@/lib/format";
import type { QueryResult } from "@/types/api";

export function Impersonate() {
  const { isAdmin } = useIdentity();
  const orgs = useOrgs();
  const users = useUsers();
  const run = useImpersonateQuery();

  const [org, setOrg] = useState("");
  const [username, setUsername] = useState("");
  const [sql, setSql] = useState("SELECT 1;");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [confirmWrite, setConfirmWrite] = useState(false);

  const orgUsers = useMemo(
    () => (users.data ?? []).filter((u) => u.org_id === org),
    [users.data, org],
  );

  if (!isAdmin) {
    return (
      <>
        <PageHeader title="Impersonate" />
        <EmptyState
          icon={<ShieldAlert className="h-6 w-6 text-warning" />}
          title="Admin only"
          description="Impersonation requires the admin role."
        />
      </>
    );
  }

  const isRead = sqlLooksLikeRead(sql);

  const execute = async (allowWrite: boolean) => {
    setError(null);
    setResult(null);
    try {
      const r = await run.mutateAsync({ org, body: { username, sql, allow_write: allowWrite } });
      setResult(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Query failed");
    }
  };

  const onRun = () => {
    if (!org || !username) {
      setError("Pick an org and a user first.");
      return;
    }
    if (isRead) {
      void execute(false);
    } else {
      setConfirmWrite(true);
    }
  };

  return (
    <>
      <PageHeader
        title="Impersonate"
        description="Run SQL as a tenant user for debugging. Writes require explicit confirmation."
      />
      <PageBody>
        {org && username && (
          <div className="mb-4 flex items-center gap-2 rounded-md border border-warning/40 bg-warning/10 px-4 py-2.5 text-sm">
            <UserCog className="h-4 w-4 text-warning" />
            <span>
              You are impersonating <span className="font-mono font-semibold">{username}</span> on org{" "}
              <span className="font-mono font-semibold">{org}</span>. Actions run with that user's privileges.
            </span>
          </div>
        )}

        <Card className="mb-4">
          <CardContent className="space-y-3 pt-4">
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-1">
                <Label>Org</Label>
                <Select
                  value={org}
                  onValueChange={(v) => {
                    setOrg(v);
                    setUsername("");
                  }}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select org…" />
                  </SelectTrigger>
                  <SelectContent>
                    {(orgs.data ?? []).map((o) => (
                      <SelectItem key={o.name} value={o.name}>
                        {o.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1">
                <Label>User</Label>
                <Select value={username} onValueChange={setUsername} disabled={!org}>
                  <SelectTrigger>
                    <SelectValue placeholder={org ? "Select user…" : "Pick an org first"} />
                  </SelectTrigger>
                  <SelectContent>
                    {orgUsers.map((u) => (
                      <SelectItem key={u.username} value={u.username}>
                        {u.username}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className="space-y-1">
              <Label>SQL</Label>
              <Textarea
                value={sql}
                onChange={(e) => setSql(e.target.value)}
                spellCheck={false}
                className="min-h-[140px] font-mono text-sm"
              />
            </div>

            <div className="flex items-center gap-3">
              <Button onClick={onRun} disabled={run.isPending}>
                <Play className="h-4 w-4" /> {run.isPending ? "Running…" : "Run"}
              </Button>
              <Badge variant={isRead ? "success" : "warning"}>
                {isRead ? "read query" : "write — confirmation required"}
              </Badge>
              {error && <span className="text-xs text-destructive">{error}</span>}
            </div>
          </CardContent>
        </Card>

        {result && <ResultGrid result={result} />}
      </PageBody>

      <Dialog open={confirmWrite} onOpenChange={setConfirmWrite}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <AlertTriangle className="h-4 w-4 text-warning" /> Confirm write
            </DialogTitle>
            <DialogDescription>
              This statement does not look like a read. It will run as{" "}
              <span className="font-mono">{username}</span> on <span className="font-mono">{org}</span> with{" "}
              <span className="font-mono">allow_write=true</span> and may modify tenant data.
            </DialogDescription>
          </DialogHeader>
          <pre className="max-h-40 overflow-auto rounded-md bg-muted/40 p-3 font-mono text-xs">{sql}</pre>
          <DialogFooter>
            <Button variant="outline" size="sm" onClick={() => setConfirmWrite(false)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              size="sm"
              onClick={() => {
                setConfirmWrite(false);
                void execute(true);
              }}
            >
              Run write
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

function ResultGrid({ result }: { result: QueryResult }) {
  return (
    <Card className="overflow-hidden">
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle>Result</CardTitle>
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          {result.truncated && <Badge variant="warning">truncated</Badge>}
          <span>{result.row_count} rows</span>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        {result.truncated && (
          <p className="px-4 py-2 text-xs text-warning">
            Result truncated by the server — showing the first {result.rows.length} of {result.row_count} rows.
          </p>
        )}
        {result.columns.length === 0 ? (
          <p className="p-6 text-sm text-muted-foreground">Statement returned no result set.</p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                {result.columns.map((c) => (
                  <TableHead key={c} className="whitespace-nowrap">
                    {c}
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {result.rows.map((row, i) => (
                <TableRow key={i} className="[&>td]:py-1.5">
                  {row.map((cell, j) => (
                    <TableCell key={j} className="font-mono text-xs">
                      {cell == null ? <span className="text-muted-foreground">NULL</span> : String(cell)}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}
