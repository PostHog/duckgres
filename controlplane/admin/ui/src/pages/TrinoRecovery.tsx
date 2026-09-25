import { useEffect, useRef, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useIdentity } from "@/components/IdentityProvider";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { api, ApiError } from "@/lib/api";
import { POLL } from "@/lib/query";
import {
  RECOVERY_PREVIEW_MAX_AGE_MS, readRecoveryRequest, recoveryAccessDenied, recoveryAllowed, recoveryBlockers, recoveryComplete,
  recoveryError, recoveryIdentity, recoveryPollingPaused, recoveryReviewKey, recoveryStorageKey, validRecoveryReason,
} from "@/lib/trinoRecovery";
import type { TrinoRecoveryBody } from "@/types/api";

export function TrinoRecovery({ cell }: { cell?: string }) {
  const { isAdmin, me, error, unauthorized } = useIdentity();
  return (
    <Card className="mb-4">
      <CardHeader><CardTitle>Instance recovery</CardTitle></CardHeader>
      <CardContent className="space-y-4 text-sm">
        {!isAdmin || error || unauthorized || !me?.email ? <p>Admin role required for instance recovery.</p> : !cell ?
          <p>Select a logical cell to inspect its instances.</p> : <RecoveryCell key={`${cell}:${me.email}`} cell={cell} actor={me.email} />}
      </CardContent>
    </Card>
  );
}

function RecoveryCell({ cell, actor }: { cell: string; actor: string }) {
  const [instance, setInstance] = useState("");
  const inventory = useQuery({
    queryKey: ["trino-recovery-instances", actor, cell], queryFn: () => api.trinoInstances(cell),
    retry: false, refetchInterval: (query) => recoveryPollingPaused(query.state.error) ? false : POLL.slow,
  });
  const instances = inventory.data?.cell === cell ? inventory.data.instances : [];
  return <>
    <p>Recover an instance that cannot finish draining. This is a destructive failure recovery, not a zero-loss deployment.</p>
    {inventory.error && <p role="alert">{recoveryError(inventory.error)}</p>}
    <div className="flex items-center gap-2">
      <select aria-label="Trino instance" className="h-9 rounded border bg-background px-2" value={instance}
        disabled={inventory.isLoading || !!inventory.error}
        onChange={(event) => setInstance(event.target.value)}>
        <option value="">{inventory.isLoading ? "Loading instances…" : "Choose an instance"}</option>
        {(instances ?? []).map((entry) => <option key={entry.instance_id} value={entry.instance_id}>
          {entry.instance_id} · {entry.phase}
        </option>)}
        {instance && !instances?.some((entry) => entry.instance_id === instance) &&
          <option value={instance}>{instance} · not in active inventory</option>}
      </select>
      <Button variant="outline" disabled={inventory.isFetching || recoveryAccessDenied(inventory.error)} onClick={() => void inventory.refetch()}>Refresh instances</Button>
    </div>
    {inventory.isSuccess && !instances?.length && !instance && <p>No active shared-pool instances.</p>}
    {instance && !recoveryAccessDenied(inventory.error) && <RecoveryInstance key={instance} cell={cell} instance={instance} actor={actor} />}
  </>;
}

function RecoveryInstance({ cell, instance, actor }: { cell: string; instance: string; actor: string }) {
  const storageKey = recoveryStorageKey(cell, instance, actor);
  const [saved] = useState(() => {
    try { return { request: readRecoveryRequest(storageKey), error: "" }; }
    catch (error) { return { request: null, error: recoveryError(error) }; }
  });
  const [pending, setPending] = useState<TrinoRecoveryBody | null>(saved.request?.body ?? null);
  const [mayHaveBeenAccepted, setMayHaveBeenAccepted] = useState(saved.request?.mayHaveBeenAccepted ?? false);
  const [localError, setLocalError] = useState(saved.error);
  const [accessDenied, setAccessDenied] = useState(false);
  const [checking, setChecking] = useState(false);
  const [readBackReady, setReadBackReady] = useState(false);
  const [notice, setNotice] = useState("");
  const submitting = useRef(false);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);
  const preview = useQuery({
    queryKey: ["trino-recovery", actor, cell, instance], queryFn: () => api.trinoRecovery(instance, cell),
    retry: false, staleTime: 0, refetchOnMount: "always", enabled: !accessDenied,
    refetchInterval: (query) => recoveryComplete(query.state.data?.instance.phase ?? "") || recoveryPollingPaused(query.state.error) ? false : POLL.normal,
  });
  const snapshot = preview.data?.cell === cell && preview.data.instance.instance_id === instance ? preview.data : undefined;
  const blockers = snapshot ? recoveryBlockers(snapshot) : [];
  const mutation = useMutation({
    mutationFn: ({ body }: { body: TrinoRecoveryBody; previouslyUnknown: boolean }) => api.requestTrinoRecovery(instance, body, cell), retry: false,
    onSettled: async (_data, error, { body, previouslyUnknown }) => {
      submitting.current = false;
      if (mounted.current && !previouslyUnknown && error instanceof ApiError && [400, 401, 403, 404, 409].includes(error.status)) {
        try {
          sessionStorage.setItem(storageKey, JSON.stringify({ body, mayHaveBeenAccepted: false }));
          setMayHaveBeenAccepted(false);
        } catch {
          setLocalError("Cannot save the rejected request status. Keep the original operation ID and inspect the recorded status.");
        }
      }
      if (recoveryAccessDenied(error)) setAccessDenied(true);
      else if (mounted.current) await refreshPreview();
    },
  });
  const errorStatus = mutation.error instanceof ApiError ? mutation.error.status : undefined;
  const rejected = !mayHaveBeenAccepted && (saved.request?.mayHaveBeenAccepted === false ||
    (errorStatus !== undefined && [400, 401, 403, 404, 409].includes(errorStatus)));
  const recorded = snapshot?.request;
  const reviewKey = snapshot ? recoveryReviewKey(snapshot) : undefined;
  const previousReviewKey = useRef<string>();
  useEffect(() => {
    if (reviewKey && previousReviewKey.current && reviewKey !== previousReviewKey.current && !pending && !recorded) {
      setNotice("The preview changed. Review the updated identity and capacity, then confirm again. No request was sent.");
    }
    if (reviewKey) previousReviewKey.current = reviewKey;
  }, [reviewKey, pending, recorded]);
  const complete = snapshot && recoveryComplete(snapshot.instance.phase);
  const uncertain = pending && !recorded && !rejected && !complete && !mutation.isSuccess;
  const ambiguousConflict = uncertain && errorStatus === 409;
  const denied = accessDenied || recoveryAccessDenied(preview.error);
  const fresh = !!snapshot && !denied && !preview.isFetching && !preview.error && Date.now() - preview.dataUpdatedAt < RECOVERY_PREVIEW_MAX_AGE_MS;

  async function refreshPreview() {
    const result = await preview.refetch();
    setReadBackReady(!result.error && result.data?.cell === cell && result.data.instance.instance_id === instance);
    return result;
  }

  function submit(body: TrinoRecoveryBody) {
    if (!mounted.current || submitting.current || mutation.isPending || recorded || complete || localError || denied) return;
    try {
      sessionStorage.setItem(storageKey, JSON.stringify({ body, mayHaveBeenAccepted: true }));
    } catch {
      setLocalError("Cannot save the exact recovery request in this tab. No request was sent. Enable session storage before continuing.");
      return;
    }
    submitting.current = true;
    setReadBackReady(false);
    setPending(body);
    setMayHaveBeenAccepted(true);
    mutation.mutate({ body, previouslyUnknown: mayHaveBeenAccepted });
  }

  async function start(reason: string) {
    if (submitting.current || !snapshot || !recoveryAllowed(snapshot) || pending || denied) return;
    if (Date.now() - preview.dataUpdatedAt >= RECOVERY_PREVIEW_MAX_AGE_MS) {
      setNotice("The preview is stale. Click Refresh preview, review the current identity and capacity, then submit again. No request was sent.");
      return;
    }
    if (!fresh) return;
    submitting.current = true;
    setChecking(true);
    setNotice("");
    let sent = false;
    try {
      const latest = await refreshPreview();
      if (!mounted.current) return;
      if (latest.error || latest.data?.cell !== cell || latest.data.instance.instance_id !== instance) return;
      if (!recoveryAllowed(latest.data) || recoveryReviewKey(latest.data) !== recoveryReviewKey(snapshot)) {
        setNotice("The preview changed. Review the updated identity and capacity, then confirm again. No request was sent.");
        return;
      }
      submitting.current = false;
      submit({ ...recoveryIdentity(latest.data.instance), operation_id: crypto.randomUUID(), reason, destructive_authorization: true });
      sent = true;
    } catch {
      setLocalError("Cannot create a secure operation ID. No request was sent.");
    } finally {
      if (!sent) submitting.current = false;
      setChecking(false);
    }
  }

  function reviewAgain() {
    if (!rejected || recorded || !fresh) return;
    try { sessionStorage.removeItem(storageKey); }
    catch { setLocalError("Cannot clear the rejected request from this tab."); return; }
    setPending(null);
    mutation.reset();
    void refreshPreview();
  }

  return <section className="space-y-4" aria-label="Recovery preview">
    <Button variant="outline" disabled={preview.isFetching || denied} onClick={() => void refreshPreview()}>Refresh preview</Button>
    {preview.isLoading && <p>Loading recovery preview…</p>}
    {preview.error && <p role="alert">{recoveryError(preview.error)} The previous preview cannot authorize a new request.</p>}
    {localError && <p role="alert">{localError}</p>}
    {notice && <p role="status">{notice}</p>}
    {!snapshot && <div className="space-y-2">
      <p>A valid preview is required before requesting recovery.</p>
      <Button variant="destructive" disabled>Request destructive recovery</Button>
    </div>}
    {snapshot && <>
      <p className="rounded border border-warning/40 p-3">This stored snapshot does not verify live workload or capacity. Check workload and remaining capacity before requesting recovery. The operator verifies its safety gates separately.</p>
      <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-xs">
        <dt>Cell</dt><dd>{cell}</dd>
        <dt>Instance</dt><dd>{instance}</dd>
        <dt>Phase</dt><dd>{snapshot.instance.phase}</dd>
        <dt>Gateway state</dt><dd>{snapshot.instance.gateway_state || "unknown"}</dd>
        {Object.entries(recoveryIdentity(snapshot.instance)).map(([key, value]) =>
          <div key={key} className="contents"><dt>{key}</dt><dd className="break-all font-mono">{value || "missing"}</dd></div>)}
        <dt>Phase changed</dt><dd>{snapshot.instance.phase_changed_at}</dd>
        <dt>Stored serving / minimum / desired</dt><dd>{snapshot.capacity.stored_serving} / {snapshot.capacity.min_serving} / {snapshot.capacity.desired_instances}</dd>
        <dt>Pool frozen</dt><dd>{snapshot.capacity.frozen ? "yes" : "no"}</dd>
      </dl>
      {snapshot.instance.last_error && <p role="alert">{snapshot.instance.last_error}</p>}
      {complete && <p role="status">{snapshot.instance.phase === "FAILURE_RETIRED" ? "Recovery completed: FAILURE_RETIRED." : "Normal retirement completed: RETIRED."} Verify replacement instances are serving.</p>}
      {recorded && <div className="space-y-1 rounded border p-3" role="status">
        <p>Recovery request recorded. It cannot be cancelled or amended.</p>
        <p>Operation: <code>{recorded.operation_id}</code></p>
        <p>Requested by: {recorded.requested_by} · {recorded.created_at}</p>
        <p>Reason: {recorded.reason}</p>
      </div>}
      {pending && !recorded && !complete && <div className="space-y-2 rounded border p-3">
        <p>Operation: <code>{pending.operation_id}</code></p>
        {mutation.isPending ? <p>Submitting recovery request…</p> : mutation.isSuccess ?
          <p>Recovery accepted. Waiting for its recorded status.</p> : uncertain && !ambiguousConflict ?
            <p>Acceptance is unknown. After a successful preview refresh, you can explicitly retry the identical request. Do not start a different operation.</p> : null}
        {mutation.error && <p role="alert">{recoveryError(mutation.error)}</p>}
        {ambiguousConflict && <div className="space-y-2">
          <p>Stop retrying and preserve the original operation. A conflict does not prove that the earlier request was rejected. Inspect the recorded recovery status and control-plane logs before continuing. Do not clear this tab&apos;s saved request or create another operation.</p>
          <p className="flex flex-wrap gap-4">
            <a className="underline" href={`/api/v1/trino/instances/${encodeURIComponent(instance)}/recovery?cell=${encodeURIComponent(cell)}`} target="_blank" rel="noreferrer">Open recorded recovery status</a>
            <a className="underline" href="https://github.com/PostHog/duckgres/blob/main/docs/runbooks/trino-pool-admin-recovery.md#resolve-an-ambiguous-conflict" target="_blank" rel="noreferrer">Ambiguous-conflict runbook</a>
          </p>
        </div>}
        {!rejected && !ambiguousConflict && !mutation.isSuccess && <Button variant="outline" disabled={mutation.isPending || !fresh || (!mutation.isIdle && !readBackReady) || !!localError || denied}
          onClick={() => submit(pending)}>Retry identical request</Button>}
        {rejected && !recorded && <Button variant="outline" disabled={!fresh} onClick={reviewAgain}>Review a new preview</Button>}
      </div>}
      {!pending && !recorded && !complete ? <>
        {blockers.length > 0 && <div className="space-y-1" role="status">
          <p>Recovery is unavailable for this instance:</p>
          <ul className="list-disc space-y-1 pl-5" aria-label="Recovery blockers">
            {blockers.map((blocker) => <li key={blocker}>{blocker}</li>)}
          </ul>
        </div>}
        <RecoveryForm key={reviewKey} instance={instance} disabled={denied || checking || !!localError || blockers.length > 0}
          submitDisabled={!fresh} onSubmit={start} />
      </> : <div className="space-y-2">
        <p>{complete ? "This instance is already retired. It cannot receive a new recovery request." : recorded ?
          "An immutable recovery request already exists for this instance. Follow its progress above." :
          "Resolve the existing request before creating a new recovery request. Its status and available actions are shown above."}</p>
        <Button variant="destructive" disabled>Request destructive recovery</Button>
      </div>}
    </>}
    <p className="text-xs text-muted-foreground">Recovery may lose retained results and continuations. Accepted requests are immutable. This tab saves the exact request for manual retries; it never automatically submits or retries recovery.</p>
  </section>;
}

function RecoveryForm({ instance, disabled, submitDisabled, onSubmit }: {
  instance: string; disabled: boolean; submitDisabled: boolean; onSubmit: (reason: string) => void;
}) {
  const [reason, setReason] = useState("");
  const [confirmation, setConfirmation] = useState("");
  const [acknowledged, setAcknowledged] = useState(false);
  const ready = !disabled && !submitDisabled && acknowledged && confirmation === instance && validRecoveryReason(reason);
  return <form className="space-y-3" onSubmit={(event) => { event.preventDefault(); if (ready) onSubmit(reason); }}>
    <label className="block space-y-1"><span>Reason</span><Input value={reason} maxLength={256} disabled={disabled}
      onChange={(event) => setReason(event.target.value)} /></label>
    <p className="text-xs text-muted-foreground">Required, maximum 256 UTF-8 bytes. Do not include credentials or query text.</p>
    <label className="block space-y-1"><span>Type the instance ID</span><Input value={confirmation} disabled={disabled}
      onChange={(event) => setConfirmation(event.target.value)} autoComplete="off" /></label>
    <label className="flex items-start gap-2"><input type="checkbox" checked={acknowledged} disabled={disabled}
      onChange={(event) => setAcknowledged(event.target.checked)} />
      <span>I authorize destructive recovery and accept losing retained results and continuations on this instance. I checked its workload and remaining capacity.</span>
    </label>
    <Button variant="destructive" type="submit" disabled={!ready}>Request destructive recovery</Button>
  </form>;
}
