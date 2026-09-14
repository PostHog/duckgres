"""Run synthetic-only probes; keep cluster identifiers out of published results."""
import json
import os
from pathlib import Path
import subprocess
import time

NAMESPACE = 'duckgres-ci-pr-' + os.environ['GITHUB_RUN_ID'] + '0'
LABEL = 'properties-memory-experiment'
WIDTHS = (4096, 16384, 65536)
OUTPUT = Path('memory-results')
OUTPUT.mkdir(exist_ok=True)


def kubectl(*args, payload=None, required=True):
    result = subprocess.run(['kubectl', *args], input=payload, capture_output=True, text=True, timeout=90)
    if required and result.returncode:
        raise RuntimeError('Kubernetes operation failed: ' + args[0])
    return result


def authorized(verb, resource, namespaced=True):
    args = ['auth', 'can-i', verb, resource]
    if namespaced:
        args += ['-n', NAMESPACE]
    if kubectl(*args, required=False).stdout.strip() != 'yes':
        raise RuntimeError('Missing permission: ' + verb + ' ' + resource)


def manifest(width):
    return {
        'apiVersion': 'batch/v1', 'kind': 'Job',
        'metadata': {'name': f'width-{width}', 'namespace': NAMESPACE},
        'spec': {'backoffLimit': 0, 'activeDeadlineSeconds': 2400,
                 'ttlSecondsAfterFinished': 3600,
                 'template': {
                     'metadata': {'labels': {'app': LABEL}, 'annotations': {'karpenter.sh/do-not-disrupt': 'true'}},
                     'spec': {
                         'restartPolicy': 'Never', 'automountServiceAccountToken': False,
                         'nodeSelector': {'kubernetes.io/arch': 'arm64', 'karpenter.sh/nodepool': 'arm64'},
                         'affinity': {'podAntiAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': [{
                             'labelSelector': {'matchLabels': {'app': LABEL}}, 'topologyKey': 'kubernetes.io/hostname'}]}},
                         'securityContext': {'runAsUser': 1000, 'runAsGroup': 1000, 'fsGroup': 1000,
                                             'runAsNonRoot': True, 'seccompProfile': {'type': 'RuntimeDefault'}},
                         'containers': [{
                             'name': 'probe', 'image': os.environ['PROBE_IMAGE'],
                             'args': ['--width', str(width), '--rows', '131072', '--row-group-size', '8192',
                                      '--threads', '1,4,8', '--memory-limit', '48GiB', '--query-timeout', '180',
                                      '--work-dir', '/work', '--output', '/work/results.jsonl'],
                             'resources': {'requests': {'cpu': '8', 'memory': '64Gi', 'ephemeral-storage': '24Gi'},
                                           'limits': {'cpu': '8', 'memory': '64Gi', 'ephemeral-storage': '32Gi'}},
                             'securityContext': {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}},
                             'volumeMounts': [{'name': 'work', 'mountPath': '/work'}]}],
                         'volumes': [{'name': 'work', 'emptyDir': {'sizeLimit': '32Gi'}}]}}}}


def collect(pods):
    for pod in pods:
        width = pod['metadata']['labels'].get('batch.kubernetes.io/job-name', '').removeprefix('width-')
        if width not in {str(w) for w in WIDTHS}:
            continue
        result = kubectl('-n', NAMESPACE, 'logs', pod['metadata']['name'], '-c', 'probe', required=False)
        if result.returncode == 0:
            # Workload has no credentials or customer data; stdout is synthetic JSONL.
            records = []
            for line in result.stdout.splitlines():
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(record, dict):
                    records.append(record)
            (OUTPUT / f'width-{width}.json').write_text(json.dumps(records, indent=2) + '\n')


def main():
    for verb, resource, scoped in [('create','namespaces',False), ('delete','namespaces',False),
                                   ('create','jobs.batch',True), ('get','jobs.batch',True),
                                   ('list','pods',True), ('get','pods/log',True)]:
        authorized(verb, resource, scoped)
    print('Required job and cleanup permissions verified.', flush=True)
    kubectl('create', 'namespace', NAMESPACE)
    last = None
    try:
        payload = {'apiVersion': 'v1', 'kind': 'List', 'items': [manifest(w) for w in WIDTHS]}
        kubectl('create', '-f', '-', payload=json.dumps(payload))
        deadline = time.monotonic() + 2460
        scheduling_deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            pods = json.loads(kubectl('-n', NAMESPACE, 'get', 'pods', '-l', 'app='+LABEL, '-o', 'json').stdout)['items']
            collect(pods)
            statuses = [{'width': int(p['metadata']['labels']['batch.kubernetes.io/job-name'].removeprefix('width-')),
                         'phase': p.get('status', {}).get('phase', 'Unknown'),
                         'exit_codes': [c['state']['terminated']['exitCode'] for c in p.get('status', {}).get('containerStatuses', [])
                                        if 'terminated' in c.get('state', {})]} for p in pods]
            statuses.sort(key=lambda x: x['width'])
            if statuses != last:
                print(json.dumps({'jobs': statuses}), flush=True)
                last = statuses
            if len(statuses) == 3 and all(s['phase'] in ('Succeeded', 'Failed') for s in statuses):
                (OUTPUT/'job-status.json').write_text(json.dumps(statuses, indent=2)+'\n')
                if any(s['phase'] != 'Succeeded' for s in statuses):
                    raise RuntimeError('At least one synthetic job failed; partial results saved.')
                return
            if time.monotonic() > scheduling_deadline and (len(statuses) < 3 or any(s['phase'] == 'Pending' for s in statuses)):
                raise RuntimeError('Workers did not schedule within ten minutes; no admin access requested.')
            time.sleep(20)
        raise RuntimeError('Synthetic experiment reached its wall-clock limit; partial results saved.')
    finally:
        cleanup = kubectl('delete', 'namespace', NAMESPACE, '--wait=true', '--timeout=90s', required=False)
        print('Temporary namespace deleted.' if cleanup.returncode == 0 else 'Namespace cleanup requires the workflow fallback.', flush=True)


if __name__ == '__main__':
    main()
