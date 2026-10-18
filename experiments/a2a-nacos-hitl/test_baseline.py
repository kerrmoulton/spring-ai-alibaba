"""Fresh baseline evidence through native Studio HTTP and the real remote audit API."""
import json
import os
import pathlib
import urllib.request
import uuid

BASE = 'http://127.0.0.1:18080'
REMOTE = 'http://127.0.0.1:18081'
OUT = pathlib.Path(os.environ.get('EVIDENCE_DIR', '/Users/cckowin/test/spring-ai-alibaba-lab/evidence/baseline/http'))
OUT.mkdir(parents=True, exist_ok=True)
RUN = 'nacos-baseline-' + uuid.uuid4().hex[:8]
observations = []

def request(path, body=None, base=BASE):
    req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(),
                                 headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=45) as response:
        return response.read().decode()

def get_json(path, base=BASE):
    return json.loads(request(path, base=base))

def save_sse(name, path, body):
    (OUT / (name + '.request.json')).write_text(json.dumps(body, ensure_ascii=False, indent=2))
    raw = request(path, body)
    (OUT / (name + '.sse')).write_text(raw)
    return [json.loads(line[5:]) for line in raw.splitlines() if line.startswith('data:')]

def run(agent, case, query):
    return save_sse(case, '/run_sse', {'appName': agent, 'userId': 'baseline-tester', 'threadId': case,
                    'newMessage': {'content': query + ' case=' + case, 'messageType': 'user'}, 'streaming': True})

def resume(agent, case, feedback, decision, suffix=''):
    decided = [dict(item, result=decision, description='Deterministic baseline acceptance decision') for item in feedback]
    return save_sse(case + '-' + decision + suffix, '/resume_sse',
                    {'appName': agent, 'userId': 'baseline-tester', 'threadId': case, 'toolFeedbacks': decided})

def confirms(events):
    return [event['message'] for event in events if event.get('message', {}).get('messageType') == 'tool-confirm']

def count(case):
    return get_json('/repro/status')['counts'].get(case, {'reads': 0, 'writes': 0})

def note(name, **details):
    row = {'observation': name, **details}
    observations.append(row)
    print(json.dumps(row, ensure_ascii=False), flush=True)

report = {'runId': RUN, 'frameworkCommit': '17e26ff6fe951d605771c1845229ca3edb043634',
          'frameworkUnmodified': True, 'observations': observations, 'visualInspection': 'separate root task',
          'frameworkUnitTests': 'skipped; this harness exercises actual HTTP integration'}
try:
    (OUT / 'apps.json').write_text(request('/list-apps'))
    for decision in ('APPROVED', 'REJECTED'):
        case = RUN + '-local-' + decision.lower()
        initial = count(case)
        events = run('local_sequential', case, '查询客户并创建订单')
        cards = confirms(events)
        before = count(case)
        row = {'caseId': case, 'initial': initial, 'approvalCards': len(cards), 'beforeDecision': before}
        if cards:
            resume('local_sequential', case, cards[0]['toolFeedback'], decision)
            row['afterDecision'] = count(case)
            row['expectedWrites'] = 1 if decision == 'APPROVED' else 0
            row['expectedBehaviorObserved'] = initial['writes'] == 0 and before['writes'] == 0 and row['afterDecision']['writes'] == row['expectedWrites']
            if decision == 'APPROVED':
                try:
                    resume('local_sequential', case, cards[0]['toolFeedback'], decision, '-replay')
                except Exception as error:
                    row['replayError'] = repr(error)
                row['afterReplay'] = count(case)
        note('local nested ' + decision, **row)

    case = RUN + '-remote-read'
    initial = count(case)
    events = run('a2a_sequential', case, 'read only 只查询客户')
    note('A2A read-only control', caseId=case, initial=initial, counts=count(case), approvalCards=len(confirms(events)))

    case = RUN + '-remote-write'
    initial = count(case)
    events = run('a2a_sequential', case, '查询客户并创建订单')
    cards = confirms(events)
    remote_audit = get_json('/repro/audit', REMOTE)
    calls = [json.loads(item['value']) for item in remote_audit if item['layer'] == 'a2a.request' and case in item['value']]
    request_ids = [item['id'] for item in calls]
    wire = [item['value'] for item in remote_audit if item['layer'] == 'a2a.event' and item['value'].get('id') in request_ids]
    interruptions = [item for item in remote_audit if item['layer'] == 'remote.graph' and
                     case in item['value'].get('threadId', '') and
                     item['value']['output']['class'].endswith('InterruptionMetadata')]
    row = {'caseId': case, 'initial': initial, 'beforeDecision': count(case), 'studioApprovalCards': len(cards),
           'remoteInterruptionCount': len(interruptions), 'remoteRequestCount': len(calls),
           'wireStates': [item.get('result', {}).get('status', {}).get('state') for item in wire]}
    # Preserve actual Studio card when present; otherwise test parent resume using actual remote feedback.
    feedback = cards[0]['toolFeedback'] if cards else interruptions[0]['value']['output'].get('toolFeedbacks', []) if interruptions else []
    if feedback:
        try:
            resume('a2a_sequential', case, feedback, 'APPROVED')
        except Exception as error:
            row['parentResumeError'] = repr(error)
        after_audit = get_json('/repro/audit', REMOTE)
        row['afterParentResume'] = count(case)
        row['remoteRequestCountAfterResume'] = len([item for item in after_audit if item['layer'] == 'a2a.request' and case in item['value']])
        row['resumeFeedbackSource'] = 'native Studio card' if cards else 'actual remote InterruptionMetadata'
    note('A2A write approval and parent resume', **row)
except Exception as error:
    report['error'] = repr(error)
    raise
finally:
    for label, base in (('client', BASE), ('remote', REMOTE)):
        for endpoint in ('status', 'audit'):
            try:
                (OUT / (label + '-' + endpoint + '.json')).write_text(json.dumps(get_json('/repro/' + endpoint, base), ensure_ascii=False, indent=2))
            except Exception as error:
                report[label + '-' + endpoint + '-error'] = repr(error)
    (OUT / 'test-results.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
