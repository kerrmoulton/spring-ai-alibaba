"""Real Nacos-backed native Studio approval/resume integration; no synthetic cards."""
import concurrent.futures
import copy
import json
import os
import pathlib
import urllib.error
import urllib.request
import uuid

BASE='http://127.0.0.1:18080'
REMOTE='http://127.0.0.1:18081'
LAB=pathlib.Path(os.environ.get('LAB_ROOT',str(pathlib.Path(__file__).resolve().parents[3]/'spring-ai-alibaba-lab')))
OUT=LAB/'evidence/feature-ui/http'
OUT.mkdir(parents=True,exist_ok=True)
RUN='nacos-feature-'+uuid.uuid4().hex[:8]
RESULTS=[]

def http(path,body=None,name=None,base=BASE):
    req=urllib.request.Request(base+path,data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=45) as response: code,raw=response.status,response.read().decode()
    except urllib.error.HTTPError as error: code,raw=error.code,error.read().decode()
    if name:
        if body is not None: (OUT/(name+'.request.json')).write_text(json.dumps(body,ensure_ascii=False,indent=2))
        (OUT/(name+'.response.txt')).write_text(raw)
    return code,raw

def get(path,base=BASE):
    code,raw=http(path,base=base)
    assert code==200,(code,raw)
    return json.loads(raw)

def counts(case): return get('/repro/status')['counts'].get(case,{'reads':0,'writes':0})
def audit(): return get('/repro/audit',REMOTE)
def reqs(case): return [json.loads(row['value']) for row in audit() if row['layer']=='a2a.request' and case in row['value']]
def rpc_events(request): return [row['value'] for row in audit() if row['layer']=='a2a.event' and isinstance(row['value'],dict) and row['value'].get('id')==request['id']]
def confirms(raw):
    events=[json.loads(line[5:]) for line in raw.splitlines() if line.startswith('data:') and line[5:].strip() not in ('','[DONE]')]
    return [event['message'] for event in events if event.get('message',{}).get('messageType')=='tool-confirm']
def run(agent,case,query='查询客户并创建订单'):
    assert counts(case)=={'reads':0,'writes':0},('nonfresh case',case)
    body={'appName':agent,'userId':'feature-tester','threadId':case,'newMessage':{'content':query+' case='+case,'messageType':'user'},'streaming':True}
    return http('/run_sse',body,case+'-run')
def resume_body(agent,case,card,decision):
    return {'appName':agent,'userId':'feature-tester','threadId':case,'toolFeedbacks':[dict(item,result=decision) for item in card['toolFeedback']]}
def note(name,**details):
    row={'test':name,**details};RESULTS.append(row);print(json.dumps(row,ensure_ascii=False),flush=True)
def pause(agent,case):
    code,raw=run(agent,case)
    cards=confirms(raw)
    assert code==200 and len(cards)==1,(code,raw)
    assert counts(case)=={'reads':1,'writes':0},counts(case)
    return cards[0]
def proposal(case):
    calls=reqs(case);assert len(calls)==1,calls
    wire=rpc_events(calls[0]);states=[event.get('result',{}).get('status',{}).get('state') for event in wire]
    assert 'input-required' in states and 'completed' not in states,wire
    payloads=[part['data']['payload'] for event in wire for part in event.get('result',{}).get('status',{}).get('message',{}).get('parts',[]) if part.get('kind')=='data' and part.get('data',{}).get('type')=='saa.approval.request']
    assert len(payloads)==1,payloads
    interruptions=[row for row in audit() if row['layer']=='remote.graph' and case in row['value'].get('threadId','') and row['value']['output']['class'].endswith('InterruptionMetadata')]
    assert len(interruptions)==1,interruptions
    return payloads[0],states

def correlation(case,p):
    calls=reqs(case);assert len(calls)==2,calls
    initial=calls[0]['params']['message']; resumed=calls[1]['params']['message']
    assert resumed['taskId']==p['taskId'] and resumed['contextId']==p['contextId']
    assert resumed['messageId']!=initial['messageId']
    parts=resumed['parts'];assert len(parts)==1 and parts[0]['kind']=='data'
    assert parts[0]['data']['type']=='saa.approval.decision'
    decision=parts[0]['data']['payload']
    assert decision['requestId']==p['requestId'] and decision['taskId']==p['taskId'] and decision['contextId']==p['contextId']
    assert all(item['argumentsSha256']==proposal['argumentsSha256'] for item,proposal in zip(decision['decisions'],p['tools']))
    return {'requestId':p['requestId'],'taskId':p['taskId'],'contextId':p['contextId'],'initialMessageId':initial['messageId'],'resumeMessageId':resumed['messageId']}

report={'runId':RUN,'profile':'spring-ai-alibaba-hitl/0.2.5-v1','tests':RESULTS,'allPassed':False,
        'limits':['Process-local pending/consumed receipts, no crash-safe distributed exactly-once guarantee.','Business API has no deduplication.','Native browser acceptance is separate.']}
try:
    (OUT/'apps.json').write_text(json.dumps(get('/list-apps')))
    for agent in ('local_sequential','a2a_sequential'):
        for decision in ('APPROVED','REJECTED'):
            case=RUN+'-'+agent+'-'+decision.lower()
            card=pause(agent,case)
            p,states=proposal(case) if agent=='a2a_sequential' else (None,None)
            body=resume_body(agent,case,card,decision)
            code,raw=http('/resume_sse',body,case+'-resume')
            expected=1 if decision=='APPROVED' else 0
            assert code==200 and not confirms(raw),(code,raw)
            assert counts(case)=={'reads':1,'writes':expected},counts(case)
            details=correlation(case,p) if p else {}
            code2,raw2=http('/resume_sse',body,case+'-sequential-replay')
            assert counts(case)=={'reads':1,'writes':expected},(code2,raw2,counts(case))
            if p:
                before=len(reqs(case))
                # Replay the actual decision body at A2A transport as a fresh HTTP request.
                replay=copy.deepcopy(reqs(case)[1]);replay['id']=str(uuid.uuid4());replay['params']['message']['messageId']=str(uuid.uuid4())
                wire_code,wire_raw=http('/a2a',replay,case+'-wire-replay',REMOTE)
                assert counts(case)=={'reads':1,'writes':expected},(wire_code,wire_raw)
                details.update(remoteRequestsAfterParentReplay=before,wireReplayHttpStatus=wire_code)
            note(agent+' '+decision,passed=True,caseId=case,counts=counts(case),pauseStates=states,httpStatuses=[code,code2],**details)

    case=RUN+'-read-control';code,raw=run('a2a_sequential',case,'read only 只查询客户')
    assert code==200 and '查询完成' in raw and not confirms(raw)
    assert counts(case)=={'reads':1,'writes':0}
    note('A2A read-only control',passed=True,caseId=case,counts=counts(case))

    case=RUN+'-concurrent';card=pause('a2a_sequential',case);p,states=proposal(case)
    body=resume_body('a2a_sequential',case,card,'APPROVED')
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        replies=list(pool.map(lambda i:http('/resume_sse',body,case+'-parallel-'+str(i)),range(2)))
    assert counts(case)=={'reads':1,'writes':1},(replies,counts(case))
    assert any(code==200 and not confirms(raw) for code,raw in replies),replies
    details=correlation(case,p)
    note('Concurrent identical native Studio approvals',passed=True,caseId=case,counts=counts(case),httpStatuses=[item[0] for item in replies],**details)
    report['allPassed']=True
except Exception as error:
    report['error']=repr(error)
    raise
finally:
    for label,base in (('client',BASE),('remote',REMOTE)):
        for endpoint in ('audit','status'):
            try: (OUT/(label+'-'+endpoint+'.json')).write_text(json.dumps(get('/repro/'+endpoint,base),ensure_ascii=False,indent=2))
            except Exception as error: report[label+'-'+endpoint+'Error']=repr(error)
    (OUT/'feature-test-results.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
