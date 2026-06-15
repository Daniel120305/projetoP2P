# ═══════════════════════════════════════════════════════════════════════════════
# dashboard.py — Sprint 4 (extra): Dashboard LOCAL "SUPERVISOR // FARM"
#
# Réplica local do dashboard do professor (nuted-ia.dev) para testar sem depender
# do servidor dele. Faz duas coisas, só com a stdlib:
#   1) COLETOR TCP  (127.0.0.1:9009): recebe os performance_report (JSON + \n) que
#      o master espelha localmente (ver supervisor.LOCAL_DASHBOARD).
#   2) SERVIDOR WEB (127.0.0.1:8080): serve a página e a API /api/nodes (JSON).
#
# Uso:
#   python dashboard.py            → sobe coletor + web; abra http://127.0.0.1:8080
#   python dashboard.py --demo     → injeta nós de exemplo (michel_1/michel_2)
# ═══════════════════════════════════════════════════════════════════════════════
import sys
import json
import time
import socket
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Garante UTF-8 na saída (mesma lição de master.py/worker.py).
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
COLLECTOR_HOST = "127.0.0.1"
COLLECTOR_PORT = 9009          # recebe performance_report dos masters (TCP + \n)
WEB_HOST       = "127.0.0.1"
WEB_PORT       = 8080          # http://127.0.0.1:8080
NODE_TIMEOUT   = 30            # s sem report → nó considerado "down"

# Estado: último report por server_uuid (protegido por _lock)
_lock  = threading.Lock()
_nodes: dict = {}              # server_uuid -> {"report": dict, "last_seen": epoch}


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [DASHBOARD] {msg}", flush=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Coletor TCP — recebe os performance_report (mesmo formato do supervisor)
# ═══════════════════════════════════════════════════════════════════════════════
def _store(report: dict) -> None:
    uuid = report.get("server_uuid")
    if not uuid:
        return
    with _lock:
        _nodes[uuid] = {"report": report, "last_seen": time.time()}


def _handle_collector_conn(conn: socket.socket, addr) -> None:
    buffer = ""
    try:
        while True:
            data = conn.recv(8192).decode("utf-8", errors="replace")
            if not data:
                break
            buffer += data
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    report = json.loads(line)
                    _store(report)
                    log(f"report ← {report.get('server_uuid','?')} "
                        f"(cpu={report.get('performance',{}).get('system',{}).get('cpu',{}).get('usage_percent','?')}%)")
                except json.JSONDecodeError as e:
                    log(f"JSON inválido ignorado: {e}")
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass


def _collector_loop() -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((COLLECTOR_HOST, COLLECTOR_PORT))
    srv.listen(16)
    log(f"Coletor TCP ouvindo em {COLLECTOR_HOST}:{COLLECTOR_PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=_handle_collector_conn, args=(conn, addr), daemon=True).start()


# ═══════════════════════════════════════════════════════════════════════════════
# Snapshot p/ a API (anexa status up/down e idade)
# ═══════════════════════════════════════════════════════════════════════════════
def _snapshot() -> dict:
    now = time.time()
    nodes = []
    with _lock:
        items = list(_nodes.items())
    for uuid, entry in items:
        age = now - entry["last_seen"]
        rep = dict(entry["report"])
        rep["_age_s"]    = int(age)
        rep["_status"]   = "up" if age <= NODE_TIMEOUT else "down"
        rep["_last_seen"] = datetime.fromtimestamp(entry["last_seen"]).strftime("%H:%M:%S")
        nodes.append(rep)
    nodes.sort(key=lambda r: r.get("server_uuid", ""))
    return {"now": datetime.now(timezone.utc).strftime("%H:%M:%S"), "nodes": nodes}


# ═══════════════════════════════════════════════════════════════════════════════
# Servidor Web
# ═══════════════════════════════════════════════════════════════════════════════
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass  # silencia o log padrão do http.server

    def _send(self, code, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/nodes"):
            body = json.dumps(_snapshot()).encode("utf-8")
            self._send(200, body, "application/json; charset=utf-8")
        elif self.path == "/" or self.path.startswith("/index"):
            self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
        else:
            self._send(404, b"not found", "text/plain; charset=utf-8")


# ═══════════════════════════════════════════════════════════════════════════════
# Página (HTML + CSS + JS embutidos — sem libs externas, funciona offline)
# ═══════════════════════════════════════════════════════════════════════════════
PAGE = r"""<!DOCTYPE html>
<html lang="pt-br">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SUPERVISOR // FARM</title>
<style>
  :root{
    --bg:#0a1324; --panel:#0f1d33; --panel2:#13243d; --border:#1d3354;
    --text:#dbe7f3; --muted:#7d92ad;
    --cyan:#38bdf8; --blue:#3b82f6; --green:#34d399; --orange:#fb923c;
    --red:#f43f5e; --yellow:#fbbf24; --purple:#a78bfa;
  }
  *{box-sizing:border-box}
  body{margin:0;background:radial-gradient(1200px 600px at 50% -200px,#11233f 0%,var(--bg) 60%);
    color:var(--text);font-family:'Segoe UI',Roboto,system-ui,sans-serif;font-size:13px}
  .wrap{max-width:1280px;margin:0 auto;padding:16px}
  .mono{font-family:'Cascadia Code',Consolas,'Courier New',monospace}

  header{display:flex;align-items:center;gap:14px;
    background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:12px 16px}
  .logo{font-weight:800;letter-spacing:1px;font-size:18px}
  .logo b{color:var(--cyan)}
  .sub{color:var(--muted);font-size:11px;margin-top:2px}
  .alert{margin-left:auto;display:flex;align-items:center;gap:10px}
  .pill{padding:6px 14px;border-radius:20px;font-weight:700;font-size:12px;
    background:#10331f;color:var(--green);border:1px solid #1c5733}
  .pill.bad{background:#3a1622;color:#fda4b4;border-color:#7f1d33;animation:pulse 1.6s infinite}
  @keyframes pulse{50%{opacity:.55}}
  .clock{color:var(--muted)} .clock b{color:var(--text)}

  .sect{color:var(--muted);font-size:11px;letter-spacing:2px;text-transform:uppercase;
    margin:20px 4px 8px}
  .grid{display:grid;gap:12px}
  .cards{grid-template-columns:repeat(auto-fit,minmax(165px,1fr))}
  .card{background:var(--panel);border:1px solid var(--border);border-radius:10px;
    padding:12px 14px;position:relative;overflow:hidden}
  .card::before{content:"";position:absolute;left:0;top:0;bottom:0;width:4px;background:var(--accent,var(--cyan))}
  .card .lbl{color:var(--muted);font-size:10px;letter-spacing:1.5px;text-transform:uppercase}
  .card .val{font-size:30px;font-weight:800;margin-top:6px;line-height:1}
  .card .val small{font-size:13px;color:var(--muted);font-weight:600}

  .two{grid-template-columns:1.6fr 1fr}
  @media(max-width:900px){.two{grid-template-columns:1fr}}
  .panel{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:14px}
  .panel h3{margin:0 0 10px;font-size:11px;letter-spacing:2px;color:var(--muted);
    text-transform:uppercase;font-weight:700}

  .donutwrap{display:flex;align-items:center;gap:18px}
  .legend{display:flex;flex-direction:column;gap:7px;font-size:12px}
  .legend i{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:8px}
  .legend b{float:right;margin-left:18px;color:var(--text)}

  .nodes{grid-template-columns:repeat(auto-fill,minmax(290px,1fr))}
  .node{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:14px}
  .node.down{opacity:.6;border-color:#7f1d33}
  .nhead{display:flex;align-items:center;gap:8px;margin-bottom:10px}
  .dot{width:9px;height:9px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green)}
  .dot.down{background:var(--red);box-shadow:0 0 8px var(--red)}
  .nhead .name{font-weight:800;font-size:15px}
  .badge{font-size:9px;font-weight:800;padding:2px 7px;border-radius:5px;letter-spacing:1px}
  .badge.up{background:#10331f;color:var(--green)} .badge.dn{background:#3a1622;color:#fda4b4}
  .badge.role{background:#16294a;color:var(--cyan);margin-left:auto}

  .mini{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:6px 0 10px}
  .mini div{background:var(--panel2);border:1px solid var(--border);border-radius:7px;padding:7px 8px;text-align:center}
  .mini .k{color:var(--muted);font-size:9px;letter-spacing:1px;text-transform:uppercase}
  .mini .v{font-size:18px;font-weight:800;margin-top:3px}

  .bar{margin:7px 0}
  .bar .top{display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin-bottom:3px}
  .bar .track{height:7px;background:#0a1628;border-radius:5px;overflow:hidden;border:1px solid var(--border)}
  .bar .fill{height:100%;border-radius:5px;transition:width .4s}

  .kv{display:grid;grid-template-columns:auto 1fr;gap:3px 10px;font-size:11px;margin-top:10px;
    border-top:1px solid var(--border);padding-top:9px}
  .kv .k{color:var(--muted)} .kv .v{text-align:right}
  .nb{font-size:10px;color:var(--muted);margin-top:8px}
  .nb span{color:var(--green)} .nb span.dn{color:var(--red)}

  .spark{width:100%;height:70px;display:block}
  .empty{color:var(--muted);text-align:center;padding:40px;font-style:italic}
  .ts{color:var(--muted);font-size:11px;text-align:right;margin-top:10px}
</style>
</head>
<body>
<div class="wrap">

  <header>
    <div>
      <div class="logo mono">▦ <b>SUPERVISOR</b> // FARM</div>
      <div class="sub">Réplica local · Arquitetura de Sistemas Distribuídos</div>
    </div>
    <div class="alert">
      <div id="alert" class="pill">● Tudo OK</div>
      <div class="clock mono">⏱ <b id="clock">--:--:--</b></div>
    </div>
  </header>

  <div class="sect">Visão Geral do Cluster</div>
  <div class="grid cards" id="stats"></div>

  <div class="grid two" style="margin-top:12px">
    <div class="panel">
      <h3>Topologia da Rede</h3>
      <svg id="topo" viewBox="0 0 640 280" style="width:100%;height:280px"></svg>
    </div>
    <div class="panel">
      <h3>Status dos Workers</h3>
      <div class="donutwrap">
        <canvas id="donut" width="150" height="150"></canvas>
        <div class="legend" id="legend"></div>
      </div>
      <h3 style="margin-top:16px">Histórico (CPU média % · tarefas pendentes)</h3>
      <canvas id="sparkCpu" class="spark"></canvas>
      <canvas id="sparkTasks" class="spark"></canvas>
    </div>
  </div>

  <div class="sect">Nós da Infraestrutura (Detalhes)</div>
  <div class="grid nodes" id="nodes"></div>
  <div class="empty" id="empty">Aguardando dados dos masters… (rode <b>python master.py</b> ou <b>python dashboard.py --demo</b>)</div>

  <div class="ts" id="ts"></div>
</div>

<script>
const COLORS = {busy:'#38bdf8', idle:'#fbbf24', borrowed:'#fb923c', received:'#a78bfa', failed:'#f43f5e'};
const hist = {cpu:[], tasks:[]};

function fmt(n,d=0){return (n==null||isNaN(n))?'–':Number(n).toLocaleString('pt-BR',{minimumFractionDigits:d,maximumFractionDigits:d});}
function barColor(p){return p>=85?'var(--red)':p>=60?'var(--orange)':'var(--green)';}

async function tick(){
  let data;
  try{ data = await (await fetch('/api/nodes',{cache:'no-store'})).json(); }
  catch(e){ return; }
  const nodes = data.nodes||[];
  document.getElementById('clock').textContent = new Date().toLocaleTimeString('pt-BR');
  document.getElementById('ts').textContent = 'última atualização: '+new Date().toLocaleTimeString('pt-BR')+' · '+nodes.length+' nó(s)';
  document.getElementById('empty').style.display = nodes.length? 'none':'block';

  // agregados
  let up=0,down=0,pend=0,run=0,done=0,fail=0,totW=0,lent=0,recv=0,idle=0,busy=0,wfail=0;
  let cpuSum=0,memSum=0,memTot=0,diskTot=0,hiCpu=0,cnt=0;
  for(const n of nodes){
    const s=n.performance?.system||{}, w=n.performance?.farm_state?.workers||{}, t=n.performance?.farm_state?.tasks||{};
    if(n._status==='up')up++; else down++;
    pend+=t.tasks_pending||0; run+=t.tasks_running||0; done+=t.tasks_completed||0; fail+=t.tasks_failed||0;
    totW+=w.total_registered||0; lent+=w.workers_borrowed||0; recv+=w.workers_received||0;
    idle+=w.workers_idle||0; busy+=w.workers_utilization||0; wfail+=w.workers_failed||0;
    const cpu=s.cpu?.usage_percent||0; cpuSum+=cpu; hiCpu=Math.max(hiCpu,cpu);
    memSum+=s.memory?.percent_used||0; memTot+=s.memory?.total_mb||0; diskTot+=s.disk?.total_gb||0; cnt++;
  }
  const cpuAvg=cnt?cpuSum/cnt:0, memAvg=cnt?memSum/cnt:0;

  // alerta
  const al=document.getElementById('alert');
  if(down>0||hiCpu>=85){al.className='pill bad';al.textContent=`● ${down} Down / ${nodes.filter(n=>(n.performance?.system?.cpu?.usage_percent||0)>=85).length} High CPU`;}
  else{al.className='pill';al.textContent='● Tudo OK';}

  // cards
  const cards=[
    ['Servers Ativos',up,'',  '--cyan'],
    ['Tarefas Pendentes',fmt(pend),'','--green'],
    ['Total Workers',totW,'','--green'],
    ['Workers Emprestados',lent,'','--orange'],
    ['CPU Média',fmt(cpuAvg,1),'%','--cyan'],
    ['Memória Média',fmt(memAvg,1),'%','--purple'],
    ['Tarefas Concluídas',fmt(done),'','--green'],
    ['Tarefas Falhas',fmt(fail),'','--red'],
    ['Memória Total',fmt(memTot),'MB','--blue'],
    ['Disco Total',fmt(diskTot,1),'GB','--yellow'],
  ];
  document.getElementById('stats').innerHTML = cards.map(c=>
    `<div class="card" style="--accent:var(${c[3]})"><div class="lbl">${c[0]}</div>
     <div class="val">${c[1]} <small>${c[2]}</small></div></div>`).join('');

  // donut status workers
  drawDonut({Ocupados:busy,Ociosos:idle,Emprestados:lent,Recebidos:recv,Falhos:wfail});

  // topologia
  drawTopo(nodes);

  // histórico
  hist.cpu.push(cpuAvg); hist.tasks.push(pend);
  if(hist.cpu.length>60){hist.cpu.shift();hist.tasks.shift();}
  drawSpark('sparkCpu',hist.cpu,'#38bdf8',100);
  drawSpark('sparkTasks',hist.tasks,'#34d399',null);

  // cards de nó
  document.getElementById('nodes').innerHTML = nodes.map(nodeCard).join('');
}

function nodeCard(n){
  const s=n.performance?.system||{}, w=n.performance?.farm_state?.workers||{}, t=n.performance?.farm_state?.tasks||{};
  const cpu=s.cpu?.usage_percent||0, mem=s.memory?.percent_used||0, disk=s.disk?.percent_used||0;
  const up=n._status==='up';
  const upt=s.uptime_seconds||0, h=Math.floor(upt/3600), m=Math.floor(upt%3600/60), sec=upt%60;
  const nb=(n.performance?.neighbors||[]).map(x=>`<span class="${x.status==='available'?'':'dn'}">${x.server_uuid}</span>`).join(' ')||'—';
  const bw=(w.borrowed_workers||[]).map(b=>`${b.direction==='out'?'↗':'↙'} ${b.peer_uuid}`).join(', ')||'—';
  const bar=(lbl,p,unit='%')=>`<div class="bar"><div class="top"><span>${lbl}</span><span>${fmt(p,1)}${unit}</span></div>
     <div class="track"><div class="fill" style="width:${Math.min(100,p)}%;background:${barColor(p)}"></div></div></div>`;
  return `<div class="node ${up?'':'down'}">
    <div class="nhead"><span class="dot ${up?'':'down'}"></span><span class="name mono">${n.server_uuid||'?'}</span>
      <span class="badge ${up?'up':'dn'}">${up?'UP':'DOWN'}</span><span class="badge role">${n.role||'master'}</span></div>
    <div class="mini">
      <div><div class="k">Pendentes</div><div class="v">${fmt(t.tasks_pending)}</div></div>
      <div><div class="k">CPU</div><div class="v" style="color:${barColor(cpu)}">${fmt(cpu,0)}%</div></div>
      <div><div class="k">MEM</div><div class="v" style="color:${barColor(mem)}">${fmt(mem,0)}%</div></div>
    </div>
    ${bar('CPU',cpu)}${bar('Memória',mem)}${bar('Disco',disk)}
    <div class="kv">
      <div class="k">Workers (vivos/ocup/ocio)</div><div class="v">${w.workers_alive||0} / ${w.workers_utilization||0} / ${w.workers_idle||0}</div>
      <div class="k">Emprestados / Recebidos</div><div class="v">${w.workers_borrowed||0} / ${w.workers_received||0}</div>
      <div class="k">Workers falhos</div><div class="v">${w.workers_failed||0}</div>
      <div class="k">Tarefas (run/ok/fail)</div><div class="v">${t.tasks_running||0} / ${t.tasks_completed||0} / ${t.tasks_failed||0}</div>
      <div class="k">Tarefa mais antiga</div><div class="v">${t.oldest_task_age_s||0}s</div>
      <div class="k">Uptime</div><div class="v">${h}h ${m}m ${sec}s</div>
      <div class="k">Hostname</div><div class="v">${n.hostname||'—'}</div>
    </div>
    <div class="nb">Vizinhos: ${nb} · Empréstimos: ${bw}<br>
      ${n.payload_version||''} · msg ${(n.message_id||'').slice(0,8)} · visto ${n._last_seen||'?'} (${n._age_s||0}s)</div>
  </div>`;
}

function drawDonut(d){
  const c=document.getElementById('donut'), ctx=c.getContext('2d');
  const keys=Object.keys(d), tot=keys.reduce((a,k)=>a+d[k],0);
  ctx.clearRect(0,0,150,150);
  let a=-Math.PI/2; const cx=75,cy=75,r=58,rin=36;
  const palette={Ocupados:COLORS.busy,Ociosos:COLORS.idle,Emprestados:COLORS.borrowed,Recebidos:COLORS.received,Falhos:COLORS.failed};
  if(tot===0){ctx.beginPath();ctx.arc(cx,cy,r,0,2*Math.PI);ctx.strokeStyle='#1d3354';ctx.lineWidth=r-rin;ctx.stroke();}
  for(const k of keys){ if(!d[k])continue; const ang=d[k]/tot*2*Math.PI;
    ctx.beginPath();ctx.moveTo(cx,cy);ctx.arc(cx,cy,r,a,a+ang);ctx.closePath();ctx.fillStyle=palette[k];ctx.fill(); a+=ang; }
  ctx.beginPath();ctx.arc(cx,cy,rin,0,2*Math.PI);ctx.fillStyle='#0f1d33';ctx.fill();
  ctx.fillStyle='#dbe7f3';ctx.font='bold 22px sans-serif';ctx.textAlign='center';ctx.textBaseline='middle';
  ctx.fillText(tot,cx,cy);
  document.getElementById('legend').innerHTML=keys.map(k=>
    `<div><i style="background:${palette[k]}"></i>${k}<b>${d[k]}</b></div>`).join('');
}

function drawTopo(nodes){
  const svg=document.getElementById('topo'); const W=640,H=280,cx=W/2,cy=H/2;
  const NS='http://www.w3.org/2000/svg'; svg.innerHTML='';
  if(!nodes.length)return;
  const pos={}; const R=nodes.length>1?150:0;
  nodes.forEach((n,i)=>{const ang=-Math.PI/2+i*2*Math.PI/nodes.length;
    pos[n.server_uuid]={x:cx+R*Math.cos(ang),y:cy+R*Math.sin(ang)};});
  // links p/ vizinhos
  const line=(a,b,col,dash)=>{const l=document.createElementNS(NS,'line');
    l.setAttribute('x1',a.x);l.setAttribute('y1',a.y);l.setAttribute('x2',b.x);l.setAttribute('y2',b.y);
    l.setAttribute('stroke',col);l.setAttribute('stroke-width','2');if(dash)l.setAttribute('stroke-dasharray','5 5');svg.appendChild(l);};
  nodes.forEach(n=>{(n.performance?.neighbors||[]).forEach(nb=>{
    if(pos[nb.server_uuid])line(pos[n.server_uuid],pos[nb.server_uuid], nb.status==='available'?'#2c5a86':'#7f1d33',true);});});
  // nós + seus workers
  nodes.forEach(n=>{const p=pos[n.server_uuid]; const w=n.performance?.farm_state?.workers||{};
    const wk=w.workers_alive||0, up=n._status==='up';
    for(let i=0;i<Math.min(wk,12);i++){const ang=i*2*Math.PI/Math.max(1,Math.min(wk,12));
      const wx=p.x+42*Math.cos(ang), wy=p.y+42*Math.sin(ang);
      line(p,{x:wx,y:wy},'#1d3354',false);
      const d=document.createElementNS(NS,'circle');d.setAttribute('cx',wx);d.setAttribute('cy',wy);
      d.setAttribute('r','5');d.setAttribute('fill','#38bdf8');svg.appendChild(d);}
    const g=document.createElementNS(NS,'circle');g.setAttribute('cx',p.x);g.setAttribute('cy',p.y);
    g.setAttribute('r','18');g.setAttribute('fill',up?'#0f1d33':'#3a1622');
    g.setAttribute('stroke',up?'#34d399':'#f43f5e');g.setAttribute('stroke-width','3');svg.appendChild(g);
    const tx=document.createElementNS(NS,'text');tx.setAttribute('x',p.x);tx.setAttribute('y',p.y+34);
    tx.setAttribute('fill','#dbe7f3');tx.setAttribute('font-size','11');tx.setAttribute('text-anchor','middle');
    tx.textContent=n.server_uuid;svg.appendChild(tx);});
}

function drawSpark(id,arr,col,maxFix){
  const c=document.getElementById(id); const w=c.clientWidth||300,h=70; c.width=w;c.height=h;
  const ctx=c.getContext('2d'); ctx.clearRect(0,0,w,h);
  if(arr.length<2)return;
  const max=maxFix||Math.max(...arr,1), min=0;
  ctx.beginPath();
  arr.forEach((v,i)=>{const x=i/(arr.length-1)*w, y=h-6-(v-min)/(max-min)*(h-12);
    i?ctx.lineTo(x,y):ctx.moveTo(x,y);});
  ctx.strokeStyle=col;ctx.lineWidth=2;ctx.stroke();
  ctx.lineTo(w,h);ctx.lineTo(0,h);ctx.closePath();ctx.fillStyle=col+'22';ctx.fill();
}

tick(); setInterval(tick,2000);
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════════════════════
# Modo --demo: injeta nós de exemplo para visualizar sem rodar os masters
# ═══════════════════════════════════════════════════════════════════════════════
def _seed_demo() -> None:
    import random
    def make(uuid, neigh):
        return {
            "server_uuid": uuid, "hostname": f"{uuid}.farm.local", "role": "master",
            "task": "performance_report", "payload_version": "sprint4-monitor",
            "message_id": "demo0000-0000",
            "performance": {
                "system": {
                    "uptime_seconds": random.randint(300, 9000),
                    "load_average_1m": round(random.uniform(0.2, 3), 2),
                    "load_average_5m": round(random.uniform(0.2, 3), 2),
                    "cpu": {"usage_percent": round(random.uniform(20, 95), 1),
                            "count_logical": 8, "count_physical": 4},
                    "memory": {"total_mb": 16384, "available_mb": 8000,
                               "percent_used": round(random.uniform(40, 80), 1), "memory_used": 8000},
                    "disk": {"total_gb": 475.9, "free_gb": 250.0, "percent_used": round(random.uniform(40, 70), 1)},
                },
                "farm_state": {
                    "workers": {"total_registered": 6, "workers_utilization": 4, "workers_alive": 6,
                                "workers_idle": 2, "workers_borrowed": 1, "workers_received": 1,
                                "workers_failed": 0, "workers_home": 5, "workers_available_capacity": 2,
                                "borrowed_workers": [{"direction": "out", "peer_uuid": neigh},
                                                     {"direction": "in", "peer_uuid": neigh}]},
                    "tasks": {"tasks_pending": random.randint(0, 50), "tasks_running": 4,
                              "tasks_completed": random.randint(100, 300), "tasks_failed": random.randint(0, 5),
                              "oldest_task_age_s": random.randint(0, 300)},
                },
                "config_thresholds": {"max_task": 10, "warn_cpu_percent": 85,
                                      "warn_memory_percent": 85, "release_task": 4},
                "neighbors": [{"server_uuid": neigh, "status": "available",
                               "last_heartbeat": "now"}],
            },
        }
    def loop():
        while True:
            _store(make("michel_1", "michel_2"))
            _store(make("michel_2", "michel_1"))
            time.sleep(2)
    threading.Thread(target=loop, daemon=True).start()
    log("Modo --demo: injetando michel_1 e michel_2 a cada 2s")


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    threading.Thread(target=_collector_loop, daemon=True).start()
    if "--demo" in sys.argv:
        _seed_demo()
    httpd = ThreadingHTTPServer((WEB_HOST, WEB_PORT), Handler)
    log(f"Dashboard web em http://{WEB_HOST}:{WEB_PORT}")
    log("Abra no navegador. Ctrl+C para encerrar.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        log("encerrando…")
