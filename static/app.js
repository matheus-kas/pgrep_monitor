// app.js - centraliza o comportamento do frontend
// Objetivos: refresh manual, controle de intervalo, toasts, modularização

const DEFAULT_INTERVAL = 5000;
let currentInterval = DEFAULT_INTERVAL;
let timerId = null;

function formatLSN(lsn){ return lsn || 'N/A'; }
function formatTimestamp(ts){ return ts ? new Date(ts).toLocaleString('pt-BR') : 'N/A'; }
function formatDuration(seconds){
  if(!seconds && seconds !== 0) return 'N/A';
  const h = Math.floor(seconds/3600), m = Math.floor((seconds%3600)/60), s = Math.floor(seconds%60);
  return `${h}h ${m}m ${s}s`;
}
function prettyBytes(bytes){
  if(bytes === null || bytes === undefined) return 'N/A';
  const units = ['B','KB','MB','GB','TB'];
  let b = Number(bytes);
  let i = 0;
  while(b >= 1024 && i < units.length-1){ b = b/1024; i++; }
  return `${b.toFixed(1)} ${units[i]}`;
}

function showToast(message, timeout = 3000, type = 'info'){
  const t = document.createElement('div');
  t.className = 'toast ' + (type||'');
  const icon = type === 'success' ? '✔️' : (type === 'error' ? '❌' : 'ℹ️');
  t.innerHTML = `<strong style="margin-right:8px">${icon}</strong> <span>${message}</span>`;
  document.body.appendChild(t);
  // force reflow
  void t.offsetWidth;
  t.classList.add('show');
  setTimeout(()=>{ t.classList.remove('show'); setTimeout(()=>t.remove(), 300); }, timeout);
}

async function fetchJson(path){
  const res = await fetch(path);
  if(!res.ok) throw new Error(`${path} -> ${res.status} ${res.statusText}`);
  return res.json();
}

// Chart state and helpers
let lagChart = null;
function initLagChart(){
  const canvas = document.getElementById('lagChart');
  if(!canvas || typeof Chart === 'undefined') return;
  const ctx = canvas.getContext('2d');
  const accent = getComputedStyle(document.body).getPropertyValue('--accent').trim() || '#2f6f9f';
  lagChart = new Chart(ctx, {
    type: 'line',
    data: { labels: [], datasets: [{ label: 'Lag (s)', data: [], borderColor: accent, backgroundColor: 'rgba(47,111,159,0.10)', tension: 0.25, spanGaps: true, pointRadius: 2 }] },
    options: { maintainAspectRatio:false, scales: { x: { type: 'time', time: { unit: 'second', tooltipFormat: 'HH:mm:ss' }, ticks: { autoSkip: true, maxTicksLimit: 8 } }, y: { beginAtZero: true } }, plugins: { legend: { display: false } } }
  });
  loadLagHistory();
}

async function loadLagHistory(limit = 200){
  try{
    const data = await fetchJson(`/api/replica_lag/history?limit=${limit}`);
    if(data.error){ return; }
    const labels = [];
    const values = [];
    data.forEach(item => { labels.push(new Date(item.ts)); values.push(item.replay_lag_seconds === null ? null : Number(item.replay_lag_seconds)); });
    if(lagChart){ lagChart.data.labels = labels; lagChart.data.datasets[0].data = values; lagChart.update(); }
  }catch(e){ console.warn('Falha ao carregar histórico:', e); }
}

function addPointToChart(sample){
  if(!lagChart || !sample) return;
  const t = new Date(sample.ts);
  const v = sample.replay_lag_seconds === null ? null : Number(sample.replay_lag_seconds);
  lagChart.data.labels.push(t);
  lagChart.data.datasets[0].data.push(v);
  const maxPoints = 200;
  while(lagChart.data.labels.length > maxPoints){ lagChart.data.labels.shift(); lagChart.data.datasets[0].data.shift(); }
  lagChart.update('none');
}

async function updateSystemInfo(){
  const el = document.getElementById('systemInfo'); el.classList.remove('skeleton');
  try{
    const data = await fetchJson('/api/system_info');
    let html = `<div class="system-grid">`;
    const master = data.master || {};
    const replica = data.replica || {};
    html += `<div class="server-info master"><h3><i class="fas fa-server"></i> Master</h3>
      <p><strong>IP:</strong> ${master.server_ip ? master.server_ip : 'N/A'}</p>
      <p><strong>Versão:</strong> ${master.pg_version ? master.pg_version.split(',')[0] : 'N/A'}</p>
      <p><strong>DB Size:</strong> ${master.db_size_pretty || 'N/A'} ${master.db_size_bytes ? `(${prettyBytes(master.db_size_bytes)})` : ''}</p>
      <p><strong>Hora:</strong> ${formatTimestamp(master.server_time)}</p></div>`;
    html += `<div class="server-info replica"><h3><i class="fas fa-copy"></i> Réplica</h3>
      <p><strong>IP:</strong> ${replica.server_ip ? replica.server_ip : 'N/A'}</p>
      <p><strong>Versão:</strong> ${replica.pg_version ? replica.pg_version.split(',')[0] : 'N/A'}</p>
      <p><strong>DB Size:</strong> ${replica.db_size_pretty || 'N/A'} ${replica.db_size_bytes ? `(${prettyBytes(replica.db_size_bytes)})` : ''}</p>
      <p><strong>Hora:</strong> ${formatTimestamp(replica.server_time)}</p>
      <p><strong>Modo:</strong> <span class="status-${replica.in_recovery ? 'warning' : 'success'}">${replica.in_recovery ? 'STANDBY' : 'PRIMARY'}</span></p></div>`;
    html += `</div>`;
    el.innerHTML = html;
  }catch(e){ el.innerHTML = `<span class="status-error">Erro: ${e.message}</span>`; showToast('Erro ao obter system_info: ' + e.message, 4000, 'error'); }
}

async function updateReplicaMode(){
  const el = document.getElementById('replicaMode'); el.classList.remove('skeleton');
  try{
    const data = await fetchJson('/api/replica_mode');
    if(data.error){ el.innerHTML = `<span class="status-error">${data.error}</span>`; showToast('replica_mode: ' + data.error, 4000, 'error'); return; }
    const isStandby = data.is_standby;
    const modeText = isStandby ? 'STANDBY' : 'PRIMARY';
    const modeClass = isStandby ? 'warning' : 'success';

    // Preferir current_lsn da réplica quando disponível, caso contrário mostrar o LSN do master (se retornado)
    const currentLsnToShow = data.current_lsn || data.master_current_lsn || null;
    const currentLsnNote = data.current_lsn ? '' : (data.master_current_lsn ? ' (master)' : '');

    let html = `<div class="replica-info">
      <p><strong>Modo:</strong> <span class="status-${modeClass}">${modeText}</span></p>
      <p><strong>Current LSN:</strong> ${formatLSN(currentLsnToShow)}${currentLsnNote}</p>
      <p><strong>Receive LSN:</strong> ${formatLSN(data.receive_lsn)}</p>
      <p><strong>Replay LSN:</strong> ${formatLSN(data.replay_lsn)}</p>
      <p><strong>Última replicação:</strong> ${formatTimestamp(data.last_replay_time)}</p>
    </div>`;
    if(data.master_current_lsn && !data.current_lsn){
      html += `<div class="server-info"><p><strong>Master hora:</strong> ${formatTimestamp(data.master_server_time)}</p></div>`;
    }
    el.innerHTML = html;
  }catch(e){ el.innerHTML = `<span class="status-error">Erro: ${e.message}</span>`; showToast('Erro ao obter replica_mode: ' + e.message, 4000, 'error'); }
}

async function updateReplicaLag(){
  const el = document.getElementById('replicaLag'); el.classList.remove('skeleton');
  try{
    const data = await fetchJson('/api/replica_lag');
    if(data.error){ el.innerHTML = `<span class="status-error">${data.error}</span>`; showToast('replica_lag: ' + data.error, 4000, 'error'); return; }
    let lagSeconds = data.replay_lag_seconds;
    let lagFormatted = (lagSeconds!==null && lagSeconds!==undefined) ? (lagSeconds.toFixed ? lagSeconds.toFixed(1) : lagSeconds) + 's' : 'N/A';
    let html = `<div class="lag-info">
      <p><strong>Lag em tempo:</strong> ${lagFormatted}</p>`;
    if(data.exact_byte_lag !== null && data.exact_byte_lag !== undefined){ html += `<p><strong>Lag exato:</strong> ${data.exact_byte_lag} B</p>`; }
    if(data.lag_pretty){ html += `<p><strong>Lag aproximado:</strong> ${data.lag_pretty}</p>`; }
    html += `<p><strong>Última transação:</strong> ${formatTimestamp(data.last_replay_timestamp)}</p>`;

    let statusText = "PRONTO";
    let statusClass = "success";
    const threshold = 5.0;
    if(data.in_recovery){
      if(lagSeconds === null || lagSeconds === undefined){ statusText = "EM RECUPERAÇÃO"; statusClass = "warning"; }
      else if(lagSeconds > threshold){ statusText = "EM RECUPERAÇÃO"; statusClass = "warning"; }
      else { statusText = "PRONTO"; statusClass = "success"; }
    }

    html += `<p><strong>Status:</strong> <span class="status-${statusClass}">${statusText}</span></p>`;
    html += `</div>`;
    el.innerHTML = html;
    // append to chart
    try{
      const sample = { ts: new Date().toISOString(), replay_lag_seconds: data.replay_lag_seconds, exact_byte_lag: data.exact_byte_lag };
      addPointToChart(sample);
    }catch(e){ /* ignore chart errors */ }
  }catch(e){ el.innerHTML = `<span class="status-error">Erro: ${e.message}</span>`; showToast('Erro ao obter replica_lag: ' + e.message, 4000, 'error'); }
}

async function updateReplicationStatus(){
  const container = document.getElementById('replicationStatus'); container.classList.remove('skeleton');
  try{
    const data = await fetchJson('/api/replication_status');
    if(data.error){ container.innerHTML = `<span class="status-error">${data.error}</span>`; showToast('replication_status: ' + data.error, 4000, 'error'); return; }
    if(!Array.isArray(data) || data.length === 0){ container.innerHTML = '<div class="no-replication">Nenhuma replicação ativa</div>'; return; }
    let html = '<div class="replication-list">';
    data.forEach(client => {
      const connDuration = client.connection_duration_seconds ? formatDuration(client.connection_duration_seconds) : 'N/A';
      html += `<div class="replication-client" data-client='${JSON.stringify(client)}' tabindex="0">
        <h4>${client.application_name || 'Unknown'}</h4>
        <p><strong>IP:</strong> ${client.client_addr || 'N/A'}</p>
        <p><strong>Estado:</strong> <span class="status-${client.state === 'streaming' ? 'success' : 'warning'}">${client.state || 'N/A'}</span></p>
        <p><strong>Sync:</strong> ${client.sync_state || 'N/A'}</p>
        <p><strong>Lag:</strong> ${(client.replay_lag === null || client.replay_lag === undefined) ? 'N/A' : client.replay_lag}</p>
        <p><strong>Conectado há:</strong> ${connDuration}</p>
        <p><strong>Iniciado em:</strong> ${formatTimestamp(client.backend_start)}</p>
      </div>`;
    });
    html += '</div>';
    container.innerHTML = html;

    // delegação: clique em cliente -> abrir modal com detalhes
    container.querySelectorAll('.replication-client').forEach(el => {
      el.addEventListener('click', (e)=>{
        const client = JSON.parse(el.getAttribute('data-client'));
        showModal(client.application_name || 'Detalhes', `<pre>${JSON.stringify(client, null, 2)}</pre>`);
      });
      el.addEventListener('keydown', (e)=>{ if(e.key === 'Enter' || e.key === ' ') { e.preventDefault(); el.click(); } });
    });
  }catch(e){ container.innerHTML = `<span class="status-error">Erro: ${e.message}</span>`; showToast('Erro ao obter replication_status: ' + e.message, 4000, 'error'); }
}

async function updateAll(){
  // show a subtle busy indicator
  try{
    await Promise.all([updateSystemInfo(), updateReplicaMode(), updateReplicaLag(), updateReplicationStatus()]);
    setLastUpdateNow();
  }catch(e){ showToast('Erro ao atualizar: ' + (e.message||e), 4000, 'error'); }
}

function startAutoUpdate(intervalMs){
  stopAutoUpdate();
  if(!intervalMs || intervalMs <= 0) return;
  currentInterval = intervalMs;
  timerId = setInterval(()=>{ updateAll(); }, intervalMs);
  document.getElementById('autoUpdateStatus').textContent = `Ativado (${intervalMs/1000}s)`;
}

function stopAutoUpdate(){
  if(timerId){ clearInterval(timerId); timerId = null; }
  document.getElementById('autoUpdateStatus').textContent = 'Desativado';
}

// copy to clipboard util
// modal helpers
function showModal(title, html){
  const modal = document.getElementById('detailModal');
  modal.setAttribute('aria-hidden', 'false');
  modal.querySelector('#modalTitle').textContent = title;
  modal.querySelector('#modalBody').innerHTML = html;
}
function hideModal(){
  const modal = document.getElementById('detailModal');
  modal.setAttribute('aria-hidden', 'true');
}

// age indicator
let lastUpdateTs = null;
function setLastUpdateNow(){ lastUpdateTs = Date.now(); document.getElementById('lastUpdate').textContent = `Última atualização: ${new Date().toLocaleString('pt-BR')}`; }
function updateDataAge(){
  const el = document.getElementById('dataAge'); if(!lastUpdateTs){ el.textContent = 'Idade dos dados: -'; return; }
  const secs = Math.floor((Date.now() - lastUpdateTs)/1000);
  el.textContent = `Idade dos dados: ${secs}s`;
  // color
  el.classList.remove('warning-value','critical-value');
  if(secs > 30) el.classList.add('critical-value');
  else if(secs > 5) el.classList.add('warning-value');
}


function bindControls(){
  const btn = document.getElementById('btnRefresh');
  const sel = document.getElementById('selectInterval');
  const themeBtn = document.getElementById('btnTheme');
  const modalClose = document.getElementById('modalClose');

  btn.addEventListener('click', async ()=>{ await updateAll(); showToast('Atualização manual concluída', 1500, 'success'); });
  sel.addEventListener('change', (e)=>{
    const ms = Number(e.target.value);
    if(ms > 0) startAutoUpdate(ms); else stopAutoUpdate();
  });

  themeBtn.addEventListener('click', ()=>{
    const isDark = document.body.classList.toggle('dark');
    themeBtn.setAttribute('aria-pressed', isDark ? 'true' : 'false');
    localStorage.setItem('pg_theme_dark', isDark ? '1' : '0');
    showToast(isDark ? 'Tema escuro ativado' : 'Tema claro ativado', 1200, 'info');
  });

  modalClose.addEventListener('click', hideModal);
  document.addEventListener('keydown', (e)=>{ if(e.key === 'Escape') hideModal(); });

  // restore theme preference
  const pref = localStorage.getItem('pg_theme_dark'); if(pref === '1') document.body.classList.add('dark');
}

document.addEventListener('DOMContentLoaded', ()=>{
  bindControls();
  // inicializa com default 5s
  const sel = document.getElementById('selectInterval');
  if(sel) sel.value = String(DEFAULT_INTERVAL);
  startAutoUpdate(DEFAULT_INTERVAL);
  // primeira atualização imediata
  updateAll();
  // inicializar gráfico se possível
  initLagChart();
  // atualiza idade dos dados a cada segundo
  setInterval(updateDataAge, 1000);
});
