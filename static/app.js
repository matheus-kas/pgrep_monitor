const DEFAULT_INTERVAL = 5000;
let currentInterval = DEFAULT_INTERVAL;
let timerId = null;

// Alertas ativos (atualizados a cada ciclo)
const _alerts = { lag: null, replication: null, cleanup: null };

// ─── Formatadores ────────────────────────────────────────────────────────────

function formatLSN(lsn){ return lsn || 'N/A'; }

function formatTimestamp(ts){
  return ts ? new Date(ts).toLocaleString('pt-BR') : 'N/A';
}

function formatDuration(seconds){
  if(seconds === null || seconds === undefined) return 'N/A';
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  if(h > 0) return `${h}h ${m}m`;
  if(m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function prettyBytes(bytes){
  if(bytes === null || bytes === undefined) return 'N/A';
  const units = ['B','KB','MB','GB','TB'];
  let b = Number(bytes), i = 0;
  while(b >= 1024 && i < units.length - 1){ b /= 1024; i++; }
  return `${b.toFixed(1)} ${units[i]}`;
}

function formatMinutes(min){
  if(min === null || min === undefined) return 'N/A';
  const h = Math.floor(min / 60);
  const m = Math.floor(min % 60);
  if(h > 0) return `${h}h ${m}min`;
  return `${m}min`;
}

function translateState(s){
  const map = { streaming: 'Transmitindo', catchup: 'Sincronizando', backup: 'Backup', startup: 'Iniciando', stopped: 'Parado' };
  return map[s] || s || 'N/A';
}

function translateSync(s){
  const map = { async: 'Assíncrono', sync: 'Síncrono', quorum: 'Quórum', potential: 'Potencial' };
  return map[s] || s || 'N/A';
}

// ─── Alerta level → CSS ──────────────────────────────────────────────────────

function alertClass(level){
  if(level === 'critical') return 'status-error';
  if(level === 'warn')     return 'status-warning';
  return 'status-success';
}

function setCardLevel(cardClass, level){
  const card = document.querySelector(`.card.${cardClass}`);
  if(!card) return;
  card.classList.remove('critical','warning');
  if(level === 'critical') card.classList.add('critical');
  else if(level === 'warn') card.classList.add('warning');
}

// ─── Banner de alertas ───────────────────────────────────────────────────────

function updateAlertBanner(){
  const banner = document.getElementById('alertBanner');
  if(!banner) return;

  const criticals = [];
  const warnings  = [];

  if(_alerts.replication === 'critical') criticals.push('Réplica desconectada do master');
  if(_alerts.lag === 'critical')         criticals.push('Lag de replicação em nível crítico');
  else if(_alerts.lag === 'warn')        warnings.push('Lag de replicação acima do limite');
  if(_alerts.cleanup === 'critical')     criticals.push('Limpeza de arquivos WAL não executada há muito tempo');
  else if(_alerts.cleanup === 'warn')    warnings.push('Limpeza de arquivos WAL atrasada');

  if(criticals.length === 0 && warnings.length === 0){
    banner.style.display = 'none';
    banner.innerHTML = '';
    return;
  }

  banner.style.display = 'flex';
  let html = '';
  criticals.forEach(msg => {
    html += `<div class="alert-item alert-critical"><i class="fas fa-exclamation-circle"></i> ${msg}</div>`;
  });
  warnings.forEach(msg => {
    html += `<div class="alert-item alert-warn"><i class="fas fa-exclamation-triangle"></i> ${msg}</div>`;
  });
  banner.innerHTML = html;
}

// ─── Toast ───────────────────────────────────────────────────────────────────

function showToast(message, timeout = 3000, type = 'info'){
  const t = document.createElement('div');
  t.className = 'toast ' + (type || '');
  const icon = type === 'success' ? '✔️' : (type === 'error' ? '❌' : 'ℹ️');
  t.innerHTML = `<strong style="margin-right:8px">${icon}</strong> <span>${message}</span>`;
  document.body.appendChild(t);
  void t.offsetWidth;
  t.classList.add('show');
  setTimeout(() => { t.classList.remove('show'); setTimeout(() => t.remove(), 300); }, timeout);
}

// ─── Fetch ───────────────────────────────────────────────────────────────────

async function fetchJson(path){
  const res = await fetch(path);
  if(!res.ok) throw new Error(`${path} → ${res.status} ${res.statusText}`);
  return res.json();
}

// ─── Gráfico de lag ──────────────────────────────────────────────────────────

let lagChart = null;

function initLagChart(){
  const canvas = document.getElementById('lagChart');
  if(!canvas || typeof Chart === 'undefined') return;
  const ctx = canvas.getContext('2d');
  const accent = getComputedStyle(document.body).getPropertyValue('--accent').trim() || '#2f6f9f';
  lagChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [{
        label: 'Lag (s)', data: [],
        borderColor: accent,
        backgroundColor: 'rgba(47,111,159,0.10)',
        tension: 0.25, spanGaps: true, pointRadius: 2
      }]
    },
    options: {
      maintainAspectRatio: false,
      scales: {
        x: { type: 'time', time: { unit: 'second', tooltipFormat: 'HH:mm:ss' }, ticks: { autoSkip: true, maxTicksLimit: 8 } },
        y: { beginAtZero: true }
      },
      plugins: { legend: { display: false } }
    }
  });
  loadLagHistory();
}

async function loadLagHistory(limit = 200){
  try{
    const data = await fetchJson(`/api/replica_lag/history?limit=${limit}`);
    if(data.error) return;
    const labels = [], values = [];
    data.forEach(item => {
      labels.push(new Date(item.ts));
      values.push(item.replay_lag_seconds === null ? null : Number(item.replay_lag_seconds));
    });
    if(lagChart){ lagChart.data.labels = labels; lagChart.data.datasets[0].data = values; lagChart.update(); }
  }catch(e){ console.warn('Falha ao carregar histórico:', e); }
}

function addPointToChart(sample){
  if(!lagChart || !sample) return;
  lagChart.data.labels.push(new Date(sample.ts));
  lagChart.data.datasets[0].data.push(sample.replay_lag_seconds === null ? null : Number(sample.replay_lag_seconds));
  const maxPoints = 200;
  while(lagChart.data.labels.length > maxPoints){ lagChart.data.labels.shift(); lagChart.data.datasets[0].data.shift(); }
  lagChart.update('none');
}

// ─── Cards ───────────────────────────────────────────────────────────────────

async function updateSystemInfo(){
  const el = document.getElementById('systemInfo');
  el.classList.remove('skeleton');
  try{
    const data = await fetchJson('/api/system_info');
    const master  = data.master  || {};
    const replica = data.replica || {};
    let html = `<div class="system-grid">
      <div class="server-info master">
        <h3><i class="fas fa-server"></i> Master</h3>
        <p><strong>IP:</strong> ${master.server_ip || 'N/A'}</p>
        <p><strong>Versão:</strong> ${master.pg_version ? master.pg_version.split(',')[0] : 'N/A'}</p>
        <p><strong>Tamanho DB:</strong> ${master.db_size_pretty || 'N/A'}</p>
        <p><strong>Hora do servidor:</strong> ${formatTimestamp(master.server_time)}</p>
        ${master.error ? `<p><strong>Erro:</strong> <span class="status-error">${master.error}</span></p>` : ''}
      </div>
      <div class="server-info replica">
        <h3><i class="fas fa-copy"></i> Réplica</h3>
        <p><strong>IP:</strong> ${replica.server_ip || 'N/A'}</p>
        <p><strong>Versão:</strong> ${replica.pg_version ? replica.pg_version.split(',')[0] : 'N/A'}</p>
        <p><strong>Tamanho DB:</strong> ${replica.db_size_pretty || 'N/A'}</p>
        <p><strong>Hora do servidor:</strong> ${formatTimestamp(replica.server_time)}</p>
        <p><strong>Modo:</strong> <span class="status-${replica.in_recovery ? 'success' : 'warning'}">${replica.in_recovery ? 'STANDBY' : 'PRIMARY'}</span></p>
        ${replica.error ? `<p><strong>Erro:</strong> <span class="status-error">${replica.error}</span></p>` : ''}
      </div>
    </div>`;
    el.innerHTML = html;
  }catch(e){
    el.innerHTML = `<span class="status-error">Erro: ${e.message}</span>`;
    showToast('Erro ao obter informações do sistema: ' + e.message, 4000, 'error');
  }
}

async function updateReplicaMode(){
  const el = document.getElementById('replicaMode');
  el.classList.remove('skeleton');
  try{
    const data = await fetchJson('/api/replica_mode');
    if(data.error){ el.innerHTML = `<span class="status-error">${data.error}</span>`; return; }

    const isStandby = data.is_standby;
    const lsnToShow = data.current_lsn || data.master_current_lsn || null;
    const lsnNote   = !data.current_lsn && data.master_current_lsn ? ' <span style="color:var(--text-faint);font-size:0.72rem">(master)</span>' : '';

    let html = `<div class="replica-info">
      <p><strong>Modo:</strong> <span class="status-${isStandby ? 'success' : 'warning'}">${isStandby ? 'STANDBY (normal)' : 'PRIMARY'}</span></p>
      <p><strong>LSN atual:</strong> ${formatLSN(lsnToShow)}${lsnNote}</p>
      <p><strong>LSN recebido:</strong> ${formatLSN(data.receive_lsn)}</p>
      <p><strong>LSN aplicado:</strong> ${formatLSN(data.replay_lsn)}</p>
      <p><strong>Última transação replicada:</strong> ${formatTimestamp(data.last_replay_time)}</p>
    </div>`;
    el.innerHTML = html;
  }catch(e){
    el.innerHTML = `<span class="status-error">Erro: ${e.message}</span>`;
    showToast('Erro ao obter modo da réplica: ' + e.message, 4000, 'error');
  }
}

async function updateReplicaLag(){
  const el = document.getElementById('replicaLag');
  el.classList.remove('skeleton');
  try{
    const data = await fetchJson('/api/replica_lag');
    if(data.error){
      el.innerHTML = `<span class="status-error">${data.error}</span>`;
      showToast('Erro no lag da réplica: ' + data.error, 4000, 'error');
      return;
    }

    _alerts.lag = data.alert_level || null;
    setCardLevel('replica-lag', data.alert_level);

    const lagS     = data.replay_lag_seconds;
    const lagFmt   = lagS !== null && lagS !== undefined ? lagS.toFixed(1) + 's' : 'N/A';
    const byteFmt  = data.exact_byte_lag !== null && data.exact_byte_lag !== undefined
      ? prettyBytes(data.exact_byte_lag)
      : '0 B';

    const sc = alertClass(data.alert_level);
    const t  = data.thresholds || {};

    let html = `<div class="lag-info">
      <p><strong>Status:</strong> <span class="${sc}">${data.status_pt || data.status || 'N/A'}</span></p>
      <p><strong>Atraso em tempo:</strong> ${lagFmt}</p>
      <p><strong>Atraso em dados:</strong> ${byteFmt}</p>
      <p><strong>Última transação replicada:</strong> ${formatTimestamp(data.last_replay_timestamp)}</p>`;

    if(t.warn_seconds){
      html += `<p><strong>Limite atenção / crítico:</strong> <span style="color:var(--text-muted)">${t.warn_seconds}s / ${t.critical_seconds}s &nbsp;·&nbsp; ${prettyBytes(t.warn_bytes)} / ${prettyBytes(t.critical_bytes)}</span></p>`;
    }
    html += `</div>`;
    el.innerHTML = html;

    try{
      addPointToChart({ ts: new Date().toISOString(), replay_lag_seconds: data.replay_lag_seconds, exact_byte_lag: data.exact_byte_lag });
    }catch(e){ /* ignore */ }
  }catch(e){
    el.innerHTML = `<span class="status-error">Erro: ${e.message}</span>`;
    showToast('Erro ao obter lag da réplica: ' + e.message, 4000, 'error');
  }
}

async function updateReplicationStatus(){
  const container = document.getElementById('replicationStatus');
  container.classList.remove('skeleton');
  try{
    const data = await fetchJson('/api/replication_status');
    if(data.error){
      container.innerHTML = `<span class="status-error">${data.error}</span>`;
      showToast('Erro no status de replicação: ' + data.error, 4000, 'error');
      return;
    }

    _alerts.replication = data.alert_level || null;
    setCardLevel('replication-status', data.alert_level);

    const clients  = data.clients  || [];
    const stateVis = data.state_visible;

    if(!data.connected || clients.length === 0){
      container.innerHTML = `<div class="no-replication">
        <span class="status-error"><i class="fas fa-exclamation-circle"></i> Nenhuma réplica conectada ao master</span>
        <p style="margin-top:8px;font-size:0.82rem;color:var(--text-muted)">Verifique se o PostgreSQL da réplica está rodando e se o streaming está ativo.</p>
      </div>`;
      return;
    }

    let html = '<div class="replication-list">';
    clients.forEach(client => {
      const connDur    = formatDuration(client.connection_duration_seconds);
      const stateStr   = stateVis ? translateState(client.state) : '<span class="status-success">Transmitindo</span>';
      const stateClass = (!stateVis || client.state === 'streaming') ? 'success' : 'warning';
      const syncStr    = stateVis ? translateSync(client.sync_state) : 'Assíncrono';
      const stateHtml  = stateVis
        ? `<span class="status-${stateClass}">${stateStr}</span>`
        : stateStr;
      html += `<div class="replication-client" data-client='${JSON.stringify(client)}' tabindex="0">
        <h4>${client.application_name || 'Réplica'}</h4>
        <p><strong>IP:</strong> ${client.client_addr || 'N/A'}</p>
        <p><strong>Estado:</strong> ${stateHtml}</p>
        <p><strong>Sincronismo:</strong> ${syncStr}</p>
        <p><strong>Lag de replay:</strong> ${client.replay_lag || '0s'}</p>
        <p><strong>Conectado há:</strong> ${connDur}</p>
        <p><strong>Conectou em:</strong> ${formatTimestamp(client.backend_start)}</p>
        ${!stateVis ? `<p style="color:var(--text-faint);font-size:0.72rem"><strong>Obs:</strong> detalhes de estado requerem role pg_monitor no master</p>` : ''}
      </div>`;
    });
    html += '</div>';
    container.innerHTML = html;

    container.querySelectorAll('.replication-client').forEach(el => {
      el.addEventListener('click', () => {
        const c = JSON.parse(el.getAttribute('data-client'));
        showModal(c.application_name || 'Detalhes', `<pre>${JSON.stringify(c, null, 2)}</pre>`);
      });
      el.addEventListener('keydown', e => { if(e.key === 'Enter' || e.key === ' '){ e.preventDefault(); el.click(); } });
    });
  }catch(e){
    container.innerHTML = `<span class="status-error">Erro: ${e.message}</span>`;
    showToast('Erro ao obter status de replicação: ' + e.message, 4000, 'error');
  }
}

async function updateArchiveCleanup(){
  const el = document.getElementById('archiveCleanup');
  el.classList.remove('skeleton');
  try{
    const data = await fetchJson('/api/archive_cleanup');
    if(data.error){
      const notFound = data.error.includes('archive_cleanup_log') || data.error.includes('does not exist');
      el.innerHTML = notFound
        ? '<div class="no-replication">Tabela <code>archive_cleanup_log</code> não encontrada no master.<br>Execute o SQL de criação conforme documentação.</div>'
        : `<span class="status-error">${data.error}</span>`;
      return;
    }

    _alerts.cleanup = data.alert_level || null;
    setCardLevel('archive-cleanup', data.alert_level);

    const sc   = alertClass(data.alert_level);
    const last = data.last_success;

    let html = `<div class="lag-info">
      <p><strong>Status:</strong> <span class="${sc}">${data.cleanup_status_pt || 'N/A'}</span></p>`;

    if(last){
      html += `<p><strong>Última limpeza:</strong> ${formatTimestamp(last.executed_at)}</p>`;
      html += `<p><strong>WAL limpo até:</strong> <span style="font-family:var(--mono);font-size:0.78rem;word-break:break-all">${last.wal_file || 'N/A'}</span></p>`;
    }

    if(data.last_error && data.alert_level !== 'ok'){
      html += `<p><strong>Último erro:</strong> <span class="status-error">${formatTimestamp(data.last_error.executed_at)} — ${data.last_error.message || ''}</span></p>`;
    }

    html += `</div>`;

    if(data.entries && data.entries.length > 0){
      html += `<div style="margin-top:12px;padding-top:12px;border-top:1px solid var(--border)">
        <div style="font-size:0.72rem;font-weight:600;text-transform:uppercase;letter-spacing:0.06em;color:var(--text-muted);margin-bottom:8px">Histórico recente</div>`;
      data.entries.slice(0, 8).forEach(e => {
        const badge = e.status === 'success'
          ? '<span class="status-success">OK</span>'
          : '<span class="status-error">ERRO</span>';
        const ts  = e.executed_at ? new Date(e.executed_at).toLocaleString('pt-BR') : 'N/A';
        const wal = e.wal_file ? '…' + e.wal_file.slice(-10) : 'N/A';
        html += `<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid var(--border);font-family:var(--mono);font-size:0.75rem;gap:8px">
          <span style="color:var(--text-muted);white-space:nowrap">${ts}</span>
          <span title="${e.wal_file || ''}" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${wal}</span>
          ${badge}
        </div>`;
      });
      html += `</div>`;
    }

    el.innerHTML = html;
  }catch(e){ el.innerHTML = `<span class="status-error">Erro: ${e.message}</span>`; }
}

// ─── Ciclo de atualização ────────────────────────────────────────────────────

async function updateAll(){
  try{
    await Promise.all([
      updateSystemInfo(),
      updateReplicaMode(),
      updateReplicaLag(),
      updateReplicationStatus(),
      updateArchiveCleanup(),
    ]);
    updateAlertBanner();
    setLastUpdateNow();
  }catch(e){ showToast('Erro ao atualizar: ' + (e.message || e), 4000, 'error'); }
}

function startAutoUpdate(intervalMs){
  stopAutoUpdate();
  if(!intervalMs || intervalMs <= 0) return;
  currentInterval = intervalMs;
  timerId = setInterval(() => updateAll(), intervalMs);
  document.getElementById('autoUpdateStatus').textContent = `Ativado (${intervalMs / 1000}s)`;
}

function stopAutoUpdate(){
  if(timerId){ clearInterval(timerId); timerId = null; }
  document.getElementById('autoUpdateStatus').textContent = 'Desativado';
}

// ─── Modal ───────────────────────────────────────────────────────────────────

function showModal(title, html){
  const modal = document.getElementById('detailModal');
  modal.setAttribute('aria-hidden', 'false');
  modal.querySelector('#modalTitle').textContent = title;
  modal.querySelector('#modalBody').innerHTML = html;
}
function hideModal(){
  document.getElementById('detailModal').setAttribute('aria-hidden', 'true');
}

// ─── Indicador de idade dos dados ────────────────────────────────────────────

let lastUpdateTs = null;

function setLastUpdateNow(){
  lastUpdateTs = Date.now();
  document.getElementById('lastUpdate').textContent = `Última atualização: ${new Date().toLocaleString('pt-BR')}`;
}

function updateDataAge(){
  const el = document.getElementById('dataAge');
  if(!lastUpdateTs){ el.textContent = 'Idade dos dados: -'; return; }
  const secs = Math.floor((Date.now() - lastUpdateTs) / 1000);
  el.textContent = `Dados com ${secs}s`;
  el.classList.remove('warning-value','critical-value');
  if(secs > 30)      el.classList.add('critical-value');
  else if(secs > 5)  el.classList.add('warning-value');
}

// ─── Controles ───────────────────────────────────────────────────────────────

function bindControls(){
  document.getElementById('btnRefresh').addEventListener('click', async () => {
    await updateAll();
    showToast('Atualização manual concluída', 1500, 'success');
  });

  document.getElementById('selectInterval').addEventListener('change', e => {
    const ms = Number(e.target.value);
    if(ms > 0) startAutoUpdate(ms); else stopAutoUpdate();
  });

  document.getElementById('btnTheme').addEventListener('click', () => {
    const isDark = document.body.classList.toggle('dark');
    document.getElementById('btnTheme').setAttribute('aria-pressed', isDark ? 'true' : 'false');
    localStorage.setItem('pg_theme_dark', isDark ? '1' : '0');
    showToast(isDark ? 'Tema escuro ativado' : 'Tema claro ativado', 1200, 'info');
  });

  document.getElementById('modalClose').addEventListener('click', hideModal);
  document.addEventListener('keydown', e => { if(e.key === 'Escape') hideModal(); });

  if(localStorage.getItem('pg_theme_dark') === '1') document.body.classList.add('dark');
}

// ─── Init ────────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  bindControls();
  const sel = document.getElementById('selectInterval');
  if(sel) sel.value = String(DEFAULT_INTERVAL);
  startAutoUpdate(DEFAULT_INTERVAL);
  updateAll();
  initLagChart();
  setInterval(updateDataAge, 1000);
});
