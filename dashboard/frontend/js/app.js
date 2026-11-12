(() => {
  'use strict';

  const API = '';
  const WS_PATH = '/ws/live';
  const RECONNECT_MS = 2500;
  const POLL_MS = 7000;

  const state = {
    mode: 'demo',
    page: 'overview',
    ws: null,
    reconnectTimer: null,
    pollTimer: null,
    processed: [],
    states: [],
    exceptions: [],
    orders: [],
    activities: [],
    selectedOrderId: null,
    config: null,
    generator: null,
  };

  const SCENARIO_HELP = {
    normal_flow: 'Simulates a customer order progressing through the normal delivery journey.',
    duplicate_events: 'Sends repeated events to demonstrate duplicate detection.',
    late_events: 'Sends events that arrive outside the configured event-time tolerance.',
    out_of_order_events: 'Sends lifecycle events in a different arrival order.',
    invalid_events: 'Sends malformed or invalid lifecycle events.',
    continuous: 'Continuously generates new customer order journeys.',
  };

  const STAGE_ORDER = ['ORDER_PLACED', 'ORDER_CONFIRMED', 'ORDER_PACKED', 'ORDER_SHIPPED', 'OUT_FOR_DELIVERY', 'ORDER_DELIVERED'];
  const STAGE_LABELS = {
    ORDER_PLACED: 'Order placed',
    ORDER_CONFIRMED: 'Confirmed',
    ORDER_PACKED: 'Packed',
    ORDER_SHIPPED: 'Shipped',
    OUT_FOR_DELIVERY: 'Out for delivery',
    ORDER_DELIVERED: 'Delivered',
    ORDER_CANCELLED: 'Cancelled',
  };

  const qs = (sel, root = document) => root.querySelector(sel);
  const qsa = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function shortId(value) {
    const text = String(value || '');
    if (text.length <= 26) return text;
    return `${text.slice(0, 12)}…${text.slice(-8)}`;
  }

  function fmtTime(value) {
    if (!value) return '--';
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return String(value);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  function fmtDateTime(value) {
    if (!value) return '--';
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return String(value);
    return d.toLocaleString([], { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  function fmtDurationSeconds(value) {
    if (value == null || value === '' || Number.isNaN(Number(value))) return '--';
    const sec = Number(value);
    if (sec < 0) return '--';
    if (sec < 90) return `${sec.toFixed(1)}s`;
    const m = Math.floor(sec / 60);
    const s = Math.round(sec % 60);
    if (m < 90) return `${m}m ${s}s`;
    const h = Math.floor(m / 60);
    return `${h}h ${m % 60}m`;
  }

  function fmtMillis(value) {
    if (value == null || Number.isNaN(Number(value))) return '--';
    return fmtDurationSeconds(Number(value) / 1000);
  }

  function fmtBytes(value) {
    if (value == null || Number.isNaN(Number(value))) return '--';
    const n = Number(value);
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  }

  function badge(text, tone = '') {
    const cls = tone ? `badge ${tone}` : 'badge';
    return `<span class="${cls}">${escapeHtml(text || '--')}</span>`;
  }

  function statusTone(value) {
    const v = String(value || '').toLowerCase();
    if (['healthy', 'running', 'ok', 'connected', 'delivered', 'completed', 'active'].some(x => v.includes(x))) return 'green';
    if (['degraded', 'warning', 'pending', 'reconnecting', 'starting'].some(x => v.includes(x))) return 'yellow';
    if (['offline', 'error', 'failed', 'missing', 'stopped', 'cancelled'].some(x => v.includes(x))) return 'red';
    if (['processing', 'manual', 'group'].some(x => v.includes(x))) return 'blue';
    return 'purple';
  }

  function toast(message, type = 'info') {
    const stack = qs('#toastStack');
    if (!stack) return;
    const el = document.createElement('div');
    el.className = `toast ${type === 'error' ? 'error' : type === 'success' ? 'success' : ''}`;
    el.textContent = message;
    stack.appendChild(el);
    setTimeout(() => {
      el.remove();
    }, 4200);
  }

  async function api(path, options = {}) {
    const headers = { Accept: 'application/json', ...(options.headers || {}) };
    if (options.body && !(options.body instanceof FormData)) {
      headers['Content-Type'] = 'application/json';
      options.body = JSON.stringify(options.body);
    }
    const res = await fetch(`${API}${path}`, { ...options, headers });
    const text = await res.text();
    let data = null;
    try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }
    if (!res.ok) {
      const detail = data?.detail || data;
      const message = typeof detail === 'string' ? detail : detail?.message || `Request failed (${res.status})`;
      const err = new Error(message);
      err.status = res.status;
      err.detail = detail;
      throw err;
    }
    return data;
  }

  function setConnection(connected, label) {
    const chip = qs('#connectionChip');
    const text = qs('#connectionText');
    if (chip) chip.className = `status-chip ${connected ? 'healthy' : 'warning'}`;
    if (text) text.textContent = label || (connected ? 'Live connected' : 'Reconnecting');
  }

  function setPipeline(infra) {
    const chip = qs('#pipelineChip');
    const text = qs('#pipelineText');
    if (!chip || !text) return;
    const status = infra?.pipeline?.status || 'offline';
    const cls = status === 'healthy' ? 'healthy' : status === 'degraded' ? 'warning' : 'offline';
    chip.className = `status-chip ${cls}`;
    text.textContent = infra?.pipeline?.label || 'Pipeline status unavailable';
  }

  function updateLastUpdate() {
    const el = qs('#lastUpdate');
    if (el) el.textContent = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  function setMode(mode) {
    state.mode = mode === 'technical' ? 'technical' : 'demo';
    document.body.dataset.mode = state.mode;
    qsa('[data-mode-btn]').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.modeBtn === state.mode);
    });
    renderAll();
  }

  function setPage(page) {
    state.page = page;
    qsa('.nav-item').forEach(btn => btn.classList.toggle('active', btn.dataset.page === page));
    qsa('[data-page-section]').forEach(sec => sec.classList.toggle('active', sec.dataset.pageSection === page));
    if (page === 'orders' && state.selectedOrderId) loadOrder(state.selectedOrderId);
  }

  function initNav() {
    qsa('.nav-item').forEach(btn => btn.addEventListener('click', () => setPage(btn.dataset.page)));
    qsa('[data-mode-btn]').forEach(btn => btn.addEventListener('click', () => setMode(btn.dataset.modeBtn)));
  }

  function initTabs() {
    qsa('.tabs').forEach(group => {
      const name = group.dataset.tabs;
      qsa('[data-tab]', group).forEach(btn => {
        btn.addEventListener('click', () => {
          qsa('[data-tab]', group).forEach(b => b.classList.toggle('active', b === btn));
          const target = btn.dataset.tab;
          const scope = group.closest('.panel') || document;
          qsa(`[data-tab-pane^="${name}-"]`, scope).forEach(pane => {
            pane.classList.toggle('active', pane.dataset.tabPane === `${name}-${target}`);
          });
        });
      });
    });
  }

  function initModal() {
    const modal = qs('#jsonModal');
    const close = () => modal.classList.remove('open');
    qs('#jsonClose')?.addEventListener('click', close);
    modal?.addEventListener('click', (e) => { if (e.target === modal) close(); });
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape') close(); });
    document.addEventListener('click', (e) => {
      const btn = e.target.closest('[data-json]');
      if (!btn) return;
      const key = btn.getAttribute('data-json');
      let payload = null;
      try { payload = JSON.parse(btn.dataset.payload || 'null'); } catch { payload = btn.dataset.payload; }
      qs('#jsonTitle').textContent = key || 'Record JSON';
      qs('#jsonContent').textContent = JSON.stringify(payload, null, 2);
      modal.classList.add('open');
    });
  }

  function recordPayload(entry) {
    return escapeHtml(JSON.stringify(entry?.record || entry || {}));
  }

  function renderProcessedRow(entry) {
    const r = entry?.record || {};
    return `<tr>
      <td>${escapeHtml(fmtDateTime(r.processed_at || r.event_timestamp || entry?.received_at))}</td>
      <td class="mono">${escapeHtml(r.order_id || entry?.kafka?.key || '--')}</td>
      <td>${badge(r.event_type || 'UNKNOWN', statusTone(r.event_type))}</td>
      <td>${escapeHtml(STAGE_LABELS[r.previous_status] || r.previous_status || '--')}</td>
      <td>${escapeHtml(STAGE_LABELS[r.current_status] || r.current_status || '--')}</td>
      <td>${escapeHtml(fmtDurationSeconds(r.stage_duration_seconds))}</td>
      <td>${escapeHtml(fmtDurationSeconds(r.total_delivery_duration_seconds))}</td>
      <td><button class="icon-btn json-btn" data-json="Processed event" data-payload="${recordPayload(entry)}" title="View JSON">{}</button></td>
    </tr>`;
  }

  function renderStateRow(entry) {
    const r = entry?.record || {};
    return `<tr>
      <td class="mono">${escapeHtml(r.order_id || entry?.kafka?.key || '--')}</td>
      <td>${badge(STAGE_LABELS[r.current_status] || r.current_status || 'Unknown', statusTone(r.current_status))}</td>
      <td>${escapeHtml(STAGE_LABELS[r.previous_status] || r.previous_status || '--')}</td>
      <td>${escapeHtml(fmtDateTime(r.updated_at || r.last_event_timestamp || entry?.received_at))}</td>
      <td>${escapeHtml(fmtDurationSeconds(r.total_delivery_duration_seconds))}</td>
      <td><button class="icon-btn json-btn" data-json="Order state" data-payload="${recordPayload(entry)}" title="View JSON">{}</button></td>
    </tr>`;
  }

  function renderExceptionRow(entry, includeCategory = false) {
    const r = entry?.record || {};
    const type = r.exception_type || r.event_type || (entry?.is_delay_alert ? 'DELAY_ALERT' : 'EXCEPTION');
    const tone = type === 'DELAY_ALERT' ? 'yellow' : 'red';
    return `<tr>
      <td>${escapeHtml(fmtDateTime(r.detected_at || r.event_timestamp || entry?.received_at))}</td>
      <td class="mono">${escapeHtml(r.order_id || '--')}</td>
      <td>${badge(type, tone)}</td>
      <td>${escapeHtml(r.alert_type || (entry?.is_delay_alert ? 'Delay' : '--'))}</td>
      ${includeCategory ? `<td>${escapeHtml(r.exception_category || (entry?.is_delay_alert ? 'DELAY_ALERTS' : 'PROCESSING'))}</td>` : ''}
      <td class="error-cell">${escapeHtml(r.error_message || r.message || r.details || '--')}</td>
      <td><button class="icon-btn json-btn" data-json="Exception record" data-payload="${recordPayload(entry)}" title="View JSON">{}</button></td>
    </tr>`;
  }

  function setTableBody(ids, rowsHtml, colCount, emptyText) {
    ids.forEach(id => {
      const el = qs(`#${id}`);
      if (!el) return;
      el.innerHTML = rowsHtml || `<tr class="empty-row"><td colspan="${colCount}">${escapeHtml(emptyText)}</td></tr>`;
    });
  }

  function renderEventTables() {
    const processedHtml = state.processed.slice(0, 120).map(renderProcessedRow).join('');
    const statesHtml = state.states.slice(0, 120).map(renderStateRow).join('');
    const exceptionsHtml = state.exceptions.slice(0, 120).map(e => renderExceptionRow(e, false)).join('');
    const exceptionsHtmlCat = state.exceptions.slice(0, 120).map(e => renderExceptionRow(e, true)).join('');

    setTableBody(['ovProcessedBody', 'liveProcessedBody'], processedHtml, 8, 'No processed events observed yet.');
    setTableBody(['ovStatesBody', 'liveStatesBody'], statesHtml, 6, 'No order states observed yet.');
    setTableBody(['ovExceptionsBody'], exceptionsHtml, 7, 'No exceptions or alerts observed yet.');
    setTableBody(['liveExceptionsBody', 'exceptionsPageBody'], exceptionsHtmlCat, 8, 'No exceptions or alerts observed yet.');
  }

  function renderMetrics(metrics) {
    if (!metrics) return;
    qs('#kpiTotal').textContent = String(metrics.total_events ?? 0);
    qs('#kpiProcessed').textContent = String(metrics.processed_events ?? 0);
    qs('#kpiActive').textContent = String(metrics.active_orders ?? 0);
    qs('#kpiExceptions').textContent = String(metrics.exceptions ?? 0);
    qs('#kpiAlerts').textContent = String(metrics.delay_alerts ?? 0);
    qs('#exceptionCount').textContent = String(metrics.exceptions ?? 0);
    qs('#delayAlertCount').textContent = String(metrics.delay_alerts ?? 0);

    const consumers = metrics.consumers || [];
    const connected = consumers.filter(c => c.connected).length;
    const health = connected === consumers.length && consumers.length ? 'healthy' : connected ? 'warning' : 'offline';
    qsa('[data-health-text="consumers"]').forEach(el => {
      el.textContent = consumers.length ? `${connected}/${consumers.length} connected` : 'Unavailable';
      el.className = `status-text ${health}`;
    });
    qsa('[data-health="consumers"]').forEach(el => el.className = `health-dot ${health}`);
    const map = { state: 'states', processed: 'processed', exceptions: 'exceptions' };
    consumers.forEach(c => {
      const el = qs(`[data-consumer="${c.kind}"]`);
      if (!el) return;
      const label = c.connected ? (c.mode === 'manual' ? 'Connected (manual)' : 'Connected') : (c.state || 'Disconnected');
      el.textContent = label;
      el.className = `mini-status ${c.connected ? 'healthy' : c.state === 'reconnecting' ? 'warning' : 'offline'}`;
    });
    qs('#exceptionsGroup').textContent = state.config?.consumer_groups?.exceptions || '--';
  }

  function renderHealthDots(infra) {
    const checks = infra?.pipeline?.checks || {};
    const docker = infra?.docker || {};
    const kafka = infra?.kafka || {};
    const flink = infra?.flink || {};
    const job = flink.job;
    const dockerStates = Object.fromEntries((docker.containers || []).map(c => [c.name, c]));

    const dot = (name, ok, warn = false) => {
      qsa(`[data-health="${name}"]`).forEach(el => el.className = `health-dot ${ok ? 'healthy' : warn ? 'warning' : 'offline'}`);
    };
    dot('docker', !!docker.available);
    dot('kafka', !!kafka.available, dockerStates[state.config?.kafka_container || 'kafka_kraft_docker']?.running);
    dot('generator', !!state.generator?.running, false);
    dot('flink', !!job?.running, !!flink.available);
    dot('jobmanager', !!dockerStates[state.config?.flink_jobmanager_container || 'flink-jobmanager']?.running, !!flink.available);
    dot('taskmanager', !!dockerStates[state.config?.flink_taskmanager_container || 'flink-taskmanager']?.running, !!flink.available);
    dot('topics', !!checks.topics, !!kafka.available);
    dot('consumers', !!checks.dashboard_consumers, false);

    qsa('[data-health-text="kafka"]').forEach(el => {
      el.textContent = kafka.available ? 'Broker reachable' : 'Unavailable';
      el.className = `status-text ${kafka.available ? 'healthy' : 'offline'}`;
    });
    qsa('[data-health-text="flink"]').forEach(el => {
      const txt = job?.running ? `Running (${shortId(job.job_id || '')})` : flink.available ? 'No running job' : 'Unavailable';
      el.textContent = txt;
      el.className = `status-text ${job?.running ? 'healthy' : flink.available ? 'warning' : 'offline'}`;
    });

    qs('#generatorFlowStatus').textContent = state.generator?.running ? `${SCENARIO_HELP[state.generator.scenario] ? state.generator.scenario.replace(/_/g, ' ') : 'Running'}` : 'Ready';
    qs('#kafkaFlowStatus').textContent = kafka.available ? `${kafka.bootstrap_servers}` : (kafka.error || 'Broker unavailable');
    qs('#flinkFlowStatus').textContent = job?.running ? `${job.state} · ${fmtDurationSeconds(job.duration_seconds)}` : (flink.error || 'No running job');

    const ckpt = infra?.last_checkpoint || flink?.checkpoints?.latest?.completed;
    qs('#overviewCheckpoint').textContent = ckpt ? `#${ckpt.id} ${fmtTime(ckpt.latest_ack_timestamp || ckpt.trigger_timestamp)}` : 'None yet';
  }

  function renderInfrastructure(infra) {
    if (!infra) return;
    setPipeline(infra);
    renderHealthDots(infra);

    const docker = infra.docker || {};
    const kafka = infra.kafka || {};
    const flink = infra.flink || {};
    const map = Object.fromEntries((docker.containers || []).map(c => [c.name, c]));
    const kName = state.config?.kafka_container || 'kafka_kraft_docker';
    const jm = state.config?.flink_jobmanager_container || 'flink-jobmanager';
    const tm = state.config?.flink_taskmanager_container || 'flink-taskmanager';

    const setSvc = (id, detailId, name) => {
      const c = map[name];
      if (qs(`#${id}`)) qs(`#${id}`).textContent = c ? (c.running ? 'Running' : c.status || 'Stopped') : 'Missing';
      if (detailId && qs(`#${detailId}`)) qs(`#${detailId}`).textContent = c?.health ? `Health: ${c.health}` : (c?.started_at ? `Started ${fmtDateTime(c.started_at)}` : (docker.error || '--'));
    };
    if (qs('#infraDocker')) qs('#infraDocker').textContent = docker.available ? `Available ${docker.version || ''}`.trim() : 'Unavailable';
    if (qs('#infraDockerDetail')) qs('#infraDockerDetail').textContent = docker.error || 'Docker Engine reachable';
    setSvc('infraKafka', 'infraKafkaDetail', kName);
    setSvc('infraJm', null, jm);
    setSvc('infraTm', 'infraTmDetail', tm);

    const job = flink.job;
    if (qs('#infraJob')) qs('#infraJob').textContent = job?.running ? job.state : (flink.available ? 'No running job' : 'Unavailable');
    if (qs('#infraJobDetail')) qs('#infraJobDetail').textContent = job?.job_id ? `Job ${shortId(job.job_id)}` : (flink.error || '--');

    qs('#flinkRestUrl').textContent = flink.rest_url || '--';
    qs('#flinkVersion').textContent = flink.flink_version || '--';
    qs('#flinkJobId').textContent = job?.job_id || '--';
    qs('#flinkJobsRunning').textContent = String(flink.jobs_running ?? 0);
    qs('#flinkJobsFinished').textContent = String(flink.jobs_finished ?? 0);
    qs('#flinkJobState').textContent = job?.state || (flink.available ? 'NO JOB' : 'N/A');
    qs('#flinkJobName').textContent = job?.name || state.config?.flink_job_name || 'Ecommerce Order Lifecycle Processor';
    qs('#flinkDuration').textContent = job ? fmtDurationSeconds(job.duration_seconds) : 'N/A';
    const tasks = job?.tasks || {};
    qs('#flinkTasks').textContent = job ? `${tasks.running || 0} / ${tasks.total || 0}` : 'N/A';
    qs('#flinkTaskmanagers').textContent = String(flink.taskmanagers ?? 0);
    qs('#flinkSlots').textContent = `Slots ${flink.slots_available ?? 0} available / ${flink.slots_total ?? 0} total`;

    renderCheckpoints(flink.checkpoints);
    renderTopics(kafka);
    renderConsumers(infra.dashboard_consumers);
  }

  function renderCheckpoints(checkpoints) {
    const latest = checkpoints?.latest?.completed || null;
    const counts = checkpoints?.counts || {};
    qs('#checkpointId').textContent = latest?.id != null ? `#${latest.id}` : 'N/A';
    qs('#checkpointTime').textContent = latest ? fmtDateTime(latest.latest_ack_timestamp || latest.trigger_timestamp) : '--';
    qs('#checkpointStatus').textContent = latest?.status || (checkpoints?.available ? 'No completed checkpoint yet' : 'Unavailable');
    qs('#checkpointChip').className = `status-chip small ${latest ? 'healthy' : checkpoints?.available ? 'warning' : 'offline'}`;
    qs('#checkpointSize').textContent = latest ? fmtBytes(latest.state_size ?? latest.checkpointed_size) : 'N/A';
    qs('#checkpointDuration').textContent = latest ? `End-to-end ${fmtMillis(latest.end_to_end_duration)}` : '--';
    qs('#checkpointCompleted').textContent = String(counts.completed ?? 0);
    const restored = checkpoints?.latest?.restored;
    qs('#checkpointRestored').textContent = restored ? `Restored #${restored.id}` : 'Not restored';

    const rows = (checkpoints?.history || []).slice(0, 10).map(c => `<tr>
      <td>${escapeHtml(c.id ?? '--')}</td>
      <td>${badge(c.status || '--', statusTone(c.status))}</td>
      <td>${escapeHtml(fmtDateTime(c.trigger_timestamp))}</td>
      <td>${escapeHtml(fmtBytes(c.state_size ?? c.checkpointed_size))}</td>
      <td>${escapeHtml(fmtMillis(c.end_to_end_duration))}</td>
    </tr>`).join('');
    setTableBody(['checkpointHistoryBody'], rows, 5, 'Checkpoint history unavailable.');
  }

  function renderTopics(kafka) {
    qs('#kafkaBootstrap').textContent = kafka?.bootstrap_servers || '--';
    const rows = (kafka?.topics || []).map(t => `<tr>
      <td class="mono">${escapeHtml(t.name)}</td>
      <td>${badge(t.exists ? 'Yes' : 'No', t.exists ? 'green' : 'red')}</td>
      <td>${escapeHtml(t.partitions ?? 0)}</td>
      <td>${escapeHtml(t.replication_factor ?? 0)}</td>
      <td>${badge(t.in_sync ? 'In sync' : 'No', t.in_sync ? 'green' : 'yellow')}</td>
    </tr>`).join('');
    setTableBody(['topicRows'], rows, 5, kafka?.available ? 'No project topics found.' : 'Kafka metadata unavailable.');
  }

  function renderConsumers(consumerHealth) {
    const el = qs('#consumerList');
    if (!el) return;
    const consumers = consumerHealth?.consumers || [];
    if (!consumers.length) { el.innerHTML = ''; return; }
    el.innerHTML = consumers.map(c => `<div class="consumer-row"><span class="mono">${escapeHtml(c.group_id || c.topic || c.kind)}</span><strong>${escapeHtml(c.connected ? `Connected (${c.mode || 'group'})` : (c.state || 'Disconnected'))}</strong></div>`).join('');
  }

  function renderGenerator(status) {
    if (!status) return;
    state.generator = status;
    const running = !!status.running;
    qs('#generatorStateText').textContent = running ? (status.mode === 'continuous' ? 'Continuous running' : 'Running') : (status.exit_code == null ? 'Idle' : (status.exit_code === 0 ? 'Completed' : 'Stopped with error'));
    qs('#generatorStatusChip').className = `status-chip small ${running ? 'healthy' : status.exit_code > 0 ? 'warning' : 'neutral'}`;
    qs('#generatorFlowStatus').textContent = running ? `${(status.scenario || 'generator').replace(/_/g, ' ')} active` : 'Ready';
    qsa('[data-health="generator"]').forEach(el => el.className = `health-dot ${running ? 'healthy' : 'neutral'}`);
    qs('#generatorLastRun').textContent = status.last_execution_at ? fmtDateTime(status.last_execution_at) : '--';
    qs('#generatorEvents').textContent = String(status.events_generated ?? 0);
    qs('#generatorErrors').textContent = String(status.errors ?? 0);
    const help = SCENARIO_HELP[qs('#generatorScenario')?.value] || SCENARIO_HELP[status.scenario] || '';
    if (qs('#generatorScenarioHelp')) qs('#generatorScenarioHelp').textContent = help;
    if (qs('#generatorRunBtn')) qs('#generatorRunBtn').disabled = running;
    if (qs('#generatorStartBtn')) qs('#generatorStartBtn').disabled = running;
    if (qs('#generatorStopBtn')) qs('#generatorStopBtn').disabled = !running;
  }

  function renderGeneratorLogs(logs) {
    const el = qs('#generatorLogs');
    if (!el) return;
    if (!logs || !logs.length) {
      el.textContent = 'Generator output will appear here.';
      return;
    }
    el.textContent = logs.slice(0, 30).map(l => `[${fmtTime(l.timestamp)}] ${l.level} ${l.message}`).join('\n');
  }

  function renderActivity(logs) {
    state.activities = logs || [];
    const html = state.activities.length ? state.activities.map(l => {
      const level = String(l.level || 'INFO').toLowerCase();
      const cls = level === 'error' ? 'error' : level === 'warning' || level === 'warn' ? 'warn' : 'info';
      const detail = l.detail ? ` — ${escapeHtml(l.detail)}` : '';
      return `<div class="log-line"><time>${escapeHtml(fmtTime(l.timestamp))}</time><span class="log-level ${cls}">${escapeHtml(l.level || 'INFO')}</span><span class="log-source">${escapeHtml(l.source || '')}</span><span class="log-message">${escapeHtml(l.message || '')}${detail}</span></div>`;
    }).join('') : '<div class="log-line"><span class="log-message">No activity yet.</span></div>';
    ['activityLog', 'activityLogLive'].forEach(id => { if (qs(`#${id}`)) qs(`#${id}`).innerHTML = html; });
  }

  function renderOrderOptions() {
    const orders = state.orders || [];
    const options = ['<option value="">Select an order</option>', ...orders.map(o => `<option value="${escapeHtml(o.order_id)}">${escapeHtml(o.order_id)} · ${escapeHtml(STAGE_LABELS[o.current_status] || o.current_status || 'Unknown')}</option>`)].join('');
    ['overviewOrderSelect', 'ordersOrderSelect'].forEach(id => {
      const sel = qs(`#${id}`);
      if (!sel) return;
      const prev = sel.value || state.selectedOrderId || '';
      sel.innerHTML = options;
      if (prev && orders.some(o => o.order_id === prev)) sel.value = prev;
    });
  }

  function renderOrdersList() {
    const rows = (state.orders || []).map(o => `<tr data-order-id="${escapeHtml(o.order_id)}" style="cursor:pointer">
      <td class="mono">${escapeHtml(o.order_id)}</td>
      <td>${badge(STAGE_LABELS[o.current_status] || o.current_status || 'Unknown', statusTone(o.current_status))}</td>
      <td>${escapeHtml(fmtDateTime(o.updated_at || o.last_event_timestamp))}</td>
    </tr>`).join('');
    setTableBody(['ordersList'], rows, 3, 'No orders observed yet.');
  }

  function renderOrderDetail(detail, prefix) {
    const summaryEl = qs(`#${prefix}Summary`);
    const timelineEl = qs(`#${prefix}Timeline`);
    if (!detail || !detail.found) {
      if (summaryEl) { summaryEl.className = 'order-summary empty'; summaryEl.textContent = detail?.message || 'No retained data was found for this order.'; }
      if (timelineEl) { timelineEl.className = 'timeline empty-timeline'; timelineEl.textContent = 'No timeline available.'; }
      if (prefix === 'orders') setTableBody(['ordersEventsBody'], '', 6, 'No processed lifecycle events found.');
      return;
    }
    const s = detail.summary || {};
    if (summaryEl) {
      summaryEl.className = 'order-summary';
      summaryEl.innerHTML = `<div class="summary-head"><div><strong>${escapeHtml(detail.order_id)}</strong><div><small>Current status: ${escapeHtml(STAGE_LABELS[s.current_status] || s.current_status || 'Unknown')}</small></div></div>${badge(s.cancelled ? 'Cancelled' : s.delivered ? 'Delivered' : 'Active', s.cancelled ? 'red' : s.delivered ? 'green' : 'blue')}</div>
        <div class="summary-grid">
          <div class="summary-item"><span>Previous status</span><strong>${escapeHtml(STAGE_LABELS[s.previous_status] || s.previous_status || '--')}</strong></div>
          <div class="summary-item"><span>Total delivery</span><strong>${escapeHtml(fmtDurationSeconds(s.total_delivery_duration_seconds))}</strong></div>
          <div class="summary-item"><span>Last event</span><strong>${escapeHtml(fmtDateTime(s.last_event_timestamp))}</strong></div>
          <div class="summary-item"><span>Processed events</span><strong>${escapeHtml(s.processed_event_count ?? 0)}</strong></div>
        </div>`;
    }
    if (timelineEl) {
      const stages = detail.stages || [];
      const parts = stages.map(stage => {
        const cls = stage.status === 'completed' ? '' : stage.status === 'not_reached' ? 'danger' : stage.status === 'pending' ? '' : 'warning';
        const statusLabel = stage.status === 'completed' ? 'Completed' : stage.status === 'pending' ? 'Pending' : stage.status === 'not_reached' ? 'Not reached' : 'State only / unknown timing';
        const dur = stage.stage_duration_seconds != null ? `<span class="stage-duration">Stage ${fmtDurationSeconds(stage.stage_duration_seconds)}</span>` : '';
        return `<div class="tl-item ${cls}"><div class="tl-marker"></div><div class="tl-content"><div class="tl-top"><strong>${escapeHtml(stage.label || stage.stage)}</strong><time>${escapeHtml(stage.timestamp ? fmtDateTime(stage.timestamp) : '')}</time></div><p>${escapeHtml(statusLabel)} ${stage.source ? `· ${stage.source}` : ''} ${dur}</p></div></div>`;
      });
      if (detail.cancellation) {
        parts.push(`<div class="tl-item danger"><div class="tl-marker"></div><div class="tl-content"><div class="tl-top"><strong>Order cancelled</strong><time>${escapeHtml(fmtDateTime(detail.cancellation.timestamp))}</time></div><p>Lifecycle ended without delivery.</p></div></div>`);
      }
      timelineEl.className = 'timeline';
      timelineEl.innerHTML = parts.join('') || '<div class="timeline empty-timeline">No timeline stages available.</div>';
    }
    if (prefix === 'orders') {
      const evRows = (detail.processed_events || []).map(e => {
        const r = e.record || {};
        return `<tr><td>${escapeHtml(fmtDateTime(r.event_timestamp || r.processed_at))}</td><td>${badge(r.event_type || '--', statusTone(r.event_type))}</td><td>${escapeHtml(STAGE_LABELS[r.previous_status] || r.previous_status || '--')}</td><td>${escapeHtml(STAGE_LABELS[r.current_status] || r.current_status || '--')}</td><td>${escapeHtml(fmtDurationSeconds(r.stage_duration_seconds))}</td><td><button class="icon-btn json-btn" data-json="Processed event" data-payload="${recordPayload(e)}">{}</button></td></tr>`;
      }).join('');
      setTableBody(['ordersEventsBody'], evRows, 6, 'No processed lifecycle events retained for this order.');
    }
  }

  function renderAll() {
    renderEventTables();
    renderOrderOptions();
    renderOrdersList();
  }

  async function loadConfig() {
    try {
      state.config = await api('/api/config');
      qs('#exceptionsGroup').textContent = state.config?.consumer_groups?.exceptions || '--';
    } catch (e) { console.warn('config', e); }
  }

  async function loadInfrastructure() {
    try {
      const infra = await api('/api/infrastructure/status');
      renderInfrastructure(infra);
      updateLastUpdate();
    } catch (e) {
      renderInfrastructure({ pipeline: { status: 'offline', label: 'Status unavailable' }, docker: { available: false, error: e.message, containers: [] }, kafka: { available: false, error: e.message, topics: [] }, flink: { available: false, error: e.message }, dashboard_consumers: { consumers: [], connected: 0, total: 0 } });
    }
  }

  async function loadRecentEvents() {
    try {
      const data = await api('/api/events/recent?limit=120');
      state.processed = data.processed || [];
      state.states = data.states || [];
      state.exceptions = data.exceptions || [];
      renderEventTables();
    } catch (e) { console.warn('events', e); }
  }

  async function loadOrders() {
    try {
      const search = qs('#orderSearchInput')?.value?.trim() || '';
      const activeOnly = !!qs('#activeOrdersOnly')?.checked;
      const params = new URLSearchParams({ limit: '150' });
      if (search) params.set('search', search);
      if (activeOnly) params.set('active_only', 'true');
      const data = await api(`/api/orders?${params.toString()}`);
      state.orders = data.orders || [];
      renderOrderOptions();
      renderOrdersList();
    } catch (e) { console.warn('orders', e); }
  }

  async function loadOrder(orderId) {
    const id = orderId || state.selectedOrderId;
    if (!id) return;
    state.selectedOrderId = id;
    ['overviewOrderSelect', 'ordersOrderSelect'].forEach(s => { if (qs(`#${s}`)) qs(`#${s}`).value = id; });
    try {
      const detail = await api(`/api/orders/${encodeURIComponent(id)}`);
      renderOrderDetail(detail, 'overview');
      renderOrderDetail(detail, 'orders');
    } catch (e) {
      renderOrderDetail({ found: false, message: e.message }, 'overview');
      renderOrderDetail({ found: false, message: e.message }, 'orders');
    }
  }

  async function loadGenerator() {
    try {
      const status = await api('/api/generator/status');
      renderGenerator(status);
      const logs = await api('/api/generator/logs?limit=40');
      renderGeneratorLogs(logs.logs || []);
    } catch (e) { console.warn('generator', e); }
  }

  async function loadActivity() {
    try {
      const data = await api('/api/logs/recent?limit=120');
      renderActivity(data.logs || []);
    } catch (e) { console.warn('activity', e); }
  }

  async function refreshAll() {
    await Promise.allSettled([loadInfrastructure(), loadRecentEvents(), loadOrders(), loadGenerator(), loadActivity()]);
    if (state.selectedOrderId) await loadOrder(state.selectedOrderId);
  }

  function mergeEntry(kind, entry) {
    if (!entry) return;
    if (kind === 'processed') {
      state.processed.unshift(entry);
      if (state.processed.length > 200) state.processed.length = 200;
    } else if (kind === 'state') {
      const orderId = entry?.record?.order_id || entry?.kafka?.key;
      if (orderId) {
        const idx = state.states.findIndex(e => (e?.record?.order_id || e?.kafka?.key) === orderId);
        if (idx >= 0) state.states.splice(idx, 1);
      }
      state.states.unshift(entry);
      if (state.states.length > 200) state.states.length = 200;
    } else if (kind === 'exceptions') {
      state.exceptions.unshift(entry);
      if (state.exceptions.length > 200) state.exceptions.length = 200;
    }
    renderEventTables();
  }

  function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const url = `${protocol}://${window.location.host}${WS_PATH}`;
    try { state.ws = new WebSocket(url); } catch { scheduleReconnect(); return; }
    state.ws.onopen = () => {
      setConnection(true, 'Live connected');
      if (state.reconnectTimer) { clearTimeout(state.reconnectTimer); state.reconnectTimer = null; }
    };
    state.ws.onclose = () => {
      setConnection(false, 'Reconnecting');
      scheduleReconnect();
    };
    state.ws.onerror = () => {
      setConnection(false, 'Reconnecting');
    };
    state.ws.onmessage = (event) => {
      let msg = null;
      try { msg = JSON.parse(event.data); } catch { return; }
      if (!msg || !msg.type) return;
      if (msg.type === 'connected') { setConnection(true, 'Live connected'); return; }
      if (msg.type === 'ping' || msg.type === 'pong') return;
      if (msg.type === 'metrics') { renderMetrics(msg.data); updateLastUpdate(); return; }
      if (msg.type === 'activity') { renderActivity([msg.entry, ...(state.activities || [])].slice(0, 150)); return; }
      if (['processed', 'state', 'exceptions'].includes(msg.type)) {
        mergeEntry(msg.type, msg.entry);
        updateLastUpdate();
        if (msg.type === 'state' || msg.type === 'processed') {
          loadOrders().then(() => {
            if (state.selectedOrderId) loadOrder(state.selectedOrderId);
          });
        }
      }
    };
  }

  function scheduleReconnect() {
    if (state.reconnectTimer) return;
    state.reconnectTimer = setTimeout(() => {
      state.reconnectTimer = null;
      connectWebSocket();
    }, RECONNECT_MS);
  }

  async function confirmAction(message) {
    return window.confirm(message);
  }

  async function runAction(fn, successMessage) {
    try {
      const result = await fn();
      toast(successMessage || result?.message || 'Action completed', 'success');
      await refreshAll();
      return result;
    } catch (e) {
      const msg = e?.detail?.message || e.message || 'Action failed';
      toast(msg, 'error');
      return null;
    }
  }

  function initControls() {
    qs('#generatorScenario')?.addEventListener('change', () => {
      qs('#generatorScenarioHelp').textContent = SCENARIO_HELP[qs('#generatorScenario').value] || '';
    });

    qs('#generatorRunBtn')?.addEventListener('click', async () => {
      const scenario = qs('#generatorScenario').value;
      const interval = Number(qs('#generatorInterval').value || 2);
      await runAction(() => api('/api/generator/run', { method: 'POST', body: { scenario, interval } }), `Started ${scenario.replace(/_/g, ' ')}`);
      await loadGenerator();
    });

    qs('#generatorStartBtn')?.addEventListener('click', async () => {
      const interval = Number(qs('#generatorInterval').value || 2);
      await runAction(() => api('/api/generator/start', { method: 'POST', body: { scenario: 'continuous', interval } }), 'Continuous generator started');
      await loadGenerator();
    });

    qs('#generatorStopBtn')?.addEventListener('click', async () => {
      await runAction(() => api('/api/generator/stop', { method: 'POST' }), 'Generator stopped');
      await loadGenerator();
    });

    qs('#startInfraBtn')?.addEventListener('click', async () => {
      if (!await confirmAction('Start Kafka and Flink infrastructure containers for this project?')) return;
      await runAction(() => api('/api/infrastructure/start', { method: 'POST' }), 'Infrastructure start requested');
    });
    qs('#stopInfraBtn')?.addEventListener('click', async () => {
      if (!await confirmAction('Stop Kafka and Flink containers for this project? Persistent checkpoint data will not be deleted.')) return;
      await runAction(() => api('/api/infrastructure/stop', { method: 'POST' }), 'Infrastructure stop requested');
    });
    const submitFlink = async () => {
      await runAction(() => api('/api/flink/submit', { method: 'POST' }), 'Flink submit requested');
    };
    const stopFlink = async () => {
      if (!await confirmAction('Cancel the running project Flink job? This stops only the project job, not the Flink cluster.')) return;
      await runAction(() => api('/api/flink/stop', { method: 'POST' }), 'Flink stop requested');
    };
    qs('#submitFlinkBtn')?.addEventListener('click', submitFlink);
    qs('#flinkSubmitBtn')?.addEventListener('click', submitFlink);
    qs('#stopFlinkBtn')?.addEventListener('click', stopFlink);
    qs('#flinkStopBtn')?.addEventListener('click', stopFlink);

    qs('#refreshInfraBtn')?.addEventListener('click', () => loadInfrastructure());
    qs('#refreshFlinkBtn')?.addEventListener('click', () => loadInfrastructure());
    qs('#flinkRefreshBtn')?.addEventListener('click', () => loadInfrastructure());
    qs('#eventsRefreshBtn')?.addEventListener('click', () => { loadRecentEvents(); loadActivity(); });
    qs('#activityRefreshBtn')?.addEventListener('click', loadActivity);
    qs('#activityRefreshBtn2')?.addEventListener('click', loadActivity);
    qs('#ordersRefreshBtn')?.addEventListener('click', loadOrders);
    qs('#exceptionsRefreshBtn')?.addEventListener('click', loadRecentEvents);

    qs('#overviewOrderLoad')?.addEventListener('click', () => loadOrder(qs('#overviewOrderSelect')?.value));
    qs('#ordersOrderLoad')?.addEventListener('click', () => loadOrder(qs('#ordersOrderSelect')?.value));
    qs('#overviewOrderSelect')?.addEventListener('change', (e) => { if (e.target.value) loadOrder(e.target.value); });
    qs('#ordersOrderSelect')?.addEventListener('change', (e) => { if (e.target.value) loadOrder(e.target.value); });
    qs('#orderSearchInput')?.addEventListener('input', () => { window.clearTimeout(state.orderSearchTimer); state.orderSearchTimer = window.setTimeout(loadOrders, 250); });
    qs('#activeOrdersOnly')?.addEventListener('change', loadOrders);
    qs('#ordersList')?.addEventListener('click', (e) => {
      const row = e.target.closest('[data-order-id]');
      if (!row) return;
      loadOrder(row.dataset.orderId);
    });
  }

  async function init() {
    initNav();
    initTabs();
    initModal();
    initControls();
    setMode('demo');
    setPage('overview');
    setConnection(false, 'Connecting');
    await loadConfig();
    await refreshAll();
    connectWebSocket();
    state.pollTimer = setInterval(() => {
      loadInfrastructure();
      loadGenerator();
      if (state.page === 'overview' || state.page === 'events' || state.page === 'exceptions') loadRecentEvents();
      if (state.page === 'orders') loadOrders();
      loadActivity();
    }, POLL_MS);
  }

  document.addEventListener('DOMContentLoaded', init);
})();
