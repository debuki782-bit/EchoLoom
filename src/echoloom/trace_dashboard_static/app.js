const $ = (s) => document.querySelector(s);
const state = {brandName: 'EchoLoom', overview: null, conversation: '', view: 'chat', timelineOffset: 0, timelineTotal: 0, callsOffset: 0, callsTotal: 0, calls: [], senders: [], requestId: 0};
function el(tag, cls, text) { const n = document.createElement(tag); if (cls) n.className = cls; if (text !== undefined) n.textContent = String(text); return n; }
function clear(node) { node.replaceChildren(); }
function json(value) { return JSON.stringify(value, null, 2) ?? ''; }
function shortTime(value) { return value && value !== '?' ? value.slice(5, 16) : '时间未知'; }
function toast(text) { const n = $('#toast'); n.textContent = text; n.classList.remove('hidden'); clearTimeout(toast.timer); toast.timer = setTimeout(() => n.classList.add('hidden'), 3200); }
async function api(url, options) { const r = await fetch(url, options); if (!r.ok) { let message = `HTTP ${r.status}`; try { message = (await r.json()).detail || message; } catch {} throw new Error(message); } return r.json(); }
function chatTitle(id) { return id.startsWith('idle-event:') ? `空闲活动 · ${id.slice(-8)}` : id || '未知会话'; }
function chatType(id) { return id.startsWith('idle-event:') ? '空闲活动' : (id.endsWith('@chatroom') || /^(room|group|channel|thread|guild):/i.test(id)) ? '群聊' : '私聊'; }
function setView(view) { state.view = view; document.querySelectorAll('.view').forEach(n => n.classList.add('hidden')); $(`#view-${view}`).classList.remove('hidden'); document.querySelectorAll('.nav').forEach(n => n.classList.toggle('active', n.dataset.view === view)); if (view === 'calls' && !state.calls.length) loadCalls(true); if (view === 'senders' && !state.senders.length) loadSenders(); }
function renderChats() {
  const list = $('#chat-list'); clear(list); const q = $('#chat-search').value.toLowerCase();
  const rows = (state.overview?.conversations || []).filter(c => c.id && `${c.id} ${chatTitle(c.id)}`.toLowerCase().includes(q));
  for (const group of ['会话', '空闲活动']) {
    const items = rows.filter(r => group === '空闲活动' ? r.id.startsWith('idle-event:') : !r.id.startsWith('idle-event:'));
    if (!items.length) continue;
    list.append(el('div', 'chat-group-label', group));
    for (const row of items) {
      const button = el('button', `chat-item${state.conversation === row.id ? ' active' : ''}`); button.type = 'button';
      button.append(el('div', 'chat-title', chatTitle(row.id)), el('div', 'chat-sub', `${chatType(row.id)} · ${shortTime(row.last)}`));
      button.addEventListener('click', () => selectChat(row.id)); list.append(button);
    }
  }
  if (!list.childElementCount) list.append(el('div', 'side-empty', '没有匹配的聊天'));
  $('#chat-count').textContent = rows.length;
}
function details(label, value) { const d = el('details', 'raw-detail'); d.append(el('summary', '', label)); d.append(el('pre', '', typeof value === 'string' ? value : json(value))); return d; }
function bodyText(content) {
  const text = el('div', 'message-text', content);
  if (content.length < 650 && content.split('\n').length < 15) return text;
  text.classList.add('collapsed'); const wrapper = el('div'); wrapper.append(text);
  const toggle = el('button', 'raw-action', '展开全文 ↓'); toggle.addEventListener('click', () => { text.classList.toggle('collapsed'); toggle.textContent = text.classList.contains('collapsed') ? '展开全文 ↓' : '收起 ↑'; }); wrapper.append(toggle); return wrapper;
}
function message(content, who, time, extra, sender) {
  const row = el('article', `message ${who}`); if (who === 'assistant') row.append(el('div', 'message-avatar', '✳'));
  const body = el('div', 'message-body'), label = el('div', 'message-label');
  label.append(el('span', '', who === 'assistant' ? state.brandName : (sender || '本轮输入')), el('span', 'event-time', time)); body.append(label, bodyText(content));
  if (extra) body.append(extra); row.append(body); return row;
}
function resultLabel(entry) {
  if (entry.result === null || entry.result === undefined) return '等待结果 / 未记录';
  const r = entry.result; const status = r?.reply?.status || r?.status;
  if (status === 'queued') return '已排队 · 未确认送达';
  if (status === 'accepted_by_server') return '服务器已接受 · 未确认送达';
  if (status) return String(status);
  if (r?.ok === true) return '完成'; if (r?.ok === false) return '失败'; return '已返回';
}
function toolCard(entry, global = false) {
  const d = el('details', 'tool-card'); if (global) d.classList.add('global-tool');
  const sum = el('summary'); sum.append(el('span', 'tool-icon', '⌘'), el('span', 'tool-name', entry.name || '未知工具'), el('span', 'tool-result', resultLabel(entry)), el('span', 'event-time', shortTime(entry.time))); d.append(sum);
  const content = el('div', 'tool-content');
  if (entry.args !== undefined) content.append(el('div', 'tool-section', '调用参数'), el('pre', '', json(entry.args)));
  if (entry.result !== null && entry.result !== undefined) content.append(el('div', 'tool-section', '工具结果'), el('pre', '', json(entry.result)));
  else content.append(el('div', 'meta-note', '没有记录到对应结果。'));
  if (entry.error) content.append(el('div', 'meta-note', `运行状态：${entry.error}`));
  d.append(content); return d;
}
function transcriptNodes(entry) {
  const nodes = [];
  if (entry.kind === 'input') {
    if (entry.messages?.length) {
      entry.messages.forEach((m, i) => { const raw = i === entry.messages.length - 1 ? details('查看 trace 中的完整输入', entry.raw) : null; const sender = m.sender.includes('|') ? m.sender.split('|').slice(1).join('|').trim() : m.sender; nodes.push(message(m.text, 'user', shortTime(entry.time), raw, sender)); });
    } else {
      const preview = entry.raw.length > 500 ? (entry.conversation_id.startsWith('idle-event:') ? '空闲维护上下文' : '模型输入上下文') : entry.raw;
      nodes.push(message(preview, 'user', shortTime(entry.time), preview === entry.raw ? null : details('查看 trace 中的完整输入', entry.raw)));
    }
  } else if (entry.kind === 'assistant') {
    nodes.push(message(entry.text, 'assistant', shortTime(entry.time)));
  } else if (entry.kind === 'tool' || entry.kind === 'tool_result') {
    const reply = entry.name === 'commit_agent_turn' && entry.args?.reply_text;
    if (reply) { const note = el('div', 'meta-note', `发送工具回执：${resultLabel(entry)}`); nodes.push(message(reply, 'assistant', shortTime(entry.time), note)); }
    nodes.push(toolCard(entry));
  } else if (entry.kind === 'request') {
    const d = el('details', 'request-card'), s = el('summary'); s.append(el('span', 'tool-icon', '◷'), el('span', '', `模型请求 · ${entry.model || '未记录模型'}`), el('span', 'tool-result', shortTime(entry.time))); d.append(s);
    const content = el('div', 'tool-content'); content.append(el('div', 'tool-section', 'trace 中的请求快照（含历史与动态上下文；写入时可能已清洗或截断）'), el('pre', '', json(entry.contents))); d.append(content); nodes.push(d);
  } else if (entry.kind === 'error') nodes.push(el('div', 'error-card', `运行状态：${entry.error}`));
  return nodes;
}
async function refresh() {
  try {
    state.overview = await api('/api/overview');
    if (state.overview?.brand_name) {
      state.brandName = String(state.overview.brand_name);
      document.title = `${state.brandName} · 对话与运行记录`;
      const brandNode = $('#brand-name');
      if (brandNode) brandNode.textContent = state.brandName;
    }
    const p = state.overview.pilot;
    $('#agent-status').textContent = !p ? 'Agent 未连接' : p.paused ? 'Agent 已暂停' : p.trusted_gateway ? 'Agent 运行中' : 'Agent 初始化中';
    $('.status-dot').classList.toggle('off', !p?.trusted_gateway || p?.paused);
    renderChats(); renderTools();
    if (!state.conversation) { const first = state.overview.conversations.find(c => c.id && !c.id.startsWith('idle-event:')) || state.overview.conversations.find(c => c.id); if (first) await selectChat(first.id); }
  } catch (error) { toast(`加载失败：${error.message}`); }
}
async function selectChat(id) {
  state.conversation = id; state.timelineOffset = 0; state.requestId++; setView('chat'); renderChats();
  $('#selected-chat').textContent = chatTitle(id); const row = state.overview.conversations.find(x => x.id === id);
  $('#selected-meta').textContent = row ? `${chatType(id)} · ${row.sessions} 代 Session · ${row.tool_calls} 次工具调用 · ${row.first} 至 ${row.last}` : id;
  $('#export-jsonl').href = `/api/records.jsonl?${new URLSearchParams({conversation: id})}`; $('#export-jsonl').classList.remove('hidden');
  await loadTimeline(true);
}
async function loadTimeline(reset) {
  if (!state.conversation) return; const id = ++state.requestId; const scroll = $('#chat-scroll');
  const previousHeight = scroll.scrollHeight; if (reset) { state.timelineOffset = 0; clear($('#timeline-list')); }
  const params = new URLSearchParams({conversation: state.conversation, kind: $('#event-filter').value, offset: state.timelineOffset, limit: 50});
  try {
    const data = await api(`/api/transcript?${params}`); if (id !== state.requestId) return;
    const fragment = document.createDocumentFragment(); let previous = '';
    for (const entry of data.entries) {
      if (entry.session_id && previous && entry.session_id !== previous) fragment.append(el('div', 'history-top', '— 新一代 Session —'));
      previous = entry.session_id || previous; transcriptNodes(entry).forEach(node => fragment.append(node));
    }
    if (reset) $('#timeline-list').append(fragment); else $('#timeline-list').prepend(fragment);
    state.timelineOffset += data.entries.length; state.timelineTotal = data.total; $('#timeline-count').textContent = `已显示 ${state.timelineOffset} / ${data.total} 条`;
    $('#timeline-more').classList.toggle('hidden', state.timelineOffset >= data.total);
    if (!data.total) $('#timeline-list').append(el('div', 'empty', '没有符合条件的记录。'));
    if (reset) scroll.scrollTop = scroll.scrollHeight; else scroll.scrollTop += scroll.scrollHeight - previousHeight;
  } catch (error) { if (id === state.requestId) toast(`加载失败：${error.message}`); }
}
function renderCalls() {
  const list = $('#calls-list'); clear(list); const q = $('#call-search').value.toLowerCase();
  const rows = state.calls.filter(r => `${r.name} ${r.conversation_id}`.toLowerCase().includes(q));
  for (const entry of rows) { const row = el('div', 'call-row'); row.append(el('div', 'call-conversation', `${chatTitle(entry.conversation_id)} · ${entry.time}`), toolCard(entry, true)); list.append(row); }
  if (!rows.length) list.append(el('div', 'empty', '没有匹配的工具调用。'));
  $('#calls-count').textContent = `已加载 ${state.calls.length} / ${state.callsTotal}`;
  $('#calls-more').classList.toggle('hidden', state.callsOffset >= state.callsTotal);
}
async function loadCalls(reset) {
  if (reset) { state.callsOffset = 0; state.calls = []; }
  try { const data = await api(`/api/calls?offset=${state.callsOffset}&limit=50`); state.calls.push(...data.entries); state.callsOffset += data.entries.length; state.callsTotal = data.total; renderCalls(); }
  catch (error) { toast(`加载失败：${error.message}`); }
}
function renderTools() {
  if (!state.overview) return; const list = $('#tools-list'); clear(list); const q = $('#tool-search').value.toLowerCase();
  const connected = Boolean(state.overview.pilot?.tool_control_version), warning = $('#control-warning'); warning.classList.toggle('hidden', connected);
  if (!connected) warning.textContent = '当前 Agent 未连接或尚未加载开关支持。设置将在支持该功能的 Agent 启动后生效。';
  const tools = state.overview.tools.filter(t => `${t.name} ${t.description}`.toLowerCase().includes(q));
  $('#tool-count').textContent = `${tools.filter(t => t.main || t.idle).length} 个已注册`;
  for (const tool of tools) {
    const row = el('div', 'tool-row'), info = el('div', 'tool-info'); info.append(el('div', 'tool-name', tool.name));
    const description = tool.description || '历史调用中的工具，当前未注册。'; info.append(el('div', 'tool-desc', description.length > 180 ? `${description.slice(0, 180)}…` : description)); if (description.length > 180) info.append(details('完整说明', description));
    const tags = el('div', 'tool-tags'); if (tool.main) tags.append(el('span', '', '主聊天')); if (tool.idle) tags.append(el('span', '', '空闲 Agent')); tags.append(el('span', '', `${tool.calls} 次调用`)); if (tool.last_call) tags.append(el('span', '', `最近 ${shortTime(tool.last_call)}`)); info.append(tags); row.append(info);
    if (tool.main || tool.idle) {
      const button = el('button', `switch${tool.enabled ? ' on' : ''}`); button.type = 'button'; button.setAttribute('role', 'switch'); button.setAttribute('aria-checked', String(Boolean(tool.enabled))); button.setAttribute('aria-label', `切换 ${tool.name}`); button.disabled = tool.enabled === null;
      button.addEventListener('click', async () => { button.disabled = true; try { await api(`/api/tools/${encodeURIComponent(tool.name)}`, {method: 'POST', headers: {'Content-Type': 'application/json', 'X-Dashboard': '1'}, body: JSON.stringify({enabled: !tool.enabled})}); toast(`${tool.name} 已${tool.enabled ? '关闭' : '开启'}${connected ? '' : '，等待 Agent 更新'}`); await refresh(); } catch (error) { toast(`保存失败：${error.message}`); button.disabled = false; } }); row.append(button);
    }
    list.append(row);
  }
  if (!tools.length) list.append(el('div', 'empty', '没有匹配的工具。'));
}
function renderSenders() {
  const list = $('#senders-list'); clear(list); const q = $('#sender-search').value.trim().toLowerCase();
  const rows = state.senders.filter(row => `${row.nickname} ${row.remark} ${row.sender_id} ${row.account_id}`.toLowerCase().includes(q));
  $('#sender-count').textContent = `${rows.length} / ${state.senders.length}`;
  for (const row of rows) {
    const item = el('div', 'sender-row'), info = el('div', 'sender-info');
    info.append(el('strong', '', row.remark ? `${row.remark}（${row.nickname || row.sender_id}）` : `（${row.nickname || row.sender_id}）`));
    info.append(el('div', 'sender-meta', `${row.sender_id} · ${row.account_id} · ${row.message_count} 条 · 最近 ${shortTime(row.last_seen)}`));
    const editor = el('div', 'sender-editor'), input = el('input', 'sender-input'); input.type = 'text'; input.maxLength = 120; input.value = row.remark; input.placeholder = "人工备注，例如：Master's 表弟"; input.setAttribute('aria-label', `设置 ${row.nickname || row.sender_id} 的 Remark`);
    const save = el('button', 'header-button', '保存'); save.type = 'button';
    save.addEventListener('click', async () => { save.disabled = true; try { const result = await api('/api/senders/remark', {method: 'POST', headers: {'Content-Type': 'application/json', 'X-Dashboard': '1'}, body: JSON.stringify({account_id: row.account_id, sender_id: row.sender_id, remark: input.value})}); row.remark = input.value.trim(); toast(result.status === 'cleared' ? 'Remark 已清除' : 'Remark 已保存'); renderSenders(); } catch (error) { toast(`保存失败：${error.message}`); save.disabled = false; } });
    editor.append(input, save); item.append(info, editor); list.append(item);
  }
  if (!rows.length) list.append(el('div', 'empty', state.senders.length ? '没有匹配的人物。' : '暂无可备注的联系人。'));
}
async function loadSenders() {
  try { const data = await api('/api/senders'); state.senders = data.senders || []; renderSenders(); }
  catch (error) { toast(`联系人加载失败：${error.message}`); }
}
document.querySelectorAll('.nav').forEach(button => button.addEventListener('click', () => setView(button.dataset.view)));
$('#refresh').addEventListener('click', async () => { await refresh(); if (state.view === 'chat') await loadTimeline(true); if (state.view === 'calls') await loadCalls(true); if (state.view === 'senders') await loadSenders(); toast('已刷新'); });
$('#chat-search').addEventListener('input', renderChats); $('#event-filter').addEventListener('change', () => loadTimeline(true)); $('#timeline-more').addEventListener('click', () => loadTimeline(false)); $('#call-search').addEventListener('input', renderCalls); $('#calls-more').addEventListener('click', () => loadCalls(false)); $('#tool-search').addEventListener('input', renderTools);
$('#sender-search').addEventListener('input', renderSenders);
refresh();
