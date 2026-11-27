const $ = (s) => document.querySelector(s);
const state = {
  brandName: 'EchoLoom',
  overview: null,
  conversation: '',
  view: 'chat',
  timelineOffset: 0,
  timelineTotal: 0,
  callsOffset: 0,
  callsTotal: 0,
  calls: [],
  senders: [],
  openSenders: new Set(),
  requestId: 0,
};
const SENDER_TOPICS = ['身份关系', '称呼', '偏好', '互动习惯', '承诺', '其他'];

function el(tag, cls, text) {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text !== undefined) n.textContent = String(text);
  return n;
}
function clear(node) { node.replaceChildren(); }
function json(value) { return JSON.stringify(value, null, 2) ?? ''; }
function shortTime(value) { return value && value !== '?' ? String(value).replace('T', ' ').slice(5, 16) : '时间未知'; }
function toast(text) {
  const n = $('#toast');
  n.textContent = text;
  n.classList.remove('hidden');
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => n.classList.add('hidden'), 3200);
}
async function api(url, options) {
  const r = await fetch(url, options);
  if (!r.ok) {
    let message = `HTTP ${r.status}`;
    try { message = (await r.json()).detail || message; } catch {}
    throw new Error(message);
  }
  return r.json();
}
async function mutate(url, method, body) {
  const headers = {'X-Dashboard': '1'};
  const options = {method, headers};
  if (body !== undefined) {
    headers['Content-Type'] = 'application/json';
    options.body = JSON.stringify(body);
  }
  return api(url, options);
}
function confirmButton(label, onConfirm, extraCls = '') {
  const btn = el('button', `subtle-btn danger-btn ${extraCls}`.trim(), label);
  btn.type = 'button';
  let armed = false;
  let timer = null;
  const reset = () => {
    armed = false;
    btn.textContent = label;
    btn.classList.remove('armed');
    clearTimeout(timer);
  };
  btn.addEventListener('click', async () => {
    if (!armed) {
      armed = true;
      btn.textContent = '确认删除？';
      btn.classList.add('armed');
      clearTimeout(timer);
      timer = setTimeout(reset, 3000);
      return;
    }
    clearTimeout(timer);
    btn.disabled = true;
    try {
      await onConfirm();
    } finally {
      if (btn.isConnected) {
        btn.disabled = false;
        reset();
      }
    }
  });
  return btn;
}
function chatTitle(id) { return id.startsWith('idle-event:') ? `空闲活动 · ${id.slice(-8)}` : id || '未知会话'; }
function chatType(id) {
  return id.startsWith('idle-event:')
    ? '空闲活动'
    : (id.endsWith('@chatroom') || /^(room|group|channel|thread|guild):/i.test(id)) ? '群聊' : '私聊';
}
function setView(view) {
  state.view = view;
  document.querySelectorAll('.view').forEach(n => n.classList.add('hidden'));
  $(`#view-${view}`).classList.remove('hidden');
  document.querySelectorAll('.nav').forEach(n => n.classList.toggle('active', n.dataset.view === view));
  if (view === 'calls' && !state.calls.length) loadCalls(true);
  if (view === 'senders' && !state.senders.length) loadSenders();
  if (view === 'memory' && window.EchoLoomMemory) window.EchoLoomMemory.onActivate();
}
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
function details(label, value) {
  const d = el('details', 'raw-detail');
  d.append(el('summary', '', label));
  d.append(el('pre', '', typeof value === 'string' ? value : json(value)));
  return d;
}
function bodyText(content) {
  const text = el('div', 'message-text', content);
  if (content.length < 650 && content.split('\n').length < 15) return text;
  text.classList.add('collapsed'); const wrapper = el('div'); wrapper.append(text);
  const toggle = el('button', 'raw-action', '展开全文 ↓');
  toggle.addEventListener('click', () => {
    text.classList.toggle('collapsed');
    toggle.textContent = text.classList.contains('collapsed') ? '展开全文 ↓' : '收起 ↑';
  });
  wrapper.append(toggle);
  return wrapper;
}
function message(content, who, time, extra, sender) {
  const row = el('article', `message ${who}`);
  if (who === 'assistant') row.append(el('div', 'message-avatar', '✳'));
  const body = el('div', 'message-body'), label = el('div', 'message-label');
  label.append(el('span', '', who === 'assistant' ? state.brandName : (sender || '本轮输入')), el('span', 'event-time', time));
  body.append(label, bodyText(content));
  if (extra) body.append(extra);
  row.append(body);
  return row;
}
function resultLabel(entry) {
  if (entry.result === null || entry.result === undefined) return '等待结果 / 未记录';
  const r = entry.result; const status = r?.reply?.status || r?.status;
  if (status === 'queued') return '已排队 · 未确认送达';
  if (status === 'accepted_by_server') return '服务器已接受 · 未确认送达';
  if (status) return String(status);
  if (r?.ok === true) return '完成';
  if (r?.ok === false) return '失败';
  return '已返回';
}
function toolCard(entry, global = false) {
  const d = el('details', 'tool-card'); if (global) d.classList.add('global-tool');
  const sum = el('summary');
  sum.append(el('span', 'tool-icon', '⌘'), el('span', 'tool-name', entry.name || '未知工具'), el('span', 'tool-result', resultLabel(entry)), el('span', 'event-time', shortTime(entry.time)));
  d.append(sum);
  const content = el('div', 'tool-content');
  if (entry.args !== undefined) content.append(el('div', 'tool-section', '调用参数'), el('pre', '', json(entry.args)));
  if (entry.result !== null && entry.result !== undefined) content.append(el('div', 'tool-section', '工具结果'), el('pre', '', json(entry.result)));
  else content.append(el('div', 'meta-note', '没有记录到对应结果。'));
  if (entry.error) content.append(el('div', 'meta-note', `运行状态：${entry.error}`));
  d.append(content);
  return d;
}
function transcriptNodes(entry) {
  const nodes = [];
  if (entry.kind === 'input') {
    if (entry.messages?.length) {
      entry.messages.forEach((m, i) => {
        const raw = i === entry.messages.length - 1 ? details('查看 trace 中的完整输入', entry.raw) : null;
        const sender = m.sender.includes('|') ? m.sender.split('|').slice(1).join('|').trim() : m.sender;
        nodes.push(message(m.text, 'user', shortTime(entry.time), raw, sender));
      });
    } else {
      const preview = entry.raw.length > 500 ? (entry.conversation_id.startsWith('idle-event:') ? '空闲维护上下文' : '模型输入上下文') : entry.raw;
      nodes.push(message(preview, 'user', shortTime(entry.time), preview === entry.raw ? null : details('查看 trace 中的完整输入', entry.raw)));
    }
  } else if (entry.kind === 'assistant') {
    nodes.push(message(entry.text, 'assistant', shortTime(entry.time)));
  } else if (entry.kind === 'tool' || entry.kind === 'tool_result') {
    const reply = entry.name === 'commit_agent_turn' && entry.args?.reply_text;
    if (reply) {
      const note = el('div', 'meta-note', `发送工具回执：${resultLabel(entry)}`);
      nodes.push(message(reply, 'assistant', shortTime(entry.time), note));
    }
    nodes.push(toolCard(entry));
  } else if (entry.kind === 'request') {
    const d = el('details', 'request-card'), s = el('summary');
    s.append(el('span', 'tool-icon', '◷'), el('span', '', `模型请求 · ${entry.model || '未记录模型'}`), el('span', 'tool-result', shortTime(entry.time)));
    d.append(s);
    const content = el('div', 'tool-content');
    content.append(el('div', 'tool-section', 'trace 中的请求快照（含历史与动态上下文；写入时可能已清洗或截断）'), el('pre', '', json(entry.contents)));
    d.append(content);
    nodes.push(d);
  } else if (entry.kind === 'error') {
    nodes.push(el('div', 'error-card', `运行状态：${entry.error}`));
  }
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
    renderChats();
    renderTools();
    if (!state.conversation) {
      const first = state.overview.conversations.find(c => c.id && !c.id.startsWith('idle-event:')) || state.overview.conversations.find(c => c.id);
      if (first) await selectChat(first.id);
    }
  } catch (error) {
    toast(`加载失败：${error.message}`);
  }
}
async function selectChat(id) {
  state.conversation = id; state.timelineOffset = 0; state.requestId++; setView('chat'); renderChats();
  $('#selected-chat').textContent = chatTitle(id);
  const row = state.overview.conversations.find(x => x.id === id);
  $('#selected-meta').textContent = row ? `${chatType(id)} · ${row.sessions} 代 Session · ${row.tool_calls} 次工具调用 · ${row.first} 至 ${row.last}` : id;
  $('#export-jsonl').href = `/api/records.jsonl?${new URLSearchParams({conversation: id})}`;
  $('#export-jsonl').classList.remove('hidden');
  await loadTimeline(true);
}
async function loadTimeline(reset) {
  if (!state.conversation) return;
  const id = ++state.requestId;
  const scroll = $('#chat-scroll');
  const previousHeight = scroll.scrollHeight;
  if (reset) { state.timelineOffset = 0; clear($('#timeline-list')); }
  const params = new URLSearchParams({conversation: state.conversation, kind: $('#event-filter').value, offset: state.timelineOffset, limit: 50});
  try {
    const data = await api(`/api/transcript?${params}`);
    if (id !== state.requestId) return;
    const fragment = document.createDocumentFragment();
    let previous = '';
    for (const entry of data.entries) {
      if (entry.session_id && previous && entry.session_id !== previous) fragment.append(el('div', 'history-top', '— 新一代 Session —'));
      previous = entry.session_id || previous;
      transcriptNodes(entry).forEach(node => fragment.append(node));
    }
    if (reset) $('#timeline-list').append(fragment); else $('#timeline-list').prepend(fragment);
    state.timelineOffset += data.entries.length;
    state.timelineTotal = data.total;
    $('#timeline-count').textContent = `已显示 ${state.timelineOffset} / ${data.total} 条`;
    $('#timeline-more').classList.toggle('hidden', state.timelineOffset >= data.total);
    if (!data.total) $('#timeline-list').append(el('div', 'empty', '没有符合条件的记录。'));
    if (reset) scroll.scrollTop = scroll.scrollHeight;
    else scroll.scrollTop += scroll.scrollHeight - previousHeight;
  } catch (error) {
    if (id === state.requestId) toast(`加载失败：${error.message}`);
  }
}
function renderCalls() {
  const list = $('#calls-list'); clear(list); const q = $('#call-search').value.toLowerCase();
  const rows = state.calls.filter(r => `${r.name} ${r.conversation_id}`.toLowerCase().includes(q));
  for (const entry of rows) {
    const row = el('div', 'call-row');
    row.append(el('div', 'call-conversation', `${chatTitle(entry.conversation_id)} · ${entry.time}`), toolCard(entry, true));
    list.append(row);
  }
  if (!rows.length) list.append(el('div', 'empty', '没有匹配的工具调用。'));
  $('#calls-count').textContent = `已加载 ${state.calls.length} / ${state.callsTotal}`;
  $('#calls-more').classList.toggle('hidden', state.callsOffset >= state.callsTotal);
}
async function loadCalls(reset) {
  if (reset) { state.callsOffset = 0; state.calls = []; }
  try {
    const data = await api(`/api/calls?offset=${state.callsOffset}&limit=50`);
    state.calls.push(...data.entries);
    state.callsOffset += data.entries.length;
    state.callsTotal = data.total;
    renderCalls();
  } catch (error) {
    toast(`加载失败：${error.message}`);
  }
}
function renderTools() {
  if (!state.overview) return;
  const list = $('#tools-list'); clear(list); const q = $('#tool-search').value.toLowerCase();
  const connected = Boolean(state.overview.pilot?.tool_control_version), warning = $('#control-warning');
  warning.classList.toggle('hidden', connected);
  if (!connected) warning.textContent = '当前 Agent 未连接或尚未加载开关支持。设置将在支持该功能的 Agent 启动后生效。';
  const tools = state.overview.tools.filter(t => `${t.name} ${t.description}`.toLowerCase().includes(q));
  $('#tool-count').textContent = `${tools.filter(t => t.main || t.idle).length} 个已注册`;
  for (const tool of tools) {
    const row = el('div', 'tool-row'), info = el('div', 'tool-info');
    info.append(el('div', 'tool-name', tool.name));
    const description = tool.description || '历史调用中的工具，当前未注册。';
    info.append(el('div', 'tool-desc', description.length > 180 ? `${description.slice(0, 180)}…` : description));
    if (description.length > 180) info.append(details('完整说明', description));
    const tags = el('div', 'tool-tags');
    if (tool.main) tags.append(el('span', '', '主聊天'));
    if (tool.idle) tags.append(el('span', '', '空闲 Agent'));
    tags.append(el('span', '', `${tool.calls} 次调用`));
    if (tool.last_call) tags.append(el('span', '', `最近 ${shortTime(tool.last_call)}`));
    info.append(tags);
    row.append(info);
    if (tool.main || tool.idle) {
      const button = el('button', `switch${tool.enabled ? ' on' : ''}`);
      button.type = 'button';
      button.setAttribute('role', 'switch');
      button.setAttribute('aria-checked', String(Boolean(tool.enabled)));
      button.setAttribute('aria-label', `切换 ${tool.name}`);
      button.disabled = tool.enabled === null;
      button.addEventListener('click', async () => {
        button.disabled = true;
        try {
          await mutate(`/api/tools/${encodeURIComponent(tool.name)}`, 'POST', {enabled: !tool.enabled});
          toast(`${tool.name} 已${tool.enabled ? '关闭' : '开启'}${connected ? '' : '，等待 Agent 更新'}`);
          await refresh();
        } catch (error) {
          toast(`保存失败：${error.message}`);
          button.disabled = false;
        }
      });
      row.append(button);
    }
    list.append(row);
  }
  if (!tools.length) list.append(el('div', 'empty', '没有匹配的工具。'));
}
function renderSenders() {
  const list = $('#senders-list'); clear(list); const q = $('#sender-search').value.trim().toLowerCase();
  const rows = state.senders.filter(row => {
    const tagText = (row.tags || []).join(' ');
    const memText = (row.memories || []).map(m => `${m.topic} ${m.content}`).join(' ');
    return `${row.nickname} ${row.remark} ${row.sender_id} ${row.account_id} ${tagText} ${memText}`.toLowerCase().includes(q);
  });
  $('#sender-count').textContent = `${rows.length} / ${state.senders.length}`;
  for (const row of rows) {
    const pairKey = `${row.account_id}\0${row.sender_id}`;
    const card = el('div', 'sender-card');
    const top = el('div', 'sender-row');
    const info = el('div', 'sender-info');
    const heading = el('div', 'sender-heading');
    heading.append(el('strong', '', row.remark ? `${row.remark}（${row.nickname || row.sender_id}）` : `（${row.nickname || row.sender_id}）`));
    const badges = el('div', 'chip-row');
    for (const tag of row.tags || []) badges.append(el('span', 'chip chip-ok', tag));
    if (row.bloated) badges.append(el('span', 'chip chip-warn', `碎片 ${row.memory_count} 条`));
    else if (row.memory_count) badges.append(el('span', 'chip chip-muted', `${row.memory_count} 条资料`));
    if (badges.childElementCount) heading.append(badges);
    info.append(heading);
    info.append(el('div', 'sender-meta', `${row.sender_id} · ${row.account_id} · ${row.message_count} 条消息 · 最近 ${shortTime(row.last_seen)}`));

    const editor = el('div', 'sender-editor');
    const input = el('input', 'sender-input');
    input.type = 'text'; input.maxLength = 120; input.value = row.remark;
    input.placeholder = '人工备注，例如：表弟 / 同事';
    input.setAttribute('aria-label', `设置 ${row.nickname || row.sender_id} 的 Remark`);
    const save = el('button', 'header-button', '保存备注'); save.type = 'button';
    save.addEventListener('click', async () => {
      save.disabled = true;
      try {
        const result = await mutate('/api/senders/remark', 'POST', {account_id: row.account_id, sender_id: row.sender_id, remark: input.value});
        row.remark = input.value.trim();
        toast(result.status === 'cleared' ? 'Remark 已清除' : 'Remark 已保存');
        renderSenders();
      } catch (error) {
        toast(`保存失败：${error.message}`);
        save.disabled = false;
      }
    });
    editor.append(input, save);
    top.append(info, editor);
    card.append(top);

    const drawer = el('details', 'sender-drawer');
    if (state.openSenders.has(pairKey)) drawer.open = true;
    drawer.addEventListener('toggle', () => {
      if (drawer.open) state.openSenders.add(pairKey);
      else state.openSenders.delete(pairKey);
    });
    const tagCount = (row.tags || []).length;
    const memCount = (row.memories || []).length;
    drawer.append(el('summary', '', `展开标签与人物资料 · ${tagCount} 个标签 · ${memCount} 条资料`));

    const body = el('div', 'sender-drawer-body');
    const tagSection = el('div', 'drawer-section');
    tagSection.append(el('div', 'drawer-label', '联系人标签（每人最多 5 个）'));
    const tagWrap = el('div', 'chip-row');
    if ((row.tags || []).length) {
      for (const tag of row.tags) {
        const chip = el('span', 'chip chip-ok chip-removable');
        chip.append(el('span', '', tag));
        const rm = el('button', 'chip-x', '×');
        rm.type = 'button';
        rm.setAttribute('aria-label', `移除标签 ${tag}`);
        rm.addEventListener('click', async () => {
          rm.disabled = true;
          try {
            const res = await mutate('/api/senders/tags', 'POST', {account_id: row.account_id, sender_id: row.sender_id, action: 'remove', tags: [tag]});
            row.tags = res.tags || [];
            toast(`已移除标签「${tag}」`);
            renderSenders();
          } catch (error) {
            toast(`操作失败：${error.message}`);
            rm.disabled = false;
          }
        });
        chip.append(rm);
        tagWrap.append(chip);
      }
    } else {
      tagWrap.append(el('span', 'meta-note', '暂无标签'));
    }
    tagSection.append(tagWrap);
    const tagForm = el('div', 'mini-form');
    const tagInput = el('input', 'sender-input');
    tagInput.type = 'text'; tagInput.maxLength = 20; tagInput.placeholder = '新增标签（例如：唯一主人、同事）';
    const tagBtn = el('button', 'header-button', '添加标签'); tagBtn.type = 'button';
    tagBtn.addEventListener('click', async () => {
      const value = tagInput.value.trim();
      if (!value) return;
      tagBtn.disabled = true;
      try {
        const res = await mutate('/api/senders/tags', 'POST', {account_id: row.account_id, sender_id: row.sender_id, action: 'add', tags: [value]});
        row.tags = res.tags || [];
        toast(`已添加标签「${value}」`);
        renderSenders();
      } catch (error) {
        toast(`添加失败：${error.message}`);
        tagBtn.disabled = false;
      }
    });
    tagForm.append(tagInput, tagBtn);
    tagSection.append(tagForm);
    body.append(tagSection);

    const memSection = el('div', 'drawer-section');
    memSection.append(el('div', 'drawer-label', '人物资料（sender_memories）'));
    if ((row.memories || []).length) {
      const memList = el('div', 'sub-list');
      for (const mem of row.memories) {
        const mRow = el('div', 'sub-item');
        const mMain = el('div', 'sub-item-main');
        const mHead = el('div', 'chip-row');
        mHead.append(el('span', 'chip chip-muted', mem.topic), el('span', 'event-time', shortTime(mem.updated_at)));
        mMain.append(mHead, el('div', 'sub-item-text', mem.content));
        const delBtn = confirmButton('删除', async () => {
          await mutate(`/api/senders/memories/${encodeURIComponent(mem.memory_id)}?${new URLSearchParams({account_id: row.account_id})}`, 'DELETE');
          row.memories = row.memories.filter(item => item.memory_id !== mem.memory_id);
          row.memory_count = row.memories.length;
          row.bloated = row.memory_count >= 8;
          toast('人物资料已删除');
          renderSenders();
        });
        mRow.append(mMain, delBtn);
        memList.append(mRow);
      }
      memSection.append(memList);
    } else {
      memSection.append(el('div', 'meta-note', '暂无沉淀的人物资料。'));
    }

    const memForm = el('div', 'mini-form');
    const topicSelect = el('select', '');
    for (const topic of SENDER_TOPICS) topicSelect.append(el('option', '', topic));
    const memInput = el('input', 'sender-input');
    memInput.type = 'text'; memInput.maxLength = 500; memInput.placeholder = '新增人物资料（≤500字）';
    const memBtn = el('button', 'header-button', '添加资料'); memBtn.type = 'button';
    memBtn.addEventListener('click', async () => {
      const content = memInput.value.trim();
      if (!content) return;
      memBtn.disabled = true;
      try {
        const res = await mutate('/api/senders/memories', 'POST', {
          account_id: row.account_id,
          sender_id: row.sender_id,
          topic: topicSelect.value,
          content,
        });
        toast(res.status === 'unchanged' ? '已有完全相同的资料' : '人物资料已保存');
        await loadSenders();
      } catch (error) {
        toast(`保存失败：${error.message}`);
        memBtn.disabled = false;
      }
    });
    memForm.append(topicSelect, memInput, memBtn);
    memSection.append(memForm);
    body.append(memSection);

    drawer.append(body);
    card.append(drawer);
    list.append(card);
  }
  if (!rows.length) list.append(el('div', 'empty', state.senders.length ? '没有匹配的人物。' : '暂无可管理的联系人。'));
}
async function loadSenders() {
  try {
    const data = await api('/api/senders');
    state.senders = data.senders || [];
    renderSenders();
  } catch (error) {
    toast(`联系人加载失败：${error.message}`);
  }
}

window.EchoLoomUI = {$, el, clear, api, mutate, toast, shortTime, chatTitle, details, confirmButton, state};

document.querySelectorAll('.nav').forEach(button => button.addEventListener('click', () => setView(button.dataset.view)));
$('#refresh').addEventListener('click', async () => {
  await refresh();
  if (state.view === 'chat') await loadTimeline(true);
  if (state.view === 'calls') await loadCalls(true);
  if (state.view === 'senders') await loadSenders();
  if (state.view === 'memory' && window.EchoLoomMemory) await window.EchoLoomMemory.refreshAll();
  toast('已刷新');
});
$('#chat-search').addEventListener('input', renderChats);
$('#event-filter').addEventListener('change', () => loadTimeline(true));
$('#timeline-more').addEventListener('click', () => loadTimeline(false));
$('#call-search').addEventListener('input', renderCalls);
$('#calls-more').addEventListener('click', () => loadCalls(false));
$('#tool-search').addEventListener('input', renderTools);
$('#sender-search').addEventListener('input', renderSenders);
refresh();
