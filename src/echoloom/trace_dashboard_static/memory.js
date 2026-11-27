(() => {
  const ui = window.EchoLoomUI;
  if (!ui) return;
  const {$, el, clear, api, mutate, toast, shortTime, displayName, chatTitle, mergeNames, confirmButton} = ui;

  const memState = {
    tab: 'facts',
    accountId: '',
    accounts: [],
    summary: null,
    facts: [],
    factIndexLimit: 40,
    events: [],
    quotas: null,
    entities: [],
    annotations: [],
    episodesData: null,
    recentMessages: [],
    episodeConversation: '',
    createOpen: false,
    editingId: null,
    summaryRequestId: 0,
    requestId: 0,
    loaded: false,
  };

  function toIsoWithOffset(localValue) {
    if (!localValue || !localValue.trim()) return null;
    const d = new Date(localValue);
    if (Number.isNaN(d.getTime())) return localValue.trim();
    return d.toISOString();
  }

  function toDatetimeLocalValue(isoValue) {
    if (!isoValue) return '';
    const d = new Date(isoValue);
    if (Number.isNaN(d.getTime())) return '';
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  function provenanceBox(label, sources) {
    const valid = (sources || []).filter(Boolean);
    if (!valid.length) return null;
    const d = el('details', 'provenance');
    d.append(el('summary', '', label));
    const list = el('div', 'provenance-list');
    for (const src of valid) {
      const box = el('div', 'provenance-item');
      const meta = el('div', 'provenance-meta');
      const chatSpan = el('span', '', chatTitle(src.conversation_id || ''));
      if (src.conversation_id) chatSpan.title = src.conversation_id;
      const senderSpan = el('span', '', displayName(src.sender_id || '') || '未知发送人');
      if (src.sender_id) senderSpan.title = src.sender_id;
      meta.append(
        chatSpan,
        document.createTextNode(' · '),
        senderSpan,
        document.createTextNode(` · ${shortTime(src.captured_at)} · ${src.message_id}`),
      );
      box.append(
        meta,
        el('div', 'provenance-body', src.body_text || '（无正文）'),
      );
      list.append(box);
    }
    d.append(list);
    return d;
  }

  function renderAccountSelect() {
    const sel = $('#memory-account');
    clear(sel);
    if (memState.accounts.length <= 1) {
      sel.classList.add('hidden');
      return;
    }
    sel.classList.remove('hidden');
    for (const acct of memState.accounts) {
      const opt = el('option', '', displayName(acct) || acct);
      opt.value = acct;
      opt.title = acct;
      if (acct === memState.accountId) opt.selected = true;
      sel.append(opt);
    }
  }

  function renderStats() {
    const bar = $('#memory-stats');
    clear(bar);
    const c = memState.summary?.counts;
    if (!c) return;
    const items = [
      {
        tab: 'facts',
        label: '长期事实',
        value: c.facts_total,
        sub: `Top ${c.fact_index_limit} 索引覆盖 ${c.facts_in_index}`,
        warn: c.facts_total > c.fact_index_limit,
      },
      {
        tab: 'events',
        label: '事件簿',
        value: c.events_high + c.events_ordinary,
        sub: c.events_stale_72h
          ? `高优 ${c.events_high}/${c.events_high_limit} · ${c.events_stale_72h} 条超 72h`
          : `高优 ${c.events_high}/${c.events_high_limit} · 普通 ${c.events_ordinary}`,
        warn: c.events_stale_72h > 0 || c.events_high >= c.events_high_limit,
      },
      {
        tab: 'entities',
        label: '词典词条',
        value: c.entities_total,
        sub: '词边界正则匹配',
        warn: false,
      },
      {
        tab: 'annotations',
        label: '消息批注',
        value: c.annotations_total,
        sub: '按消息 ID 挂载',
        warn: false,
      },
      {
        tab: 'episodes',
        label: '事件记忆',
        value: c.episodes_available ? c.episodes_total : '—',
        sub: !c.episodes_available
          ? '未检测到向量库'
          : c.episodes_bloated_chats
            ? `${c.episodes_bloated_chats} 个会话待合并 (≥12)`
            : '会话向量事件摘要',
        warn: c.episodes_bloated_chats > 0,
      },
    ];
    for (const item of items) {
      const cell = el('button', `stat-cell${memState.tab === item.tab ? ' active' : ''}`);
      cell.type = 'button';
      cell.append(
        el('div', 'stat-label', item.label),
        el('div', 'stat-value', item.value),
        el('div', `stat-sub${item.warn ? ' warn' : ''}`, item.sub),
      );
      cell.addEventListener('click', () => switchTab(item.tab));
      bar.append(cell);
    }
  }

  function updateAddButton() {
    const btn = $('#memory-add-toggle');
    const canCreate = Boolean(memState.accountId) && ['facts', 'events', 'entities'].includes(memState.tab);
    btn.classList.toggle('hidden', !canCreate);
    btn.textContent = memState.createOpen ? '收起表单' : '+ 新增';
  }

  function renderSubbar() {
    const bar = $('#memory-subbar');
    clear(bar);
    if (memState.tab === 'events' && memState.quotas) {
      const q = memState.quotas;
      bar.append(
        el('span', `chip ${q.high_used >= q.high_limit ? 'chip-warn' : 'chip-muted'}`, `高优槽位 ${q.high_used}/${q.high_limit}`),
        el('span', 'chip chip-muted', `普通字数 ${q.ordinary_chars_used} / ${q.ordinary_chars_limit}`),
      );
    } else if (memState.tab === 'facts') {
      const total = memState.facts.length;
      const inIndex = memState.facts.filter(f => f.in_index).length;
      bar.append(el('span', 'chip chip-muted', `显示 ${total} 条 · 索引内 ${inIndex} 条`));
    } else if (memState.tab === 'episodes' && memState.episodesData?.available) {
      const convs = memState.episodesData.conversations || [];
      if (convs.length) {
        const sel = el('select', '');
        sel.setAttribute('aria-label', '按会话筛选事件记忆');
        const allOpt = el('option', '', `全部会话（${convs.length}）`);
        allOpt.value = '';
        sel.append(allOpt);
        for (const c of convs) {
          const opt = el('option', '', `${chatTitle(c.conversation_id)}（${c.count} 条${c.bloated ? ' · 待合并' : ''}）`);
          opt.value = c.conversation_id;
          opt.title = c.conversation_id;
          if (c.conversation_id === memState.episodeConversation) opt.selected = true;
          sel.append(opt);
        }
        sel.addEventListener('change', async () => {
          memState.episodeConversation = sel.value;
          await loadActiveTab();
        });
        bar.append(sel);
      }
    }
  }

  function renderCreatePanel() {
    const panel = $('#memory-create-panel');
    clear(panel);
    if (!memState.createOpen || !memState.accountId) {
      panel.classList.add('hidden');
      return;
    }
    panel.classList.remove('hidden');

    if (memState.tab === 'facts') {
      panel.append(el('div', 'panel-title', '新增账号级长期事实（须绑定一条来源消息以保留溯源）'));
      const form = el('div', 'stacked-form');
      const row1 = el('div', 'form-row');
      const keyInput = el('input', 'sender-input');
      keyInput.placeholder = 'fact_key（如 user_allergy、cat_name，≤100字）';
      keyInput.maxLength = 100;
      const msgSelect = el('select', 'source-select');
      const defaultOpt = el('option', '', '选择近期消息作为来源（或右侧手填 ID）');
      defaultOpt.value = '';
      msgSelect.append(defaultOpt);
      for (const m of memState.recentMessages) {
        const senderLabel = displayName(m.sender_id) || '未知发送人';
        const preview = `${shortTime(m.captured_at)} · ${senderLabel}：${m.body_text.slice(0, 32)}`;
        const opt = el('option', '', preview);
        opt.value = m.message_id;
        opt.title = `${m.conversation_id || ''} / ${m.sender_id || ''} · ${m.message_id}`;
        msgSelect.append(opt);
      }
      const msgIdInput = el('input', 'sender-input');
      msgIdInput.placeholder = 'source_message_id';
      msgSelect.addEventListener('change', () => {
        if (msgSelect.value) msgIdInput.value = msgSelect.value;
      });
      row1.append(keyInput, msgIdInput);
      const contentInput = el('textarea', 'form-textarea');
      contentInput.rows = 2;
      contentInput.maxLength = 2000;
      contentInput.placeholder = '事实内容（简短准确，≤2000字）';
      const actions = el('div', 'form-actions');
      const submit = el('button', 'header-button primary-btn', '保存事实');
      submit.type = 'button';
      submit.addEventListener('click', async () => {
        if (!keyInput.value.trim() || !contentInput.value.trim() || !msgIdInput.value.trim()) {
          toast('请填写 fact_key、内容与来源消息 ID');
          return;
        }
        submit.disabled = true;
        try {
          await mutate('/api/memory/facts', 'POST', {
            account_id: memState.accountId,
            fact_key: keyInput.value.trim(),
            content: contentInput.value.trim(),
            source_message_id: msgIdInput.value.trim(),
          });
          memState.createOpen = false;
          toast('长期事实已保存');
          await refreshAll();
        } catch (error) {
          toast(`保存失败：${error.message}`);
          submit.disabled = false;
        }
      });
      actions.append(submit);
      form.append(row1, msgSelect, contentInput, actions);
      panel.append(form);
    } else if (memState.tab === 'events') {
      panel.append(el('div', 'panel-title', '新增跨会话事件簿条目'));
      const form = el('div', 'stacked-form');
      const row1 = el('div', 'form-row');
      const prio = el('select', '');
      const optOrd = el('option', '', '普通事件（ordinary，≤1000字）'); optOrd.value = 'ordinary';
      const optHigh = el('option', '', '高优事件（high，全账限4条，≤200字）'); optHigh.value = 'high';
      prio.append(optOrd, optHigh);
      const expiryInput = el('input', 'sender-input');
      expiryInput.type = 'datetime-local';
      expiryInput.setAttribute('aria-label', '到期时间（可选）');
      row1.append(prio, expiryInput);
      const contentInput = el('textarea', 'form-textarea');
      contentInput.rows = 2;
      contentInput.placeholder = '事件内容（跨会话需记住的事实、承诺或安排）';
      const exitInput = el('input', 'sender-input');
      exitInput.maxLength = 500;
      exitInput.placeholder = '退出条件（必填，例如：周五晚上赴约结束后删除）';
      const actions = el('div', 'form-actions');
      const submit = el('button', 'header-button primary-btn', '保存事件');
      submit.type = 'button';
      submit.addEventListener('click', async () => {
        if (!contentInput.value.trim() || !exitInput.value.trim()) {
          toast('请填写事件内容和退出条件');
          return;
        }
        submit.disabled = true;
        try {
          await mutate('/api/memory/events', 'POST', {
            account_id: memState.accountId,
            priority: prio.value,
            content: contentInput.value.trim(),
            exit_condition: exitInput.value.trim(),
            expires_at: toIsoWithOffset(expiryInput.value),
          });
          memState.createOpen = false;
          toast('事件已保存');
          await refreshAll();
        } catch (error) {
          toast(`保存失败：${error.message}`);
          submit.disabled = false;
        }
      });
      actions.append(submit);
      form.append(row1, contentInput, exitInput, actions);
      panel.append(form);
    } else if (memState.tab === 'entities') {
      panel.append(el('div', 'panel-title', '新增或更新词典词条（黑话 / 别名 / 项目术语）'));
      const form = el('div', 'stacked-form');
      const termInput = el('input', 'sender-input');
      termInput.maxLength = 80;
      termInput.placeholder = '词条 term（≤80字）';
      const expInput = el('textarea', 'form-textarea');
      expInput.rows = 2;
      expInput.maxLength = 500;
      expInput.placeholder = '简短释义 explanation（≤500字）';
      const actions = el('div', 'form-actions');
      const submit = el('button', 'header-button primary-btn', '保存词条');
      submit.type = 'button';
      submit.addEventListener('click', async () => {
        if (!termInput.value.trim() || !expInput.value.trim()) {
          toast('请填写词条和释义');
          return;
        }
        submit.disabled = true;
        try {
          await mutate('/api/memory/entities', 'POST', {
            account_id: memState.accountId,
            term: termInput.value.trim(),
            explanation: expInput.value.trim(),
          });
          memState.createOpen = false;
          toast('词典词条已保存');
          await refreshAll();
        } catch (error) {
          toast(`保存失败：${error.message}`);
          submit.disabled = false;
        }
      });
      actions.append(submit);
      form.append(termInput, expInput, actions);
      panel.append(form);
    }
  }

  function renderFactsList(container) {
    if (!memState.facts.length) {
      container.append(el('div', 'empty', '暂无匹配的长期事实。'));
      return;
    }
    for (const fact of memState.facts) {
      const row = el('article', 'memory-row');
      const top = el('div', 'memory-row-head');
      const titleGroup = el('div', 'chip-row');
      titleGroup.append(el('span', 'fact-key', fact.fact_key));
      if (fact.in_index) titleGroup.append(el('span', 'chip chip-ok', `索引内 #${fact.index_rank}`));
      else titleGroup.append(el('span', 'chip chip-muted', '已沉底'));
      titleGroup.append(el('span', 'chip chip-muted', `v${fact.version}`));
      const authorTime = el('span', 'event-time', `${displayName(fact.author_id) || fact.author_id} · ${shortTime(fact.updated_at)}`);
      if (fact.author_id) authorTime.title = fact.author_id;
      titleGroup.append(authorTime);

      const actions = el('div', 'memory-actions');
      const editBtn = el('button', 'subtle-btn', memState.editingId === fact.fact_id ? '取消' : '编辑');
      editBtn.type = 'button';
      editBtn.addEventListener('click', () => {
        memState.editingId = memState.editingId === fact.fact_id ? null : fact.fact_id;
        renderList();
      });
      const delBtn = confirmButton('删除', async () => {
        await mutate(`/api/memory/facts/${encodeURIComponent(fact.fact_id)}?${new URLSearchParams({account_id: fact.account_id})}`, 'DELETE');
        toast(`已删除事实 ${fact.fact_key}`);
        await refreshAll();
      });
      actions.append(editBtn, delBtn);
      top.append(titleGroup, actions);
      row.append(top);

      if (memState.editingId === fact.fact_id) {
        const form = el('div', 'inline-edit');
        const textarea = el('textarea', 'form-textarea');
        textarea.rows = 3;
        textarea.maxLength = 2000;
        textarea.value = fact.content;
        const saveRow = el('div', 'form-actions');
        const save = el('button', 'header-button primary-btn', '保存新版本');
        save.type = 'button';
        save.addEventListener('click', async () => {
          const content = textarea.value.trim();
          if (!content) {
            toast('事实内容不能为空');
            return;
          }
          save.disabled = true;
          try {
            await mutate('/api/memory/facts', 'POST', {
              account_id: fact.account_id,
              fact_key: fact.fact_key,
              content,
              source_message_id: fact.source_message_id,
            });
            memState.editingId = null;
            toast(`已更新事实 ${fact.fact_key}`);
            await refreshAll();
          } catch (error) {
            toast(`更新失败：${error.message}`);
            save.disabled = false;
          }
        });
        saveRow.append(save);
        form.append(textarea, saveRow);
        row.append(form);
      } else {
        row.append(el('div', 'memory-body', fact.content));
      }

      const prov = provenanceBox('查看来源消息原句', [fact.source]);
      if (prov) row.append(prov);
      container.append(row);
    }
  }

  function renderEventsList(container) {
    if (!memState.events.length) {
      container.append(el('div', 'empty', '当前事件簿为空。'));
      return;
    }
    for (const evt of memState.events) {
      const row = el('article', 'memory-row');
      const top = el('div', 'memory-row-head');
      const badges = el('div', 'chip-row');
      badges.append(el('span', `chip ${evt.priority === 'high' ? 'chip-ok' : 'chip-muted'}`, evt.priority === 'high' ? '高优' : '普通'));
      if (evt.stale_72h) badges.append(el('span', 'chip chip-warn', '超 72h 待复核'));
      if (evt.expires_at) badges.append(el('span', 'chip chip-muted', `到期 ${shortTime(evt.expires_at)}`));
      badges.append(el('span', 'event-time', `${evt.age_hours}h 前更新 · ${evt.event_id}`));

      const actions = el('div', 'memory-actions');
      const editBtn = el('button', 'subtle-btn', memState.editingId === evt.event_id ? '取消' : '编辑');
      editBtn.type = 'button';
      editBtn.addEventListener('click', () => {
        memState.editingId = memState.editingId === evt.event_id ? null : evt.event_id;
        renderList();
      });
      const delBtn = confirmButton('删除', async () => {
        await mutate(`/api/memory/events/${encodeURIComponent(evt.event_id)}?${new URLSearchParams({account_id: memState.accountId})}`, 'DELETE');
        toast('事件已删除');
        await refreshAll();
      });
      actions.append(editBtn, delBtn);
      top.append(badges, actions);
      row.append(top);

      if (memState.editingId === evt.event_id) {
        const form = el('div', 'inline-edit');
        const contentInput = el('textarea', 'form-textarea');
        contentInput.rows = 2;
        contentInput.value = evt.content;
        const exitInput = el('input', 'sender-input');
        exitInput.value = evt.exit_condition;
        exitInput.placeholder = '退出条件';
        const expRow = el('div', 'form-row');
        const expInput = el('input', 'sender-input');
        expInput.type = 'datetime-local';
        expInput.value = toDatetimeLocalValue(evt.expires_at);
        const clearLabel = el('label', 'check-label');
        const clearBox = el('input');
        clearBox.type = 'checkbox';
        clearLabel.append(clearBox, el('span', '', '清除到期时间'));
        expRow.append(expInput, clearLabel);
        const saveRow = el('div', 'form-actions');
        const save = el('button', 'header-button primary-btn', '保存修改');
        save.type = 'button';
        save.addEventListener('click', async () => {
          const content = contentInput.value.trim();
          const exitCond = exitInput.value.trim();
          if (!content || !exitCond) {
            toast('请填写事件内容和退出条件');
            return;
          }
          save.disabled = true;
          try {
            const payload = {
              account_id: memState.accountId,
              content,
              exit_condition: exitCond,
            };
            if (clearBox.checked) payload.expires_at = 'clear';
            else if (expInput.value) payload.expires_at = toIsoWithOffset(expInput.value);
            await mutate(`/api/memory/events/${encodeURIComponent(evt.event_id)}`, 'PATCH', payload);
            memState.editingId = null;
            toast('事件已更新');
            await refreshAll();
          } catch (error) {
            toast(`更新失败：${error.message}`);
            save.disabled = false;
          }
        });
        saveRow.append(save);
        form.append(contentInput, exitInput, expRow, saveRow);
        row.append(form);
      } else {
        row.append(
          el('div', 'memory-body', evt.content),
          el('div', 'memory-subline', `退出条件：${evt.exit_condition}`),
        );
      }
      container.append(row);
    }
  }

  function renderEntitiesList(container) {
    if (!memState.entities.length) {
      container.append(el('div', 'empty', '暂无匹配的词典词条。'));
      return;
    }
    for (const item of memState.entities) {
      const row = el('article', 'memory-row');
      const top = el('div', 'memory-row-head');
      const badges = el('div', 'chip-row');
      const entityAuthorTime = el('span', 'event-time', `${displayName(item.author_id) || item.author_id} · ${shortTime(item.updated_at)}`);
      if (item.author_id) entityAuthorTime.title = item.author_id;
      badges.append(el('strong', 'entity-term', item.term), el('span', 'chip chip-muted', `v${item.version}`), entityAuthorTime);
      const actions = el('div', 'memory-actions');
      const editBtn = el('button', 'subtle-btn', memState.editingId === item.entity_id ? '取消' : '编辑');
      editBtn.type = 'button';
      editBtn.addEventListener('click', () => {
        memState.editingId = memState.editingId === item.entity_id ? null : item.entity_id;
        renderList();
      });
      const delBtn = confirmButton('删除', async () => {
        await mutate(`/api/memory/entities/${encodeURIComponent(item.entity_id)}?${new URLSearchParams({account_id: item.account_id})}`, 'DELETE');
        toast(`已删除词条「${item.term}」`);
        await refreshAll();
      });
      actions.append(editBtn, delBtn);
      top.append(badges, actions);
      row.append(top);

      if (memState.editingId === item.entity_id) {
        const form = el('div', 'inline-edit');
        const expInput = el('textarea', 'form-textarea');
        expInput.rows = 2;
        expInput.maxLength = 500;
        expInput.value = item.explanation;
        const saveRow = el('div', 'form-actions');
        const save = el('button', 'header-button primary-btn', '保存释义');
        save.type = 'button';
        save.addEventListener('click', async () => {
          const explanation = expInput.value.trim();
          if (!explanation) {
            toast('词条释义不能为空');
            return;
          }
          save.disabled = true;
          try {
            await mutate('/api/memory/entities', 'POST', {
              account_id: item.account_id,
              term: item.term,
              explanation,
            });
            memState.editingId = null;
            toast(`已更新词条「${item.term}」`);
            await refreshAll();
          } catch (error) {
            toast(`更新失败：${error.message}`);
            save.disabled = false;
          }
        });
        saveRow.append(save);
        form.append(expInput, saveRow);
        row.append(form);
      } else {
        row.append(el('div', 'memory-body', item.explanation));
      }
      container.append(row);
    }
  }

  function renderAnnotationsList(container) {
    if (!memState.annotations.length) {
      container.append(el('div', 'empty', '暂无匹配的消息批注。'));
      return;
    }
    for (const note of memState.annotations) {
      const row = el('article', 'memory-row');
      const top = el('div', 'memory-row-head');
      const badges = el('div', 'chip-row');
      const noteAuthorTime = el('span', 'event-time', `${displayName(note.author_id) || note.author_id} · ${shortTime(note.updated_at)}`);
      if (note.author_id) noteAuthorTime.title = note.author_id;
      badges.append(
        el('span', 'chip chip-muted', note.source),
        el('span', 'chip chip-muted', `v${note.version}`),
        noteAuthorTime,
      );
      const actions = el('div', 'memory-actions');
      if (note.editable) {
        const editBtn = el('button', 'subtle-btn', memState.editingId === note.note_id ? '取消' : '编辑');
        editBtn.type = 'button';
        editBtn.addEventListener('click', () => {
          memState.editingId = memState.editingId === note.note_id ? null : note.note_id;
          renderList();
        });
        const delBtn = confirmButton('删除', async () => {
          await mutate(`/api/memory/annotations/${encodeURIComponent(note.note_id)}?${new URLSearchParams({account_id: note.account_id})}`, 'DELETE');
          toast('批注已删除');
          await refreshAll();
        });
        actions.append(editBtn, delBtn);
      }
      top.append(badges, actions);
      row.append(top);

      if (memState.editingId === note.note_id) {
        const form = el('div', 'inline-edit');
        const textarea = el('textarea', 'form-textarea');
        textarea.rows = 2;
        textarea.maxLength = 4000;
        textarea.value = note.content;
        const saveRow = el('div', 'form-actions');
        const save = el('button', 'header-button primary-btn', '保存批注');
        save.type = 'button';
        save.addEventListener('click', async () => {
          const content = textarea.value.trim();
          if (!content) {
            toast('批注内容不能为空');
            return;
          }
          save.disabled = true;
          try {
            await mutate(`/api/memory/annotations/${encodeURIComponent(note.note_id)}`, 'PATCH', {
              account_id: note.account_id,
              content,
            });
            memState.editingId = null;
            toast('批注已更新');
            await refreshAll();
          } catch (error) {
            toast(`更新失败：${error.message}`);
            save.disabled = false;
          }
        });
        saveRow.append(save);
        form.append(textarea, saveRow);
        row.append(form);
      } else {
        row.append(el('div', 'memory-body', note.content));
      }
      const prov = provenanceBox('查看原消息', [note.source_message]);
      if (prov) row.append(prov);
      container.append(row);
    }
  }

  function renderEpisodesList(container) {
    const data = memState.episodesData;
    if (!data || !data.available) {
      const blank = el('div', 'blank-state');
      blank.append(
        el('div', 'blank-icon', '◧'),
        el('strong', '', '尚未启用或未生成事件记忆库'),
        el('span', '', '开启 ECHOLOOM_EPISODE_RAG=1 并在会话中提取事件后，data/episode-memory.sqlite3 会自动展示在这里。'),
      );
      container.append(blank);
      return;
    }
    const items = data.episodes || [];
    if (!items.length) {
      container.append(el('div', 'empty', '暂无匹配的事件记忆。'));
      return;
    }
    for (const ep of items) {
      const row = el('article', 'memory-row');
      const top = el('div', 'memory-row-head');
      const badges = el('div', 'chip-row');
      const convBadge = el('span', 'chip chip-muted', chatTitle(ep.conversation_id));
      if (ep.conversation_id) convBadge.title = ep.conversation_id;
      badges.append(convBadge);
      if (ep.conversation_bloated) {
        badges.append(el('span', 'chip chip-warn', `该会话 ${ep.conversation_count} 条 · 待合并`));
      }
      badges.append(el('span', 'event-time', `${ep.model} · ${shortTime(ep.updated_at)}`));
      const actions = el('div', 'memory-actions');
      actions.append(confirmButton('删除', async () => {
        await mutate(`/api/memory/episodes/${encodeURIComponent(ep.id)}?${new URLSearchParams({account_id: ep.account_id})}`, 'DELETE');
        toast('事件记忆已删除');
        await refreshAll();
      }));
      top.append(badges, actions);
      row.append(top, el('div', 'memory-body', ep.summary));
      const prov = provenanceBox(`来源消息（${(ep.source_message_ids || []).length} 条）`, ep.sources);
      if (prov) row.append(prov);
      container.append(row);
    }
  }

  function renderList() {
    renderSubbar();
    updateAddButton();
    renderCreatePanel();
    const list = $('#memory-list');
    clear(list);
    if (!memState.accountId && memState.tab !== 'episodes') {
      list.append(el('div', 'empty', '当前消息档案库暂无记录，收到聊天消息后会自动出现。'));
      return;
    }
    if (memState.tab === 'facts') renderFactsList(list);
    else if (memState.tab === 'events') renderEventsList(list);
    else if (memState.tab === 'entities') renderEntitiesList(list);
    else if (memState.tab === 'annotations') renderAnnotationsList(list);
    else if (memState.tab === 'episodes') renderEpisodesList(list);
  }

  async function loadActiveTab() {
    const reqId = ++memState.requestId;
    const targetTab = memState.tab;
    const targetAccount = memState.accountId;
    const targetConv = memState.episodeConversation;
    const q = $('#memory-search').value.trim();
    const params = new URLSearchParams();
    if (targetAccount) params.set('account_id', targetAccount);
    if (q) params.set('q', q);

    try {
      if (targetTab === 'facts') {
        const data = await api(`/api/memory/facts?${params}`);
        if (reqId !== memState.requestId) return;
        memState.facts = data.facts || [];
        memState.factIndexLimit = data.index_limit || 40;
        if (memState.createOpen && !memState.recentMessages.length && targetAccount) {
          const rec = await api(`/api/memory/recent-messages?${new URLSearchParams({account_id: targetAccount, limit: '30'})}`);
          if (reqId !== memState.requestId) return;
          memState.recentMessages = rec.messages || [];
        }
      } else if (targetTab === 'events') {
        const data = await api(`/api/memory/events?${params}`);
        if (reqId !== memState.requestId) return;
        memState.events = data.events || [];
        memState.quotas = data.quotas || null;
      } else if (targetTab === 'entities') {
        const data = await api(`/api/memory/entities?${params}`);
        if (reqId !== memState.requestId) return;
        memState.entities = data.entities || [];
      } else if (targetTab === 'annotations') {
        const data = await api(`/api/memory/annotations?${params}`);
        if (reqId !== memState.requestId) return;
        memState.annotations = data.annotations || [];
      } else if (targetTab === 'episodes') {
        if (targetConv) params.set('conversation_id', targetConv);
        const data = await api(`/api/memory/episodes?${params}`);
        if (reqId !== memState.requestId) return;
        memState.episodesData = data;
      }
      renderList();
    } catch (error) {
      if (reqId === memState.requestId) toast(`记忆加载失败：${error.message}`);
    }
  }

  async function refreshAll() {
    const sumId = ++memState.summaryRequestId;
    memState.recentMessages = [];
    try {
      const params = new URLSearchParams();
      if (memState.accountId) params.set('account_id', memState.accountId);
      const summary = await api(`/api/memory/summary?${params}`);
      if (sumId !== memState.summaryRequestId) return;
      memState.summary = summary;
      memState.accounts = summary.accounts || [];
      memState.accountId = summary.account_id || '';
      memState.loaded = true;
      if (summary.names) mergeNames(summary.names);
      renderAccountSelect();
      renderStats();
      await loadActiveTab();
    } catch (error) {
      if (sumId === memState.summaryRequestId) toast(`记忆概览加载失败：${error.message}`);
    }
  }

  async function switchTab(tab) {
    memState.tab = tab;
    memState.createOpen = false;
    memState.editingId = null;
    document.querySelectorAll('#memory-tabs .seg-btn').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.tab === tab);
      btn.setAttribute('aria-selected', String(btn.dataset.tab === tab));
    });
    renderStats();
    await loadActiveTab();
  }

  document.querySelectorAll('#memory-tabs .seg-btn').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });

  $('#memory-account').addEventListener('change', async () => {
    memState.accountId = $('#memory-account').value;
    memState.episodeConversation = '';
    memState.recentMessages = [];
    memState.editingId = null;
    await refreshAll();
  });

  let searchTimer = null;
  $('#memory-search').addEventListener('input', () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => loadActiveTab(), 160);
  });

  $('#memory-add-toggle').addEventListener('click', async () => {
    memState.createOpen = !memState.createOpen;
    if (memState.createOpen && memState.tab === 'facts' && !memState.recentMessages.length && memState.accountId) {
      try {
        const rec = await api(`/api/memory/recent-messages?${new URLSearchParams({account_id: memState.accountId, limit: '30'})}`);
        memState.recentMessages = rec.messages || [];
      } catch {}
    }
    updateAddButton();
    renderCreatePanel();
  });

  window.EchoLoomMemory = {
    onActivate: async () => {
      if (!memState.loaded || (!memState.editingId && !memState.createOpen)) {
        await refreshAll();
        return;
      }
      const sumId = ++memState.summaryRequestId;
      try {
        const params = new URLSearchParams();
        if (memState.accountId) params.set('account_id', memState.accountId);
        const summary = await api(`/api/memory/summary?${params}`);
        if (sumId !== memState.summaryRequestId) return;
        memState.summary = summary;
        memState.accounts = summary.accounts || [];
        memState.accountId = summary.account_id || '';
        if (summary.names) mergeNames(summary.names);
        renderAccountSelect();
        renderStats();
      } catch {}
    },
    onNamesUpdated: () => {
      if (!memState.loaded) return;
      renderAccountSelect();
      renderList();
    },
    refreshAll,
  };
})();
