// Zeitung Theme - Content Search (build-time index)
document.addEventListener('DOMContentLoaded', () => {
  const searchInput = document.getElementById('navbar-search-input');
  const resultsPanel = document.getElementById('navbar-search-results');
  if (!searchInput || !resultsPanel) return;

  const normalize = (s) => (s || '').toLowerCase();

  let contentIndex = null;
  let indexFailed = false;
  let indexPromise = null;

  const indexUrl = searchInput.dataset.searchIndex || 'search-index.json';

  const fetchIndex = () => {
    if (indexPromise) return indexPromise;
    indexPromise = fetch(indexUrl, { credentials: 'same-origin' })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data) => {
        contentIndex = Array.isArray(data) ? data : [];
        return contentIndex;
      })
      .catch((err) => {
        console.warn('Failed to load search index', err);
        contentIndex = [];
        indexFailed = true;
        return [];
      });
    return indexPromise;
  };

  const showMessage = (message) => {
    resultsPanel.innerHTML = '';
    activeIndex = 0;
    const d = document.createElement('div');
    d.className = 'navbar-search__empty';
    d.textContent = message;
    resultsPanel.appendChild(d);
    resultsPanel.classList.remove('d-none');
  };

  const escapeHtml = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

  const highlightHtml = (text, query) => {
    const safe = escapeHtml(text);
    if (!query) return safe;
    const esc = query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const re = new RegExp(esc, 'ig');
    return safe.replace(re, (m) => `<mark class="search-mark">${m}</mark>`);
  };

  let activeIndex = 0;

  const getResultItems = () => Array.from(resultsPanel.querySelectorAll('.navbar-search__item'));

  const setActiveItem = (index) => {
    const items = getResultItems();
    if (!items.length) return;
    items.forEach((el) => el.classList.remove('navbar-search__item--active'));
    activeIndex = ((index % items.length) + items.length) % items.length;
    const active = items[activeIndex];
    active.classList.add('navbar-search__item--active');
    active.scrollIntoView({ block: 'nearest' });
  };

  const navigateToActive = () => {
    const items = getResultItems();
    if (!items.length) return;
    const active = items[activeIndex];
    if (active) {
      try { sessionStorage.setItem('zeitung-search-q', searchInput.value || ''); } catch (_) {}
      window.location.href = active.href;
    }
  };

  const renderResults = (matches) => {
    resultsPanel.innerHTML = '';
    if (!matches.length) {
      const d = document.createElement('div');
      d.className = 'navbar-search__empty';
      d.textContent = 'No results';
      resultsPanel.appendChild(d);
      resultsPanel.classList.remove('d-none');
      return;
    }
    matches.slice(0, 8).forEach((m) => {
      const a = document.createElement('a');
      a.className = 'navbar-search__item';
      a.href = m.url;
      a.setAttribute('role', 'option');
      const title = document.createElement('div');
      title.className = 'navbar-search__item-title';
      title.innerHTML = highlightHtml(m.title, searchInput.value);
      const snippet = document.createElement('div');
      snippet.className = 'navbar-search__snippet';
      snippet.innerHTML = m.snippet || '';
      a.addEventListener('click', () => {
        try { sessionStorage.setItem('zeitung-search-q', searchInput.value || ''); } catch (_) {}
        resultsPanel.classList.add('d-none');
      });
      a.appendChild(title);
      if (m.snippet) a.appendChild(snippet);
      resultsPanel.appendChild(a);
    });
    resultsPanel.classList.remove('d-none');
    setActiveItem(0);
  };

  const buildSnippet = (originalText, userQuery, pos) => {
    if (pos === -1 || !originalText) return '';
    const beforeChars = 40;
    const afterChars = 90;
    let start = Math.max(0, pos - beforeChars);
    if (start > 0) {
      const boundary = originalText.slice(start).search(/\s/);
      if (boundary !== -1) start = Math.min(pos, start + boundary + 1);
    }
    let end = Math.min(originalText.length, pos + userQuery.length + afterChars);
    const boundary = originalText.slice(end).search(/\s/);
    if (boundary !== -1) end = Math.min(originalText.length, end + boundary);
    const lead = escapeHtml(originalText.slice(start, pos));
    const match = escapeHtml(originalText.slice(pos, pos + userQuery.length));
    const tail = escapeHtml(originalText.slice(pos + userQuery.length, end));
    let snippet = '';
    if (start > 0) snippet += '… ';
    snippet += `${lead}<mark class="search-mark">${match}</mark>${tail}`.trim();
    if (end < originalText.length) snippet += ' …';
    return snippet;
  };

  const searchContent = (q) => {
    const query = normalize(q);
    if (!query) { resultsPanel.classList.add('d-none'); return; }
    if (!contentIndex) { showMessage('Loading search index…'); fetchIndex().then(() => searchContent(q)); return; }
    if (indexFailed && !contentIndex.length) { showMessage('Search unavailable — index failed to load'); return; }
    const matches = contentIndex
      .map((item) => {
        const titleL = normalize(item.title);
        const textL = normalize(item.text);
        const inTitle = titleL.includes(query);
        const pos = textL.indexOf(query);
        if (!inTitle && pos === -1) return null;
        const snippet = buildSnippet(item.text, query, pos);
        const score = (titleL.startsWith(query) ? 0 : inTitle ? 1 : 2) + (pos === -1 ? 2 : 0);
        return { url: item.url, title: item.title, snippet, score, len: item.title.length };
      })
      .filter(Boolean)
      .sort((a, b) => a.score - b.score || a.len - b.len);
    renderResults(matches);
  };

  searchInput.addEventListener('focus', () => {
    fetchIndex();
    // Re-highlight first result when refocusing input
    const items = getResultItems();
    if (items.length) setActiveItem(0);
  });
  searchInput.addEventListener('input', (e) => { searchContent(e.target.value); });
  searchInput.addEventListener('keydown', (e) => {
    if (e.isComposing) return; // don't intercept IME composition (CJK input)
    if (e.key === 'Escape') {
      e.preventDefault();
      resultsPanel.classList.add('d-none');
      searchInput.blur();
      return;
    }
    const items = getResultItems();
    if (!items.length) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActiveItem(activeIndex + 1);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActiveItem(activeIndex - 1);
    } else if (e.key === 'Enter') {
      e.preventDefault();
      navigateToActive();
    }
  });
  document.addEventListener('keydown', (e) => {
    const isCmdK = (e.key === 'k' || e.key === 'K') && (e.metaKey || e.ctrlKey) && !e.altKey && !e.shiftKey;
    if (!isCmdK) return;
    e.preventDefault();
    if (!searchInput) return;
    searchInput.focus();
    searchInput.select();
    fetchIndex();
    resultsPanel?.classList.remove('d-none');
  });
  document.addEventListener('click', (e) => {
    const target = e.target;
    if (target === searchInput || resultsPanel.contains(target)) return;
    resultsPanel.classList.add('d-none');
  });

  // Prefetch index on idle so it's ready when the user searches
  if ('requestIdleCallback' in window) {
    requestIdleCallback(() => fetchIndex());
  } else {
    setTimeout(() => fetchIndex(), 1000);
  }

  // Highlight search terms on navigated-to page
  try {
    const q = sessionStorage.getItem('zeitung-search-q');
    if (q) {
      const esc = q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const re = new RegExp(esc, 'ig');
      const walker = document.createTreeWalker(document.querySelector('main') || document.body, NodeFilter.SHOW_TEXT, null);
      const nodes = [];
      while (walker.nextNode()) nodes.push(walker.currentNode);
      nodes.forEach((node) => {
        const text = node.nodeValue;
        if (!text || !re.test(text)) return;
        const span = document.createElement('span');
        span.innerHTML = text.replace(re, (m) => `<mark class="search-mark">${m}</mark>`);
        node.parentNode.replaceChild(span, node);
      });
      sessionStorage.removeItem('zeitung-search-q');
    }
  } catch (_) {}
});
