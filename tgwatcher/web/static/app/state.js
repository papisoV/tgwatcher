// state.js — central state module (Phase 3B)
// Plain global const (non-module) so all scripts share the same `state` object.
// Phase 3B keeps legacy globals (authToken, API, etc.) in app.js; this object
// is reserved for future Phase 3C migration.
const state = {
  API: '',
  currentChat: null,
  currentPage: 1,
  pageSize: 50,
  totalMessages: 0,
  crawlRunning: false,
  expandedRow: null,
  phoneCodeHash: null,
  allDialogs: [],
  authToken: localStorage.getItem('tgwatcher_token') || '',
  sseConnected: false,
  fallBackInterval: null,
  searchTimer: null,
  _loginCheckPending: false,
  _connCheckPending: false,
  CHART_COLORS: ['#00e5ff','#00ff88','#ffb300','#a855f7','#f472b6','#60a5fa','#2dd4bf','#ff3d71'],
};
