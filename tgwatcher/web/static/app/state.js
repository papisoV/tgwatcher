// state.js — central state module (Phase 3A)
// Re-exported from app.js global declarations. Phase 3B will move consumers here.
export const state = {
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
