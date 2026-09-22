/**
 * Arena2API - Background Script (Firefox)
 *
 * 管理 token 池、cookies，定期推送到本地代理服务器
 */
(function() {
  'use strict';

  var TAG = '[Arena2API]';
  var api = typeof browser !== 'undefined' ? browser : chrome;

  // ========== 状态 ==========
  var state = {
    proxyUrl: 'http://127.0.0.1:9090',
    connected: false,
    lastError: '',
    lastPush: 0,

    // tokens
    v3Tokens: [],   // [{token, action, ts}]
    v2Token: null,

    // cookies
    cookies: {},
    authToken: '',
    cfClearance: '',

    // models
    models: null,

    // tab
    tabId: null,
  };

  // ========== 从页面获取 cookies ==========
  async function requestPageCookies() {
    if (!state.tabId) return null;
    try {
      return await new Promise(function(resolve) {
        api.tabs.sendMessage(state.tabId, { type: 'NEED_COOKIES' }, function(resp) {
          if (api.runtime.lastError || !resp || !resp.cookies) {
            resolve(null);
          } else {
            resolve(resp.cookies);
          }
        });
      });
    } catch(e) {
      return null;
    }
  }

  // ========== Cookie 刷新 ==========
  async function refreshCookies() {
    try {
      var byDomain = await api.cookies.getAll({ domain: 'arena.ai' });
      var byDotDomain = await api.cookies.getAll({ domain: '.arena.ai' });
      var byUrl = await api.cookies.getAll({ url: 'https://arena.ai' });

      console.log(TAG, 'api.cookies.getAll results:');
      console.log(TAG, '  domain=arena.ai:', byDomain.length, 'cookies:', byDomain.map(function(c) { return c.name; }).join(', '));
      console.log(TAG, '  domain=.arena.ai:', byDotDomain.length, 'cookies:', byDotDomain.map(function(c) { return c.name; }).join(', '));
      console.log(TAG, '  url=https://arena.ai:', byUrl.length, 'cookies:', byUrl.map(function(c) { return c.name; }).join(', '));

      state.cookies = {};
      byDomain.concat(byDotDomain).concat(byUrl).forEach(function(c) {
        state.cookies[c.name] = c.value;
      });

      // 尝试从页面获取 document.cookie（可以读取非 HttpOnly cookies）
      var pageCookies = await requestPageCookies();
      if (pageCookies) {
        console.log(TAG, 'Page cookies:', Object.keys(pageCookies).join(', '));
        // 合并页面 cookies
        for (var k in pageCookies) {
          if (!state.cookies[k]) {
            state.cookies[k] = pageCookies[k];
          }
        }
      }

      console.log(TAG, 'All cookies:', Object.keys(state.cookies).join(', '));

      state.cfClearance = state.cookies['cf_clearance'] || '';

      // auth token 可能分片存储
      var auth = state.cookies['arena-auth-prod-v1'] || '';
      if (!auth) {
        var p0 = state.cookies['arena-auth-prod-v1.0'] || '';
        var p1 = state.cookies['arena-auth-prod-v1.1'] || '';
        console.log(TAG, 'Checking fragmented auth cookies - p0:', !!p0, 'p1:', !!p1);
        if (p0) {
          auth = p0 + (p1 || '');
          console.log(TAG, 'Combined auth token length:', auth.length);
        }
      }
      state.authToken = auth;

      if (auth) {
        console.log(TAG, 'Auth Cookie found! Length:', auth.length, 'Preview:', auth.substring(0, 50) + '...');
      } else {
        console.log(TAG, 'Auth Cookie NOT found. Available cookies:', Object.keys(state.cookies));
      }
    } catch(e) {
      console.error(TAG, 'Cookie error:', e);
    }
  }

  // ========== Token 管理 ==========
  function addToken(token, action) {
    if (!token || token.length < 20) return;
    if (state.v3Tokens.some(function(t) { return t.token === token; })) return;
    state.v3Tokens.push({ token: token, action: action || 'chat_submit', ts: Date.now() });
    while (state.v3Tokens.length > 10) state.v3Tokens.shift();
    console.log(TAG, 'Token added, pool:', state.v3Tokens.length);
  }

  // 清理过期 token
  function cleanTokens() {
    var now = Date.now();
    state.v3Tokens = state.v3Tokens.filter(function(t) { return now - t.ts < 110000; });
  }

  // ========== 向 content script 请求 token ==========
  async function requestToken() {
    if (!state.tabId) {
      try {
        var tabs = await api.tabs.query({ url: 'https://arena.ai/*' });
        if (tabs.length > 0) state.tabId = tabs[0].id;
        else return;
      } catch(e) { return; }
    }
    try {
      api.tabs.sendMessage(state.tabId, {
        type: 'NEED_TOKEN',
        action: 'chat_submit',
      }, function(resp) {
        if (api.runtime.lastError) {
          state.tabId = null;
          return;
        }
        if (resp && resp.token) {
          addToken(resp.token, resp.action);
          pushToServer();
        }
      });
    } catch(e) {
      state.tabId = null;
    }
  }

  // ========== 推送到服务器 ==========
  async function pushToServer() {
    if (!state.proxyUrl) return;
    try {
      await refreshCookies();
      cleanTokens();

      var data = {
        cookies: state.cookies,
        auth_token: state.authToken,
        cf_clearance: state.cfClearance,
        v3_tokens: state.v3Tokens.map(function(t) {
          return { token: t.token, action: t.action, age_ms: Date.now() - t.ts };
        }),
        v2_token: state.v2Token ? {
          token: state.v2Token.token,
          age_ms: Date.now() - state.v2Token.ts,
        } : null,
        models: state.models,
      };

      var url = state.proxyUrl.replace(/\/+$/, '') + '/v1/extension/push';
      var resp = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });

      if (resp.ok) {
        state.connected = true;
        state.lastError = '';
        state.lastPush = Date.now();
        var result = await resp.json();
        if (result.need_tokens) {
          requestToken();
        }
      } else {
        state.connected = false;
        state.lastError = 'HTTP ' + resp.status;
      }
    } catch(e) {
      state.connected = false;
      state.lastError = e.message || 'Connection failed';
    }
  }

  // ========== 消息处理 ==========
  api.runtime.onMessage.addListener(function(msg, sender, sendResponse) {
    switch (msg.type) {
      case 'TAB_READY':
        state.tabId = sender.tab ? sender.tab.id : null;
        console.log(TAG, 'Tab ready:', state.tabId);
        refreshCookies().then(function() { pushToServer(); });
        sendResponse({ ok: true });
        break;

      case 'PAGE_INIT':
        if (msg.models && msg.models.length > 0) {
          state.models = msg.models;
          console.log(TAG, 'Models received:', msg.models.length);
        }
        if (msg.pageCookies) {
          console.log(TAG, 'Page cookies received:', Object.keys(msg.pageCookies).join(', '));
          // 合并页面 cookies
          for (var k in msg.pageCookies) {
            if (!state.cookies[k]) {
              state.cookies[k] = msg.pageCookies[k];
            }
          }
          // 重新检查 auth token
          var auth = state.cookies['arena-auth-prod-v1'] || '';
          if (!auth) {
            var p0 = state.cookies['arena-auth-prod-v1.0'] || '';
            var p1 = state.cookies['arena-auth-prod-v1.1'] || '';
            if (p0) {
              auth = p0 + (p1 || '');
              state.authToken = auth;
              console.log(TAG, 'Auth token updated from page cookies! Length:', auth.length);
            }
          }
        }
        pushToServer();
        sendResponse({ ok: true });
        break;

      case 'NEW_TOKEN':
        addToken(msg.token, msg.action);
        pushToServer();
        sendResponse({ ok: true });
        break;

      case 'GET_STATUS':
        cleanTokens();
        refreshCookies().then(function() {
          sendResponse({
            connected: state.connected,
            proxyUrl: state.proxyUrl,
            lastError: state.lastError,
            lastPush: state.lastPush,
            v3Count: state.v3Tokens.length,
            hasV2: !!state.v2Token,
            hasAuth: !!state.authToken,
            hasCf: !!state.cfClearance,
            hasModels: !!(state.models && state.models.length),
            modelCount: state.models ? state.models.length : 0,
            tabId: state.tabId,
          });
        });
        return true;

      case 'SET_PROXY_URL':
        state.proxyUrl = msg.url;
        api.storage.local.set({ proxyUrl: msg.url });
        pushToServer();
        sendResponse({ ok: true });
        break;

      case 'FORCE_PUSH':
        pushToServer();
        sendResponse({ ok: true });
        break;

      case 'FORCE_TOKEN':
        requestToken();
        sendResponse({ ok: true });
        break;

      default:
        sendResponse({ error: 'unknown' });
    }
  });

  // ========== 定时任务 ==========
  // 每 80 秒请求新 token（token 有效期约 2 分钟）
  setInterval(function() {
    cleanTokens();
    if (state.v3Tokens.length < 5) {
      requestToken();
    }
  }, 80000);

  // 每 30 秒推送一次
  setInterval(function() {
    pushToServer();
  }, 30000);

  // ========== 初始化 ==========
  api.storage.local.get(['proxyUrl'], function(result) {
    if (result.proxyUrl) state.proxyUrl = result.proxyUrl;
    console.log(TAG, 'Proxy URL:', state.proxyUrl);
    // 启动后立即推送
    refreshCookies().then(function() { pushToServer(); });
  });

  // 监听 tab 关闭
  api.tabs.onRemoved.addListener(function(tabId) {
    if (tabId === state.tabId) state.tabId = null;
  });

  // 监听 cookie 变化
  api.cookies.onChanged.addListener(function(info) {
    if (info.cookie.domain.indexOf('arena.ai') >= 0) {
      refreshCookies();
    }
  });

  console.log(TAG, 'Background started');
})();
