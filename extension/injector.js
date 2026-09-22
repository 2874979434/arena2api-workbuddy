/**
 * Arena2API - Injector (MAIN world)
 * 
 * 在页面主世界中运行，可以直接访问：
 * - window.grecaptcha.enterprise
 * - window.__next_f (Next.js 数据)
 * - 页面的所有全局变量
 * 
 * 通过 window.postMessage 与 content.js 通信
 */
(function() {
  'use strict';

  var SITEKEY = '6LeTGMcsAAAAALuIlkVwIxaAuZA8VledA6d3Nnb0';
  var TAG = '[Arena2API]';

  var cachedModels = null;

  function parseModelArray(str) {
    var from = 0;
    while (str && from < str.length) {
      var keyAt = str.indexOf('initialModels', from);
      if (keyAt < 0) return null;
      var bracket = str.indexOf('[', keyAt);
      if (bracket < 0 || bracket - keyAt > 40) {
        from = keyAt + 13;
        continue;
      }
      var depth = 0, inStr = false, esc = false, found = null;
      for (var j = bracket; j < str.length; j++) {
        var c = str.charAt(j);
        if (inStr) {
          if (esc) esc = false;
          else if (c === '\\') esc = true;
          else if (c === '"') inStr = false;
          continue;
        }
        if (c === '"') inStr = true;
        else if (c === '[') depth++;
        else if (c === ']') {
          if (--depth === 0) {
            var raw = str.substring(bracket, j + 1);
            try { found = JSON.parse(raw); } catch (e) {
              try { found = JSON.parse(raw.replace(/\\"/g, '"')); } catch (e2) { found = null; }
            }
            break;
          }
        }
      }
      if (found && found.length && found[0] && (found[0].publicName || found[0].id)) return found;
      from = keyAt + 13;
    }
    return null;
  }

  function modelTexts() {
    var out = [];
    try {
      var props = window.__NEXT_DATA__ && window.__NEXT_DATA__.props;
      if (props && props.pageProps && props.pageProps.initialModels) {
        out.push(JSON.stringify({ initialModels: props.pageProps.initialModels }));
      }
    } catch (e) {}
    if (window.__next_f) {
      var joined = '';
      for (var i = 0; i < window.__next_f.length; i++) {
        var entry = window.__next_f[i];
        if (entry && typeof entry[1] === 'string') joined += entry[1];
      }
      if (joined) out.push(joined);
    }
    var scripts = document.scripts;
    for (var k = 0; k < scripts.length; k++) {
      var text = scripts[k].textContent || '';
      if (text.indexOf('initialModels') >= 0) out.push(text);
    }
    return out;
  }

  // ========== 提取模型列表 ==========
  function extractModels() {
    if (cachedModels) return cachedModels;
    try {
      var texts = modelTexts();
      for (var i = 0; i < texts.length; i++) {
        var models = parseModelArray(texts[i]) || parseModelArray(texts[i].replace(/\\"/g, '"'));
        if (models) {
          cachedModels = models;
          return models;
        }
      }
    } catch (e) {
      console.error(TAG, 'extractModels error:', e);
    }
    return null;
  }

  var postedModels = false;
  function publishModels(models) {
    if (postedModels || !models || !models.length) return;
    postedModels = true;
    cachedModels = models;
    window.postMessage({
      from: 'arena2api-injector',
      type: 'INIT',
      models: models,
      cookies: extractCookies(),
    }, '*');
    console.log(TAG, 'models:', models.length);
  }

  // ========== 提取 Next.js server action hashes ==========
  function extractNextActions() {
    // 这些 hash 用于 Next.js server actions（如 generateUploadUrl 等）
    // 暂时不需要，后续如果支持图片上传再添加
    return {};
  }

  // 页面实际加载的 enterprise.js?render= 才是有效 key，写死的 SITEKEY 只做兜底
  function pageSiteKey() {
    var urls = [];
    var nodes = document.querySelectorAll('script[src]');
    for (var i = 0; i < nodes.length; i++) urls.push(nodes[i].src);
    if (performance.getEntriesByType) {
      var entries = performance.getEntriesByType('resource');
      for (var j = 0; j < entries.length; j++) urls.push(entries[j].name);
    }
    for (var k = 0; k < urls.length; k++) {
      var m = String(urls[k]).match(/[?&]render=([^&]+)/);
      if (m && m[1] && m[1] !== 'explicit') return decodeURIComponent(m[1]);
    }
    return SITEKEY;
  }

  // ========== reCAPTCHA token 获取 ==========
  function getRecaptchaToken(action) {
    return new Promise(function(resolve, reject) {
      var g = window.grecaptcha && window.grecaptcha.enterprise
        ? window.grecaptcha.enterprise
        : window.grecaptcha;

      if (!g || typeof g.execute !== 'function') {
        reject(new Error('grecaptcha not available'));
        return;
      }

      var key = pageSiteKey();
      console.log(TAG, 'using site key', key);
      g.ready(function() {
        try {
          g.execute(key, { action: action || 'chat_submit' })
            .then(resolve)
            .catch(reject);
        } catch (err) {
          reject(err);
        }
      });
    });
  }

  // ========== 提取 cookies ==========
  function extractCookies() {
    var cookies = {};
    try {
      var cookieStr = document.cookie;
      if (cookieStr) {
        cookieStr.split(';').forEach(function(pair) {
          var parts = pair.trim().split('=');
          if (parts.length >= 2) {
            cookies[parts[0]] = parts.slice(1).join('=');
          }
        });
      }
    } catch(e) {
      console.error(TAG, 'extractCookies error:', e);
    }
    return cookies;
  }

  // ========== 消息处理 ==========
  window.addEventListener('message', function(event) {
    if (event.source !== window) return;
    if (!event.data || event.data.from !== 'arena2api-content') return;

    var msg = event.data;
    var rid = msg.rid;

    switch (msg.type) {
      case 'GET_TOKEN':
        getRecaptchaToken(msg.action).then(function(token) {
          window.postMessage({
            from: 'arena2api-injector',
            type: 'TOKEN_OK',
            rid: rid,
            token: token,
            action: msg.action || 'chat_submit',
          }, '*');
        }).catch(function(err) {
          window.postMessage({
            from: 'arena2api-injector',
            type: 'TOKEN_ERR',
            rid: rid,
            error: err.message || String(err),
          }, '*');
        });
        break;

      case 'GET_MODELS':
        var models = extractModels();
        window.postMessage({
          from: 'arena2api-injector',
          type: 'MODELS_OK',
          rid: rid,
          models: models,
        }, '*');
        break;

      case 'GET_COOKIES':
        var cookies = extractCookies();
        window.postMessage({
          from: 'arena2api-injector',
          type: 'COOKIES_OK',
          rid: rid,
          cookies: cookies,
        }, '*');
        break;

      case 'CHECK':
        var g = window.grecaptcha && window.grecaptcha.enterprise
          ? window.grecaptcha.enterprise
          : window.grecaptcha;
        window.postMessage({
          from: 'arena2api-injector',
          type: 'CHECK_OK',
          rid: rid,
          recaptcha: !!(g && typeof g.execute === 'function'),
          enterprise: !!(window.grecaptcha && window.grecaptcha.enterprise),
        }, '*');
        break;
    }
  });

  var origFetch = window.fetch;
  if (typeof origFetch === 'function') {
    window.fetch = function() {
      return origFetch.apply(this, arguments).then(function(resp) {
        var ct = '';
        try { ct = resp.headers.get('content-type') || ''; } catch (e) {}
        if (ct.indexOf('text') >= 0 || ct.indexOf('json') >= 0) {
          resp.clone().text().then(function(t) {
            if (!t || t.indexOf('initialModels') < 0) return;
            var models = parseModelArray(t) || parseModelArray(t.replace(/\\"/g, '"'));
            if (models) publishModels(models);
          }).catch(function() {});
        }
        return resp;
      });
    };
  }

  // ========== 初始化通知 ==========
  setTimeout(function() {
    var models = extractModels();
    window.postMessage({
      from: 'arena2api-injector',
      type: 'INIT',
      models: models,
      cookies: extractCookies(),
    }, '*');
    console.log(TAG, 'Injector ready, models:', models ? models.length : 0);
    if (models && models.length) {
      postedModels = true;
      return;
    }
    var tries = 0;
    var timer = setInterval(function() {
      var found = extractModels();
      if (found && found.length) {
        clearInterval(timer);
        publishModels(found);
      } else if (++tries > 15) {
        clearInterval(timer);
        console.warn(TAG, 'initialModels not found');
      }
    }, 2000);
  }, 1000);

})();
