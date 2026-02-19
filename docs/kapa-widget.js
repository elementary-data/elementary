(function () {
  'use strict';

  var STORAGE_KEY = 'elementary_kapa_email';
  var HUBSPOT_PORTAL_ID = '142608385';
  var HUBSPOT_FORM_ID = '4734860b-68fb-4f7f-aada-afb14e61afe7';
  var HUBSPOT_SUBMIT_URL = 'https://api.hsforms.com/submissions/v3/integration/submit/' + HUBSPOT_PORTAL_ID + '/' + HUBSPOT_FORM_ID;
  var PRIMARY = '#FF20B8';
  var PRIMARY_HOVER = '#E01A9F';

  function getStoredEmail() {
    try {
      return localStorage.getItem(STORAGE_KEY);
    } catch (e) {
      return null;
    }
  }

  function storeEmail(email) {
    try {
      localStorage.setItem(STORAGE_KEY, email);
    } catch (e) {}
  }

  function openKapa(email) {
    window.kapaSettings = {
      user: {
        email: email,
        uniqueClientId: email,
      },
    };
    if (window.Kapa && typeof window.Kapa.open === 'function') {
      window.Kapa.open();
    } else if (typeof window.Kapa === 'function') {
      window.Kapa('open');
    }
  }

  function submitToHubSpot(email) {
    fetch(HUBSPOT_SUBMIT_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        fields: [{ name: 'email', value: email }],
        context: {
          pageUri: window.location.href,
          pageName: 'Elementary Docs - Community Agent',
        },
      }),
    }).catch(function () {});
  }

  function injectButtonAndPopover() {
    if (document.getElementById('elementary-kapa-support-root')) return;

    var root = document.createElement('div');
    root.id = 'elementary-kapa-support-root';
    root.setAttribute('style', 'position:fixed;bottom:0;right:0;z-index:2147483646;pointer-events:none;');
    root.innerHTML = '';
    document.body.appendChild(root);

    var pointerStyle = 'pointer-events:auto;';
    var button = document.createElement('button');
    button.type = 'button';
    button.setAttribute('aria-label', 'OSS Support');
    button.innerHTML =
      '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:middle;margin-right:8px;"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg> OSS Support';
    button.style.cssText =
      pointerStyle +
      'display:inline-flex;align-items:center;position:fixed;bottom:24px;right:24px;padding:12px 20px;background-color:' +
      PRIMARY +
      ';color:#fff;border:none;border-radius:50px;font-size:14px;font-weight:600;cursor:pointer;box-shadow:0 4px 14px rgba(255,32,184,0.4);font-family:system-ui,-apple-system,sans-serif;';
    button.onmouseenter = function () {
      button.style.backgroundColor = PRIMARY_HOVER;
    };
    button.onmouseleave = function () {
      button.style.backgroundColor = PRIMARY;
    };

    var popover = document.createElement('div');
    popover.id = 'elementary-kapa-popover';
    popover.setAttribute(
      'style',
      pointerStyle +
        'display:none;position:fixed;bottom:80px;right:24px;width:100%;max-width:320px;background:#fff;border-radius:12px;padding:20px;box-shadow:0 10px 40px rgba(0,0,0,0.15);font-family:system-ui,-apple-system,sans-serif;z-index:2147483647;'
    );

    var message = document.createElement('div');
    message.style.cssText = 'font-size:14px;color:#374151;margin:0 0 16px;line-height:1.5;';
    message.innerHTML =
      'Ask the Community Agent anything about Elementary OSS.<br>Leave your email in case a follow up is needed:';
    popover.appendChild(message);

    var form = document.createElement('form');
    form.style.margin = '0';

    var input = document.createElement('input');
    input.type = 'email';
    input.placeholder = 'you@company.com';
    input.style.cssText =
      'width:100%;box-sizing:border-box;padding:12px 14px;font-size:14px;color:#111;background:#fff;background-color:#fff;border-radius:8px;border:1px solid #e5e7eb;margin-bottom:12px;outline:none;';
    form.appendChild(input);

    var errEl = document.createElement('p');
    errEl.id = 'elementary-kapa-error';
    errEl.style.cssText = 'font-size:12px;color:#dc2626;margin:8px 0 0;display:none;';
    form.appendChild(errEl);

    var submitBtn = document.createElement('button');
    submitBtn.type = 'submit';
    submitBtn.textContent = 'Start chat';
    submitBtn.style.cssText =
      'width:100%;padding:12px 16px;font-size:14px;font-weight:600;color:#fff;background:' +
      PRIMARY +
      ';border:none;border-radius:8px;cursor:pointer;';
    form.appendChild(submitBtn);

    popover.appendChild(form);

    function hidePopover() {
      popover.style.display = 'none';
    }

    function showPopover() {
      popover.style.display = 'block';
      input.value = '';
      errEl.style.display = 'none';
      errEl.textContent = '';
      setTimeout(function () {
        input.focus();
      }, 50);
    }

    form.onsubmit = function (e) {
      e.preventDefault();
      var emailVal = (input.value || '').trim();
      if (!emailVal) {
        errEl.textContent = 'Please enter your email.';
        errEl.style.display = 'block';
        return;
      }
      var re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!re.test(emailVal)) {
        errEl.textContent = 'Please enter a valid email address.';
        errEl.style.display = 'block';
        return;
      }
      storeEmail(emailVal);
      submitToHubSpot(emailVal);
      openKapa(emailVal);
      hidePopover();
    };

    button.onclick = function () {
      var stored = getStoredEmail();
      if (stored && stored.trim()) {
        openKapa(stored.trim());
        return;
      }
      if (popover.style.display === 'block') {
        hidePopover();
      } else {
        showPopover();
      }
    };

    document.addEventListener('mousedown', function (e) {
      if (
        popover.style.display !== 'block' ||
        popover.contains(e.target) ||
        button.contains(e.target)
      ) {
        return;
      }
      hidePopover();
    });

    root.appendChild(popover);
    root.appendChild(button);
  }

  function loadKapaScript() {
    var script = document.createElement('script');
    script.src = 'https://widget.kapa.ai/kapa-widget.bundle.js';
    script.async = true;
    script.setAttribute('data-website-id', 'e558d15b-d976-4a89-b2f0-e33ee6dab58b');
    script.setAttribute('data-project-name', 'Elementary Community Agent');
    script.setAttribute('data-project-color', '#FF20B8');
    script.setAttribute('data-project-logo', 'https://res.cloudinary.com/do5hrgokq/image/upload/v1771424391/Elementary_2025_Pink_Mark_Black_Frame_rbexli.png');
    script.setAttribute('data-button-hide', 'true');
    document.head.appendChild(script);
  }

  function init() {
    loadKapaScript();
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', injectButtonAndPopover);
    } else {
      injectButtonAndPopover();
    }
  }

  init();
})();
