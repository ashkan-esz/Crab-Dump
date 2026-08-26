(() => {
  const METHODS_WITH_CSRF = new Set(['POST', 'PUT', 'DELETE', 'PATCH']);
  let loginPromise = null;
  let sessionEstablished = false;

  function csrfToken() {
    return document.cookie
      .split('; ')
      .find(value => value.startsWith('crab_csrf='))
      ?.split('=')
      .slice(1)
      .join('=') || '';
  }

  function closeDialog(dialog, result) {
    if (dialog.open) dialog.close();
    dialog.remove();
    return result;
  }

  function requestCredentials() {
    return new Promise(resolve => {
      const dialog = document.createElement('dialog');
      dialog.className = 'dashboard-auth-dialog';
      dialog.setAttribute('aria-labelledby', 'dashboard-auth-title');
      dialog.setAttribute('aria-describedby', 'dashboard-auth-description');
      dialog.innerHTML = `
        <form method="dialog" class="dashboard-auth-form">
          <h2 id="dashboard-auth-title">Dashboard sign in</h2>
          <p id="dashboard-auth-description" class="dashboard-auth-description">Sign in to continue.</p>
          <div class="dashboard-auth-error" role="alert" aria-live="polite" hidden></div>
          <label for="dashboard-auth-username">Username</label>
          <input id="dashboard-auth-username" name="username" autocomplete="username" required>
          <label for="dashboard-auth-password">Password</label>
          <input id="dashboard-auth-password" name="password" type="password" autocomplete="current-password" required>
          <menu class="dashboard-auth-actions">
            <button type="button" data-dashboard-auth-cancel>Cancel</button>
            <button type="submit" class="primary" data-dashboard-auth-submit>Sign in</button>
          </menu>
        </form>`;

      const form = dialog.querySelector('form');
      const username = form.elements.username;
      const password = form.elements.password;
      const error = dialog.querySelector('.dashboard-auth-error');
      const submit = dialog.querySelector('[data-dashboard-auth-submit]');
      let settled = false;

      const finish = value => {
        if (settled) return;
        settled = true;
        resolve(closeDialog(dialog, value));
      };

      dialog.querySelector('[data-dashboard-auth-cancel]').addEventListener('click', () => finish(null));
      dialog.addEventListener('cancel', event => {
        event.preventDefault();
        finish(null);
      });
      dialog.addEventListener('close', () => {
        if (!settled) finish(null);
      });
      form.addEventListener('submit', async event => {
        event.preventDefault();
        if (submit.disabled) return;
        submit.disabled = true;
        submit.textContent = 'Signing in…';
        error.hidden = true;
        try {
          const response = await fetch('/api/auth/login', {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username: username.value, password: password.value }),
          });
          if (!response.ok) {
            const payload = await response.json().catch(() => ({}));
            throw new Error(payload.error || 'Sign in failed. Check your credentials and try again.');
          }
          finish(true);
        } catch (cause) {
          error.textContent = cause instanceof Error ? cause.message : 'Sign in failed. Try again.';
          error.hidden = false;
          submit.disabled = false;
          submit.textContent = 'Sign in';
          password.focus();
        }
      });

      document.body.append(dialog);
      dialog.showModal();
      username.focus();
    });
  }

  async function ensureSession() {
    if (sessionEstablished) return true;
    if (loginPromise) return loginPromise;
    loginPromise = (async () => {
      const sessionCheck = await fetch('/api/config', {
        credentials: 'same-origin',
        cache: 'no-store',
      });
      if (sessionCheck.ok) {
        sessionEstablished = true;
        return true;
      }
      // Do not prompt for credentials when the server is unavailable or the
      // session is forbidden for another reason. A login prompt can only fix
      // an absent/expired session (401).
      if (sessionCheck.status !== 401) return false;
      const authenticated = await requestCredentials();
      sessionEstablished = authenticated === true;
      return sessionEstablished;
    })().catch(() => {
      sessionEstablished = false;
      return false;
    }).finally(() => {
      loginPromise = null;
    });
    return loginPromise;
  }

  async function request(url, options = {}) {
    const method = String(options.method || 'GET').toUpperCase();
    const headers = { ...(options.headers || {}) };
    if (options.body && !Object.keys(headers).some(key => key.toLowerCase() === 'content-type')) {
      headers['Content-Type'] = 'application/json';
    }
    if (METHODS_WITH_CSRF.has(method)) headers['X-CSRF-Token'] = csrfToken();

    let response = await fetch(url, { credentials: 'same-origin', ...options, headers });
    if (response.status === 401) {
      sessionEstablished = false;
      if (await ensureSession()) {
        if (METHODS_WITH_CSRF.has(method)) headers['X-CSRF-Token'] = csrfToken();
        response = await fetch(url, { credentials: 'same-origin', ...options, headers });
      }
    }
    return response;
  }

  async function signOut() {
    try {
      await fetch('/api/auth/logout', { method: 'POST', credentials: 'same-origin' });
    } finally {
      sessionEstablished = false;
      window.location.reload();
    }
  }

  window.dashboardAuth = { csrfToken, ensureSession, request, requestCredentials, signOut };
  window.signOut = signOut;
})();
