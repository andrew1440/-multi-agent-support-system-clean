const config = window.NOVAMART_CONFIG || {};
const elements = {
  authGate: document.querySelector('#auth-gate'),
  authHint: document.querySelector('#auth-hint'),
  chatPanel: document.querySelector('#chat-panel'),
  connectionLabel: document.querySelector('#connection-label'),
  customerAvatar: document.querySelector('#customer-avatar'),
  customerIdLabel: document.querySelector('#customer-id-label'),
  customerLabel: document.querySelector('#customer-label'),
  customerSummary: document.querySelector('#customer-summary'),
  form: document.querySelector('#chat-form'),
  input: document.querySelector('#message-input'),
  messages: document.querySelector('#messages'),
  notice: document.querySelector('#notice'),
  sendButton: document.querySelector('#send-button'),
  sessionLabel: document.querySelector('#session-label'),
  signIn: document.querySelector('#sign-in'),
  signOut: document.querySelector('#sign-out'),
  topbarStatus: document.querySelector('.topbar-status'),
};

const authKeys = {
  state: 'novamart.auth.state',
  verifier: 'novamart.auth.verifier',
  idToken: 'novamart.auth.idToken',
  sessionId: 'novamart.sessionId',
};

function isConfigured() {
  return Boolean(config.region && config.userPoolId && config.appClientId && config.hostedUiDomain && config.runtimeArn);
}

function setNotice(message, visible = true) {
  elements.notice.textContent = message;
  elements.notice.hidden = !visible;
}

function setStatus(message, ready = false) {
  elements.connectionLabel.textContent = message;
  elements.topbarStatus.dataset.state = ready ? 'ready' : 'pending';
}

function randomString(size = 64) {
  const bytes = crypto.getRandomValues(new Uint8Array(size));
  return Array.from(bytes, value => value.toString(16).padStart(2, '0')).join('');
}

function base64UrlEncode(buffer) {
  let binary = '';
  for (const byte of new Uint8Array(buffer)) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

async function createCodeChallenge(verifier) {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier));
  return base64UrlEncode(digest);
}

function issuerDomain() {
  return `https://${config.hostedUiDomain.replace(/^https?:\/\//, '').replace(/\/$/, '')}`;
}

function decodeIdToken(token) {
  const payload = token.split('.')[1];
  if (!payload) throw new Error('Cognito returned an invalid ID token.');
  const base64 = payload.replace(/-/g, '+').replace(/_/g, '/');
  const decoded = decodeURIComponent(Array.from(atob(base64), char => `%${char.charCodeAt(0).toString(16).padStart(2, '0')}`).join(''));
  return JSON.parse(decoded);
}

function getSessionId() {
  let sessionId = sessionStorage.getItem(authKeys.sessionId);
  if (!sessionId) {
    sessionId = crypto.randomUUID();
    sessionStorage.setItem(authKeys.sessionId, sessionId);
  }
  return sessionId;
}

async function beginSignIn() {
  const state = randomString(24);
  const verifier = randomString(48);
  const challenge = await createCodeChallenge(verifier);
  sessionStorage.setItem(authKeys.state, state);
  sessionStorage.setItem(authKeys.verifier, verifier);

  const authorizeUrl = new URL(`${issuerDomain()}/oauth2/authorize`);
  authorizeUrl.search = new URLSearchParams({
    client_id: config.appClientId,
    response_type: 'code',
    scope: 'openid email profile',
    redirect_uri: config.redirectUri,
    state,
    code_challenge_method: 'S256',
    code_challenge: challenge,
  });
  window.location.assign(authorizeUrl.toString());
}

async function completeSignIn() {
  const query = new URLSearchParams(window.location.search);
  const code = query.get('code');
  if (!code) return false;

  const expectedState = sessionStorage.getItem(authKeys.state);
  const actualState = query.get('state');
  const verifier = sessionStorage.getItem(authKeys.verifier);
  sessionStorage.removeItem(authKeys.state);
  sessionStorage.removeItem(authKeys.verifier);
  window.history.replaceState({}, document.title, window.location.pathname);

  if (!expectedState || actualState !== expectedState || !verifier) {
    throw new Error('Sign-in state validation failed. Please sign in again.');
  }

  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    client_id: config.appClientId,
    redirect_uri: config.redirectUri,
    code,
    code_verifier: verifier,
  });
  const response = await fetch(`${issuerDomain()}/oauth2/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
  });
  const tokenResponse = await response.json();
  if (!response.ok || !tokenResponse.id_token) {
    throw new Error(tokenResponse.error_description || tokenResponse.error || 'Cognito token exchange failed.');
  }

  sessionStorage.setItem(authKeys.idToken, tokenResponse.id_token);
  return true;
}

function showChat(token) {
  const claims = decodeIdToken(token);
  const customerId = claims['custom:customer_id'] || claims.customer_id;
  const email = claims.email || 'Signed-in customer';

  elements.authGate.hidden = true;
  elements.chatPanel.hidden = false;
  elements.customerSummary.hidden = false;
  elements.signOut.hidden = false;
  elements.customerLabel.textContent = email;
  elements.customerIdLabel.textContent = customerId || 'Customer profile not linked';
  elements.customerAvatar.textContent = (email[0] || 'N').toUpperCase();
  elements.sessionLabel.textContent = `Session ${getSessionId().slice(0, 8)}`;

  if (!customerId) {
    elements.input.disabled = true;
    elements.sendButton.disabled = true;
    setNotice('This Cognito account is not linked to a NovaMart customer. Ask an administrator to set its custom:customer_id attribute.');
    setStatus('Customer profile not linked');
    return;
  }

  elements.input.disabled = false;
  elements.sendButton.disabled = false;
  elements.input.focus();
  setStatus('Secure session active', true);
}

function addMessage(role, text, timeLabel = 'Just now') {
  const article = document.createElement('article');
  article.className = `message ${role === 'user' ? 'user-message' : 'assistant-message'}`;
  const avatar = document.createElement('div');
  avatar.className = 'message-avatar';
  avatar.textContent = role === 'user' ? elements.customerAvatar.textContent : 'N';
  const body = document.createElement('div');
  body.className = 'message-body';
  const meta = document.createElement('div');
  meta.className = 'message-meta';
  const name = document.createElement('strong');
  name.textContent = role === 'user' ? 'You' : 'NovaMart Support';
  const time = document.createElement('span');
  time.textContent = timeLabel;
  const paragraph = document.createElement('p');
  paragraph.textContent = text;
  meta.append(name, time);
  body.append(meta, paragraph);
  article.append(avatar, body);
  elements.messages.append(article);
  elements.messages.scrollTop = elements.messages.scrollHeight;
  return paragraph;
}

function runtimeEndpoint() {
  const encodedArn = encodeURIComponent(config.runtimeArn);
  return `https://bedrock-agentcore.${config.region}.amazonaws.com/runtimes/${encodedArn}/invocations?qualifier=DEFAULT`;
}

async function sendMessage(event) {
  event.preventDefault();
  const prompt = elements.input.value.trim();
  if (!prompt || elements.sendButton.disabled) return;

  const token = sessionStorage.getItem(authKeys.idToken);
  if (!token) {
    setNotice('Your sign-in session ended. Sign in again to continue.');
    return;
  }
  const claims = decodeIdToken(token);
  const customerId = claims['custom:customer_id'] || claims.customer_id;
  if (!customerId) {
    setNotice('This account has no linked NovaMart customer ID.');
    return;
  }

  setNotice('', false);
  addMessage('user', prompt);
  elements.input.value = '';
  elements.sendButton.disabled = true;
  elements.input.disabled = true;
  const pending = addMessage('assistant', 'Checking that for you…');
  pending.closest('.message').classList.add('typing');

  try {
    const response = await fetch(runtimeEndpoint(), {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      body: JSON.stringify({
        prompt,
        session_id: getSessionId(),
        customer_id: customerId,
      }),
    });
    const responseText = await response.text();
    if (!response.ok) throw new Error(`Runtime returned ${response.status}: ${responseText || response.statusText}`);
    let answer = responseText;
    try {
      const result = JSON.parse(responseText);
      answer = result.response || result.message || responseText;
    } catch (_) {
      // AgentCore may return a plain-text body for non-JSON protocol responses.
    }
    pending.closest('.message').remove();
    addMessage('assistant', answer);
  } catch (error) {
    pending.closest('.message').remove();
    addMessage('assistant', 'I could not reach the support service. Your message was not processed.');
    setNotice(error.message || 'The support request failed.');
  } finally {
    elements.input.disabled = false;
    elements.sendButton.disabled = false;
    elements.input.focus();
  }
}

function signOut() {
  sessionStorage.removeItem(authKeys.idToken);
  sessionStorage.removeItem(authKeys.sessionId);
  const logoutUrl = new URL(`${issuerDomain()}/logout`);
  logoutUrl.search = new URLSearchParams({
    client_id: config.appClientId,
    logout_uri: config.logoutUri,
  });
  window.location.assign(logoutUrl.toString());
}

async function start() {
  elements.signIn.addEventListener('click', beginSignIn);
  elements.signOut.addEventListener('click', signOut);
  elements.form.addEventListener('submit', sendMessage);

  if (!isConfigured()) {
    elements.authHint.textContent = 'Set the Cognito and AgentCore values in config.js after applying infra/.';
    setNotice('Authentication is not configured yet. Apply infra/ and copy its Cognito outputs into web/config.js.');
    elements.signIn.disabled = true;
    setStatus('Setup required');
    return;
  }

  try {
    await completeSignIn();
    const token = sessionStorage.getItem(authKeys.idToken);
    if (token) {
      showChat(token);
      return;
    }
  } catch (error) {
    setNotice(error.message || 'Sign-in failed.');
  }
  elements.signIn.disabled = false;
  setStatus('Authentication required');
}

start();
