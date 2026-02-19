import { useState, useEffect, useRef } from 'react';

const STORAGE_KEY = 'elementary_kapa_email';
const HUBSPOT_PORTAL_ID = '142608385';
const HUBSPOT_FORM_ID = '4734860b-68fb-4f7f-aada-afb14e61afe7';
const HUBSPOT_SUBMIT_URL = `https://api.hsforms.com/submissions/v3/integration/submit/${HUBSPOT_PORTAL_ID}/${HUBSPOT_FORM_ID}`;
const PRIMARY_COLOR = '#FF20B8';
const PRIMARY_HOVER = '#E01A9F';

function getStoredEmail() {
  if (typeof window === 'undefined') return null;
  try {
    return localStorage.getItem(STORAGE_KEY);
  } catch {
    return null;
  }
}

function storeEmail(email) {
  try {
    localStorage.setItem(STORAGE_KEY, email);
  } catch (_) {}
}

function openKapa(email) {
  if (typeof window === 'undefined') return;
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
  if (typeof window === 'undefined') return;
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
  }).catch(() => {});
}

const KapaSupport = () => {
  const [mounted, setMounted] = useState(false);
  const [popoverOpen, setPopoverOpen] = useState(false);
  const [email, setEmail] = useState('');
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const popoverRef = useRef(null);
  const buttonRef = useRef(null);

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    if (!mounted || !popoverOpen) return;
    const handleClickOutside = (e) => {
      if (
        popoverRef.current &&
        !popoverRef.current.contains(e.target) &&
        buttonRef.current &&
        !buttonRef.current.contains(e.target)
      ) {
        setPopoverOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [mounted, popoverOpen]);

  const handleButtonClick = () => {
    const stored = getStoredEmail();
    if (stored && stored.trim()) {
      openKapa(stored.trim());
      return;
    }
    setPopoverOpen((open) => !open);
    setError('');
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    const trimmed = (email || '').trim();
    if (!trimmed) {
      setError('Please enter your email.');
      return;
    }
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(trimmed)) {
      setError('Please enter a valid email address.');
      return;
    }
    setError('');
    setSubmitting(true);
    storeEmail(trimmed);
    submitToHubSpot(trimmed);
    openKapa(trimmed);
    setPopoverOpen(false);
    setEmail('');
    setSubmitting(false);
  };

  if (!mounted) return null;

  const buttonStyle = {
    position: 'fixed',
    bottom: 24,
    right: 24,
    zIndex: 2147483646,
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    padding: '12px 20px',
    backgroundColor: PRIMARY_COLOR,
    color: '#ffffff',
    border: 'none',
    borderRadius: 50,
    fontSize: 14,
    fontWeight: 600,
    cursor: 'pointer',
    boxShadow: '0 4px 14px rgba(255, 32, 184, 0.4)',
    fontFamily: 'system-ui, -apple-system, sans-serif',
  };

  const popoverStyle = {
    position: 'fixed',
    bottom: 80,
    right: 24,
    zIndex: 2147483647,
    width: '100%',
    maxWidth: 320,
    backgroundColor: '#ffffff',
    borderRadius: 12,
    padding: 20,
    boxShadow: '0 10px 40px rgba(0,0,0,0.15)',
    fontFamily: 'system-ui, -apple-system, sans-serif',
  };

  const messageStyle = { fontSize: 14, color: '#374151', margin: '0 0 16px', lineHeight: 1.5 };
  const inputStyle = {
    width: '100%',
    boxSizing: 'border-box',
    padding: '12px 14px',
    fontSize: 14,
    color: '#111827',
    borderRadius: 8,
    border: '1px solid #e5e7eb',
    marginBottom: 12,
    outline: 'none',
  };
  const submitBtnStyle = {
    width: '100%',
    padding: '12px 16px',
    fontSize: 14,
    fontWeight: 600,
    color: '#ffffff',
    backgroundColor: PRIMARY_COLOR,
    border: 'none',
    borderRadius: 8,
    cursor: submitting ? 'not-allowed' : 'pointer',
    opacity: submitting ? 0.8 : 1,
  };

  return (
    <>
      <button
        ref={buttonRef}
        type="button"
        aria-label="OSS Support"
        style={buttonStyle}
        onMouseEnter={(e) => {
          e.currentTarget.style.backgroundColor = PRIMARY_HOVER;
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.backgroundColor = PRIMARY_COLOR;
        }}
        onClick={handleButtonClick}
      >
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
        </svg>
        OSS Support
      </button>

      {popoverOpen && (
        <div ref={popoverRef} style={popoverStyle}>
          <p style={messageStyle}>
            Ask the Community Agent anything about Elementary OSS.
            <br />
            Leave your email in case a follow up is needed:
          </p>
          <form onSubmit={handleSubmit}>
            <input
              type="email"
              placeholder="you@company.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              disabled={submitting}
              style={inputStyle}
              autoFocus
            />
            <button type="submit" disabled={submitting} style={submitBtnStyle}>
              {submitting ? 'Opening…' : 'Start chat'}
            </button>
          </form>
          {error && <p style={{ fontSize: 12, color: '#dc2626', margin: '8px 0 0' }}>{error}</p>}
        </div>
      )}
    </>
  );
};

export default KapaSupport;
export { KapaSupport };
