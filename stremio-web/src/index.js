// Copyright (C) 2017-2023 Smart code 203358507

if (typeof process.env.SENTRY_DSN === 'string') {
    const Sentry = require('@sentry/browser');
    Sentry.init({ dsn: process.env.SENTRY_DSN });
}

const Bowser = require('bowser');
const browser = Bowser.parse(window.navigator?.userAgent || '');
if (browser?.platform?.type === 'desktop') {
    document.querySelector('meta[name="viewport"]')?.setAttribute('content', '');
}

const React = require('react');
const ReactDOM = require('react-dom/client');
const { HashRouter } = require('react-router-dom');
const i18n = require('i18next');
const { initReactI18next } = require('react-i18next');
const stremioTranslations = require('stremio-translations');
const App = require('./App');
const { CoreProvider } = require('./core');
const { FileDropProvider, PlatformProvider } = require('./common');

const translations = Object.fromEntries(Object.entries(stremioTranslations()).map(([key, value]) => [key, {
    translation: value
}]));

i18n
    .use(initReactI18next)
    .init({
        resources: translations,
        lng: 'en-US',
        fallbackLng: 'en-US',
        interpolation: {
            escapeValue: false
        }
    });

const appInfo = {
    appVersion: process.env.VERSION,
    shellVersion: null
};

const root = ReactDOM.createRoot(document.getElementById('app'));
root.render(
    <React.StrictMode>
        <PlatformProvider>
            <CoreProvider appInfo={appInfo}>
                <FileDropProvider>
                    <HashRouter>
                        <App />
                    </HashRouter>
                </FileDropProvider>
            </CoreProvider>
        </PlatformProvider>
    </React.StrictMode>
);

// Serverless Stremio: register the in-browser streaming server. This module
// Service Worker must control the whole page (scope '/'), because a controlled
// client's subresource fetches are routed to *its* controlling SW — and the
// controlling SW is chosen by the page URL, not the request URL. Only a
// root-scoped SW ever sees the media element's /stream/* Range requests. The
// SW passes through every non-/stream/ request untouched (it only calls
// respondWith for /stream/*), so it fully replaces the default workbox SW
// without breaking normal asset loading. Registered in all environments so
// local dev works too.
if ('serviceWorker' in navigator && process.env.SERVICE_WORKER_DISABLED !== 'true' && process.env.SERVICE_WORKER_DISABLED !== true) {
    const registerStreamingServer = () => {
        navigator.serviceWorker
            .register('/local-server-sw.js', { type: 'module', scope: '/' })
            .then((reg) => {
                console.log('[v0] streaming server SW registered (scope:', reg.scope, ')');
            })
            .catch((err) => {
                console.error('[v0] streaming server SW registration failed:', err);
            });
    };
    if (document.readyState === 'complete') registerStreamingServer();
    else window.addEventListener('load', registerStreamingServer);
}
