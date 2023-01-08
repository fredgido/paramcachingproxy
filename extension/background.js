'use strict';

if (!globalThis.hasOwnProperty('browser')) {
    globalThis.browser = globalThis.chrome;
}

// settings
const settingsDefaults = {
    proxyUrl: "http://localhost:7020",
    apiUrl: "http://localhost:7021",
    originalProxyEnabled: false,
};

browser.settingsDefaults = settingsDefaults;

let settings;

browser.storage.local.get([
        "proxyUrl",
        "apiUrl",
        "originalProxyEnabled",
    ], (storageReturn) => {
        settings = storageReturn;
    }
)


let active = true;

// images
function onBeforeReq(req) {
    if (settings?.proxyUrl) {
        if (settings?.originalProxyEnabled) {
            return {redirectUrl: settings.proxyUrl + "/twitter_proxy?url=" + encodeURIComponent(req.url)};
        } else {
            fetch(settings.proxyUrl + '/twitter_downloader?url=' + encodeURIComponent(req.url), {mode: 'no-cors'})
        }
    }
    return req
}

// api
function onMessage(request, sender, sendResponse) {
    if (settings?.apiUrl) {
        fetch(settings.apiUrl + '/notify?url=' + encodeURIComponent(request.detail.url), {
            method: 'post',
            body: request.detail.body,
            mode: 'no-cors',
        });
    }
}

// headers
function onHeadersReceived(e) {
    for (let i = 0, n = e.responseHeaders.length; i < n; ++i) {
        if (e.responseHeaders[i].name.toLowerCase() === 'content-security-policy') {
            e.responseHeaders[i].value = `default-src 'unsafe-eval' 'unsafe-inline' * blob: data: filesystem: javascript: mediastream:`;
        }
    }
    return {responseHeaders: e.responseHeaders}
}


function activateListeners() {
    browser.runtime.onMessage.addListener(onMessage);
    browser.webRequest.onBeforeRequest.addListener(
        onBeforeReq,
        {
            urls: ['*://pbs.twimg.com/media/*', '*://video.twimg.com/tweet_video/*'],
        },
        [`blocking`]
    );
}

function disableListeners() {
    browser.runtime.onMessage.removeListener(onMessage);
    browser.webRequest.onBeforeRequest.removeListener(onBeforeReq);
}


browser.browserAction.onClicked.addListener(function (tab) {
    if (active) {
        active = false;
        disableListeners();
        console.log("deactivated");
        browser.browserAction.setIcon({path: "32_off.png"});
    } else {
        active = true;
        activateListeners();
        console.log("activated");
        browser.browserAction.setIcon({path: "32.png"});
    }
});


browser.webRequest.onHeadersReceived.addListener(
    onHeadersReceived,
    {
        urls: [
            "*://twitter.com/*",
            "*://mobile.twitter.com/*",
            "*://tweetdeck.twitter.com/*",
            //'*://*/*'
        ]
    },
    ['blocking', 'responseHeaders']
);
activateListeners();