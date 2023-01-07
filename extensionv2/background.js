'use strict';

if (!globalThis.hasOwnProperty('browser')) {
    globalThis.browser = globalThis.chrome;
}

let active = true;

// images
function onBeforeReq(req) {
    if (req.method === 'GET') {
        return {redirectUrl: "http://127.0.0.1:5000/twitter_proxy?url=" + encodeURIComponent(req.url)};
    }
    return {};
}

// api
function onMessage(request, sender, sendResponse) {
    fetch('http://localhost:1024/notify?url=' + encodeURIComponent(request.detail.url), {
        method: 'post',
        body: request.detail.body,
    });
}

// headers
function onHeadersReceived(e) {
    let hdrs = e.responseHeaders;

    for (let i = 0, n = hdrs.length; i < n; ++i) {
        if (hdrs[i].name.toLowerCase() === 'content-security-policy') {
            hdrs[i].value = `script-src 'self' 'unsafe-eval'; default-src 'unsafe-inline' * blob: data: filesystem: javascript: mediastream:`;
        }
    }

    return {responseHeaders: hdrs};
}


function activateListeners() {
    browser.runtime.onMessage.addListener(onMessage);
    browser.webRequest.onHeadersReceived.addListener(
        onHeadersReceived,
        {urls: ['*://*.twitter.com/*',]},
        ['blocking', 'responseHeaders']
    );
    browser.webRequest.onHeadersReceived.addListener(
        onHeadersReceived,
        {urls: ['*://*.twitter.com/*',]},
        ['blocking', 'responseHeaders']
    );
}


function disableListeners() {
    browser.runtime.onMessage.removeListener(onMessage);
    browser.webRequest.onHeadersReceived.removeListener(onHeadersReceived);
    browser.webRequest.onHeadersReceived.removeListener(onHeadersReceived);
}


chrome.browserAction.onClicked.addListener(function (tab) {
    if (active) {
        active = false;
        disableListeners();
        console.log("deactivated");
        chrome.browserAction.setIcon({path: "32_off.png"});
    } else {
        active = true;
        activateListeners();
        console.log("activated");
        chrome.browserAction.setIcon({path: "32.png"});
    }
});


activateListeners();