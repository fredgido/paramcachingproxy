'use strict';

if (!globalThis.hasOwnProperty('browser')) {
    globalThis.browser = globalThis.chrome;
}


// images
function onBeforeReq(req) {
    if (req.method === 'GET') {
        return {redirectUrl: "http://127.0.0.1:5000/twitter_proxy?url=" + encodeURIComponent(req.url)};
    }
    return {};
}

browser.webRequest.onBeforeRequest.addListener(
    onBeforeReq,
    {
        urls: ['*://pbs.twimg.com/media/*', '*://video.twimg.com/tweet_video/*'],
    },
    [`blocking`]
);

// api
function onMessage(request, sender, sendResponse) {
    console.log(`A content script sent a message: ${JSON.stringify(request)}`);

    fetch('http://localhost:1024/notify?url=' + encodeURIComponent(request.detail.url), {
        method: 'post',
        body: request.detail.body,
    });
}

browser.runtime.onMessage.addListener(onMessage);


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


browser.webRequest.onHeadersReceived.addListener(
    onHeadersReceived,
    {urls: ['*://*.twitter.com/*',]},
    ['blocking', 'responseHeaders']
);


