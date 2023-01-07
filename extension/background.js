'use strict';

if (!globalThis.hasOwnProperty('browser')) {
    globalThis.browser = globalThis.chrome;
}

let active = true;

// images
function onBeforeReq(req) {
    return {redirectUrl: "http://127.0.0.1:5000/twitter_proxy?url=" + encodeURIComponent(req.url)};
}

// api
function onMessage(request, sender, sendResponse) {
    fetch('http://localhost:1024/notify?url=' + encodeURIComponent(request.detail.url), {
        method: 'post',
        body: request.detail.body,
        mode: 'no-cors',
    });
}

// headers
function onHeadersReceived(e) {
    let hdrs = e.responseHeaders;
    for (let i = 0, n = hdrs.length; i < n; ++i) {
        if (hdrs[i].name.toLowerCase() === 'content-security-policy') {
            hdrs[i].value = `default-src 'unsafe-eval' 'unsafe-inline' * blob: data: filesystem: javascript: mediastream:`;
        }
        // if (hdrs[i].name.toLowerCase() === 'access-control-allow-origin') {
        //     hdrs[i].value = hdrs[i].value + " ";
        // }
    }
    return {responseHeaders: hdrs};
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


browser.webRequest.onHeadersReceived.addListener(
    onHeadersReceived,
    {
        urls: [
            // "*://twitter.com/*",
            // "*://mobile.twitter.com/*",
            // "*://tweetdeck.twitter.com/*",
            '*://*/*'
        ]
    },
    ['blocking', 'responseHeaders']
);
activateListeners();