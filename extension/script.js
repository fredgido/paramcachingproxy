///
// Redirect Twitter image URLs to original quality
///

// Twitter image URL
const origFilter =
    {
        urls: ['*://pbs.twimg.com/media/*','*://video.twimg.com/tweet_video/*'] /*, '*://video.twimg.com/ext_tw_video'],*/
    };

chrome.webRequest.onBeforeRequest.addListener(origHandler, origFilter, ['blocking']);

function origHandler(info) {
    let {url} = info;

    const newUrl = "http://127.0.0.1:5000/twitter_proxy?url=" + encodeURIComponent(url);
    return {redirectUrl: newUrl};
}

/*
// Twitter video URL scrubbed idea
const videoFilter =
    {
        urls: ['*://video.twimg.com/ext_tw_video/!*']
    };

chrome.webRequest.onBeforeRequest.addListener(origHandler, origFilter, ['blocking']);

function videoHandler(info) {
    let {url} = info;

    const newUrl = "http://127.0.0.1:5000/twitter_proxy?video=" + encodeURIComponent(url);
    return {redirectUrl: newUrl};
}
*/


const onHeadersReceived = function (details) {
    for (let i = 0; i < details.responseHeaders.length; i++) {
        if (details.responseHeaders[i].name.toLowerCase() === 'content-security-policy') {
            details.responseHeaders[i].value = `'unsafe-inline' * blob: data: filesystem: javascript: mediastream:`;
        }
    }
    return {
        responseHeaders: details.responseHeaders
    };
};

const onHeaderFilter = {
    urls: ['*://*/*'],
};

chrome.webRequest.onHeadersReceived.addListener(
    onHeadersReceived, onHeaderFilter, ['blocking', 'responseHeaders']
);
