///
// Redirect Twitter image URLs to original quality
///

// Twitter image URL
const origFilter =
    {
        urls: ['*://pbs.twimg.com/media/*']
    };

chrome.webRequest.onBeforeRequest.addListener(origHandler, origFilter, ['blocking']);

function origHandler(info) {
    let {url} = info;

    const newUrl = "http://127.0.0.1:5000/twitter_proxy?url=" + encodeURIComponent(url);
    return {redirectUrl: newUrl};
}


const onHeadersReceived = function (details) {
    for (let i = 0; i < details.responseHeaders.length; i++) {
        if (details.responseHeaders[i].name.toLowerCase() === 'content-security-policy') {
            console.log("removed");
            //details.responseHeaders[i].value = '';
            // details.responseHeaders[i].value = "default-src * blob: 'unsafe-inline' 'unsafe-eval'; script-src * blob: 'unsafe-inline' 'unsafe-eval'; connect-src * blob: 'unsafe-inline'; img-src * data: blob: 'unsafe-inline'; frame-src * blob: ; style-src * blob: 'unsafe-inline';";
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
