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
    console.log("start");
    console.log(details);
    console.log(details.responseHeaders);
    for (let i = 0; i < details.responseHeaders.length; i++) {
        if (details.responseHeaders[i].name.toLowerCase() === 'content-security-policy') {
            console.log("removed");
            //details.responseHeaders[i].value = '';
            details.responseHeaders[i].value = "default-src * 'unsafe-inline' 'unsafe-eval'; script-src * 'unsafe-inline' 'unsafe-eval'; connect-src * 'unsafe-inline'; img-src * data: blob: 'unsafe-inline'; frame-src *; style-src * 'unsafe-inline';";

        }
    }
    console.log("end");
    console.log(details);
    console.log(details.responseHeaders);
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
