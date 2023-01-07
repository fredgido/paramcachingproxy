


// Twitter api
const apiFilter =
    {
        urls: ['*://api.twitter.com/*']
    };

function apiHandler(info) {
    console.log(info);
}

chrome.webRequest.onCompleted.addListener(apiHandler, apiFilter);


let active = true;

///
// Redirect Twitter image URLs to original quality
///

// Twitter image URL
const origFilter =
    {
        urls: ['*://pbs.twimg.com/media/*', '*://video.twimg.com/tweet_video/*'] /*, '*://video.twimg.com/ext_tw_video'],*/
    };

function origHandler(info) {
    let {url} = info;

    const newUrl = "http://127.0.0.1:5000/twitter_proxy?url=" + encodeURIComponent(url);
    return {redirectUrl: newUrl};
}

chrome.webRequest.onBeforeRequest.addListener(origHandler, origFilter, ['blocking']);


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


// const onHeadersReceived = function (details) {
//     for (let i = 0; i < details.responseHeaders.length; i++) {
//         if (details.responseHeaders[i].name.toLowerCase() === 'content-security-policy') {
//             details.responseHeaders[i].value = `default-src 'unsafe-inline' * blob: data: filesystem: javascript: mediastream:`;
//         }
//     }
//     return {
//         responseHeaders: details.responseHeaders
//     };
// };


function addOrReplaceHeader(responseHeaders, newHeaders) {
    newHeaders.forEach(function (header) {
        let headerPosition = responseHeaders.findIndex(x => x.name.toLowerCase() === header.name.toLowerCase());
        if (headerPosition > -1) {
            responseHeaders[headerPosition] = header;
        } else {
            responseHeaders.push(header);
        }
    }, this);
}

const onHeadersReceived = function (details) {
    let crossDomainHeaders = [
        {
            name: "access-control-allow-origin",
            value: "*"
        },
        {
            name: "access-control-allow-methods",
            value: "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS"
        },
        {
            name: "access-control-allow-headers",
            value: "*"
        },
        {
            name: "access-control-expose-headers",
            value: "*"
        },
        {
            name: "content-security-policy",
            value: "default-src 'unsafe-inline' * blob: data: filesystem: javascript: mediastream:"
        }
    ];

    addOrReplaceHeader(details.responseHeaders, crossDomainHeaders);

    return {
        responseHeaders: details.responseHeaders
    };
};

const onHeaderFilter = {
    urls: [    "*://twitter.com/*",
    "*://mobile.twitter.com/*",
    "*://tweetdeck.twitter.com/*",],
};

chrome.webRequest.onHeadersReceived.addListener(
    onHeadersReceived, onHeaderFilter, ['blocking', 'responseHeaders']
);


chrome.browserAction.onClicked.addListener(function (tab) {
    if (active) {
        active = false;
        chrome.webRequest.onHeadersReceived.removeListener(onHeadersReceived);
        chrome.webRequest.onBeforeRequest.removeListener(origHandler);
        console.log("deactivated");
        chrome.browserAction.setIcon({path: "32_off.png"});
    } else {
        active = true;
        chrome.webRequest.onHeadersReceived.addListener(onHeadersReceived, onHeaderFilter, ['blocking', 'responseHeaders']);
        chrome.webRequest.onBeforeRequest.addListener(origHandler, origFilter, ['blocking']);
        console.log("activated");
        chrome.browserAction.setIcon({path: "32.png"});

    }
});
