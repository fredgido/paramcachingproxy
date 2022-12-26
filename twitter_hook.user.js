// ==UserScript==
// @name        twitter-xhr-hook-test
// @namespace   https://twitter.com/bipface
// @match       *://*.twitter.com/*
// @grant       GM.xmlHttpRequest
// @inject-into page
// ==/UserScript==

/*
note: twitter's CSP rules need to be circumvented to allow this script to be injected.
*/

'use strict';

const originalXhrFn = unsafeWindow.XMLHttpRequest;

unsafeWindow.XMLHttpRequest = function(...xs) {
	/* Was this called with the new operator? */
	let xhr = new.target === undefined
		? Reflect.apply(originalXhrFn, this, xs)
		: Reflect.construct(originalXhrFn, xs, originalXhrFn);

	xhr.addEventListener('load', onXhrLoad,
		{once: true, passive: true, capture: true});

	return xhr;
}

function onXhrLoad(e) {
	let xhr = e.target;
	let url = new URL(xhr.responseURL);

	if (url.origin === 'https://api.twitter.com') {
		queueMicrotask(() => handleApiResponse(xhr));
	}
}

function handleApiResponse(xhr) {
	console.log('intercepted xhr "load" event - response length:', xhr.responseText.length, 'url:', xhr.responseURL);
    console.log( xhr.response);
	GM.xmlHttpRequest({
		url: 'http://localhost:1024/notify?url=' + encodeURIComponent(xhr.responseURL),
		method: 'POST',
		//data: { body: xhr.response, url:xhr.responseURL},
        //binary : true,
        data : xhr.responseText,
		anonymous: true});
}
