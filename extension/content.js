'use strict';

if (!globalThis.hasOwnProperty('browser')) {
    globalThis.browser = globalThis.chrome;
}

globalThis.addEventListener('twitter-proxy-page-script:api-response',
    function (e) {
        e.stopImmediatePropagation();
        browser.runtime.sendMessage({type: e.type, detail: e.detail});
    },
    {passive: true, capture: true});

const pageScript = `
	(function() {
		'use strict';

		const originalXhrFn = globalThis.XMLHttpRequest;

		globalThis.XMLHttpRequest = function(...xs) {
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
            console.log(url);
            console.log(xhr.responseURL);

			if (url.origin === 'https://api.twitter.com' ||  ((url.origin === 'https://twitter.com' || url.origin === 'https://tweetdeck.twitter.com') && url.pathname.startsWith('/i/api') ) ) {
				console.log(url.origin,url.pathname, "accepted");
				queueMicrotask(() => handleApiResponse(xhr));
			}
		}
		
		function handleApiResponse(xhr) {
			console.log('intercepted xhr "load" event - response length:', xhr.responseText.length, 'url:', xhr.responseURL);

			globalThis.dispatchEvent(
				new CustomEvent('twitter-proxy-page-script:api-response',
					{detail: {
						url: xhr.responseURL,
						body: xhr.responseText.toString()}}))
		}
	})();
`;

var script = document.createElement('script');
script.id = 'twitter-proxy-page-script';
script.textContent = pageScript;
if (!document.getElementById('twitter-proxy-page-script')) {
    (document.head ?? document.documentElement).appendChild(script);
}
