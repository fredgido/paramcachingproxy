console.log("hello world");


function onXhrLoad(e) {
    console.log("hook2");
    let xhr = e.target;
    let url = new URL(xhr.responseURL);

    if (url.origin === 'https://api.twitter.com') {
        queueMicrotask(() => handleApiResponse(xhr));
    }
}

function handleApiResponse(xhr) {
    console.log('intercepted xhr "load" event - response length:', xhr.responseText.length, 'url:', xhr.responseURL);
    console.log(xhr.response);
    GM.xmlHttpRequest({
        url: 'http://localhost:1024/notify?url=' + encodeURIComponent(xhr.responseURL),
        method: 'POST',
        //data: { body: xhr.response, url:xhr.responseURL},
        //binary : true,
        data: xhr.responseText,
        anonymous: true
    });
}


const injectedScript = "(" +
    function () {
        console.log("Script Injected");
        // define monkey patch function
        // const monkeyPatch = () => {
        //
        //     let oldXHROpen = window.XMLHttpRequest.prototype.open;
        //
        //     window.XMLHttpRequest.prototype.open = function () {
        //         console.log("hook");
        //         this.addEventListener("load", function () {
        //             const responseBody = this.responseText;
        //             console.log(`Response Body: ${responseBody}`);
        //         });
        //         // let xhr = new.target === undefined
        //         //     ? Reflect.apply(originalXhrFn, this, xs)
        //         //     : Reflect.construct(originalXhrFn, xs, originalXhrFn);
        //
        //         this.addEventListener('load', onXhrLoad,
        //             {once: true, passive: true, capture: true});
        //
        //         return xhr;
        //     }
        // };
        const monkeyPatch = () => {
            let oldXHROpen = window.XMLHttpRequest.prototype.open;
            window.XMLHttpRequest.prototype.open = function (...xs) {
                let xhr;
                if (new.target === undefined) {
                    xhr = Reflect.apply(oldXHROpen, this, xs);
                }else {
                    xhr = Reflect.construct(oldXHROpen, xs, oldXHROpen);
                }
                console.log("xhr " + xhr);
                xhr.addEventListener('load', onXhrLoad,
                    {once: true, passive: true, capture: true});
            };
        };
        monkeyPatch();
    } + ")();";


var script = document.createElement("script");
script.textContent = injectedScript;
(document.head || document.documentElement).appendChild(script);
script.remove();


// let oldXHROpen = window.XMLHttpRequest.prototype.open;
// window.XMLHttpRequest.prototype.open = function() {
//   this.addEventListener("load", function() {
//     const responseBody = this.responseText;
//     console.log(`Response Body: {responseBody}`);
//   });
//   return oldXHROpen.apply(this, arguments);
// };

