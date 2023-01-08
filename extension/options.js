if (!globalThis.hasOwnProperty('browser')) {
    globalThis.browser = globalThis.chrome;
}


const settingsDefaults = browser.settingsDefaults;

async function saveOptions(e) {
    e.preventDefault();
    let proxyUrl = document.getElementById('proxyUrl').value;
    let apiUrl = document.getElementById('apiUrl').value;
    let originalProxyEnabled = document.getElementById('originalProxyEnabled').value;
    browser.storage.local.set({
        proxyUrl: proxyUrl || settingsDefaults.proxyUrl,
        apiUrl: apiUrl || settingsDefaults.apiUrl,
        originalProxyEnabled: originalProxyEnabled,
    }, function () {
        console.log("options saved")
    });
}

async function restoreOptions() {
    browser.storage.local.get(settingsDefaults, function (items) {
        document.getElementById('proxyUrl').value = items.proxyUrl || settingsDefaults.proxyUrl;
        document.getElementById('apiUrl').value = items.apiUrl || settingsDefaults.apiUrl;
        document.getElementById('originalProxyEnabled').value = items.originalProxyEnabled;
    });
}

document.addEventListener("DOMContentLoaded", restoreOptions);
document.querySelector("form").addEventListener("submit", saveOptions);
