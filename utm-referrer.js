
  function setUTMCookie(utmParams) {
    var existingUTM = getCookie('utm');
    if (existingUTM) {
        var existingUTMParams = JSON.parse(existingUTM);

        // Replace only if existing UTM parameters were set by referrer (helper_ref)
        if (existingUTMParams.utm_medium === "helper_ref") {
            setCookie('utm', JSON.stringify(utmParams), 300, '.yourdomain.com');
        }
        // Otherwise, retain the existing UTM parameters
    } else {
        // If no UTM parameters in cookie, set the new ones
        setCookie('utm', JSON.stringify(utmParams), 300, '.yourdomain.com');
    }
}

function getCookie(name) {
    var nameEQ = name + "=";
    var ca = document.cookie.split(';');
    for(var i=0;i < ca.length;i++) {
        var c = ca[i];
        while (c.charAt(0)==' ') c = c.substring(1,c.length);
        if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length,c.length);
    }
    return null;
}

function setCookie(name, value, days, domain) {
    var expires = "";
    if (days) {
        var date = new Date();
        date.setTime(date.getTime() + (days*24*60*60*1000));
        expires = "; expires=" + date.toUTCString();
    }
    document.cookie = name + "=" + (value || "")  + expires + "; path=/; domain=" + domain;
}

function parseUTMParams() {
    var currentURL = new URL(window.location.href);
    var params = currentURL.searchParams;

    // Check if any UTM parameters are present in the URL
    var isUTMInURL = params.get("utm_source") || params.get("utm_medium") || 
                     params.get("utm_campaign") || params.get("utm_term") || 
                     params.get("utm_content") || params.get("gclid") || 
                     params.get("fbclid");

    if (isUTMInURL) {
        var utmParams = {
            utm_source: params.get("utm_source") ? params.get("utm_source").toLowerCase() : "",
            utm_medium: params.get("utm_medium") ? params.get("utm_medium").toLowerCase() : "",
            utm_campaign: params.get("utm_campaign") ? params.get("utm_campaign").toLowerCase() : "",
            utm_term: params.get("utm_term") ? params.get("utm_term").toLowerCase() : "",
            utm_content: params.get("utm_content") ? params.get("utm_content").toLowerCase() : "",
            utm_gclid: params.get("gclid") ? params.get("gclid").toLowerCase() : "",
            utm_fbclid: params.get("fbclid") ? params.get("fbclid").toLowerCase() : "",
        };
        setUTMCookie(utmParams);
    } else if (document.referrer) {
        var referrer = new URL(document.referrer).hostname;
        if (referrer.toLowerCase().includes("yourdomain") || referrer.toLowerCase().includes("yourdomain")) {
            // Do nothing if referrer is yourdomain1/2/3..
            return;
        }

        var hostnameParts = referrer.split(".");
        var referrerDomain = hostnameParts.length >= 2 ? hostnameParts[hostnameParts.length - 2] : "not-set";

        if (referrerDomain !== "not-set") {
            var referrerUTMParams = {
                utm_source: referrerDomain.toLowerCase(),
                utm_medium: "helper_ref",
            };
            setUTMCookie(referrerUTMParams);
        }
    }
}

// Start
parseUTMParams();
  
