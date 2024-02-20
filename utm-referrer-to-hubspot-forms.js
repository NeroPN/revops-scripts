// ADJUST YOUR DOMAIN ON ROW 65

function CustomTelemetry() {
    this.init = function() {
        // Initialize functionalities in the same order as the Telemetry script
        this.setUTMCookie();
        this.watchHubspotForms();
    };

    this.setUTMCookie = function() {
        var existingUTM = this.getCookie('utm');
        var shouldSetCookie = true;

        if (existingUTM) {
            var existingUTMParams = JSON.parse(existingUTM);
            if (existingUTMParams.utm_medium !== "helper_referrer") {
                shouldSetCookie = false;
            }
        }

        if (shouldSetCookie) {
            var params = new URLSearchParams(window.location.search);
            var utmParams = {};

            ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content'].forEach(function(param) {
                if (params.get(param)) {
                    utmParams[param] = params.get(param).toLowerCase();
                }
            });

            if (Object.keys(utmParams).length === 0 && document.referrer) {
                var referrerURL = new URL(document.referrer);
                var referrerDomain = referrerURL.hostname.split(".").slice(-2).join(".");
                
                utmParams = {
                    utm_source: referrerDomain.toLowerCase(),
                    utm_medium: "helper_referrer"
                };
            }

            if (Object.keys(utmParams).length > 0) {
                this.setCookie('utm', JSON.stringify(utmParams), 14);
            }
        }
    };

    this.getCookie = function(name) {
        var nameEQ = name + "=";
        var ca = document.cookie.split(';');
        for(var i=0; i < ca.length; i++) {
            var c = ca[i];
            while (c.charAt(0) === ' ') c = c.substring(1, c.length);
            if (c.indexOf(nameEQ) === 0) return c.substring(nameEQ.length, c.length);
        }
        return null;
    };

    this.setCookie = function(name, value, days) {
        var expires = "";
        if (days) {
            var date = new Date();
            date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
            expires = "; expires=" + date.toUTCString();
        }
        var domain = ".yourdomain.com"; // Adjust your domain here
        document.cookie = name + "=" + (value || "") + expires + "; path=/; domain=" + domain;
    };

    this.watchHubspotForms = function() {
        var scope = this;
        window.addEventListener('message', function(event) {
            if (event.data.type === 'hsFormCallback' && event.data.eventName === 'onFormReady') {
                scope.processUTMParams();
            }
        });
    };

    this.populateHSField = function(fields, value) {
        Array.prototype.forEach.call(fields, function(field) {
            var input = field.getElementsByTagName("input")[0];
            if(input) {
                input.value = value || "not-set";
                input.dispatchEvent(new Event("change"));
            }
        });
    };

    this.processUTMParams = function() {
        var utmParams = this.getCookie("utm");
        if (utmParams) {
            try {
                utmParams = JSON.parse(utmParams);

                ['utm_medium', 'utm_source', 'utm_campaign', 'utm_term', 'utm_content'].forEach(function(utmKey) {
                    var fields = document.getElementsByClassName("hs_" + utmKey);
                    console.log(fields);
                    this.populateHSField(fields, utmParams[utmKey]);
                    console.log(utmParams[utmKey]);
                }, this); // Ensure the correct context is passed to forEach
            } catch(e) {
                console.error("Could not parse UTM session:", e);
            }
        }
    };

    // Initialize CustomTelemetry
    this.init();
}

// Start CustomTelemetry
new CustomTelemetry();
