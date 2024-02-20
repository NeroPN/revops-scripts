
  // Configurable portal ID and form ID
  var portalID = 'HUBSPOT-ID';
  var formID = 'HUBSPOT-FORM-ID';

  window.addEventListener('message', function (event) {
    if (event.data.meetingBookSucceeded) {
      var userEmail = event.data.meetingsPayload.bookingResponse.postResponse.contact.email;
      formv3(userEmail); 
    }
  });

  function formv3(a) {
    var utmData = getCookie('utm') ? JSON.parse(getCookie('utm')) : {};

    var utmMedium = utmData['utm_medium'] || '';
    var utmSource = utmData['utm_source'] || '';
    var utmCampaign = utmData['utm_campaign'] || '';
    var utmTerm = utmData['utm_term'] || '';
    var utmContent = utmData['utm_content'] || '';

    // Get time
    var currentTime = Date.now();

    // Get the hubspotutk cookie value
    var hubspotutk = getCookie('hubspotutk');

    var data = {
      "submittedAt": currentTime.toString(),
      "fields": [
        {
          "objectTypeId": "0-1",
          "name": "email",
          "value": a
        }
      ],
      "context": {
        "hutk": hubspotutk,
        "pageUri": window.location.href
      }
    };

    addUTMField(data.fields, "utm_medium", utmMedium);
    addUTMField(data.fields, "utm_source", utmSource);
    addUTMField(data.fields, "utm_campaign", utmCampaign);
    addUTMField(data.fields, "utm_term", utmTerm);
    addUTMField(data.fields, "utm_content", utmContent);
    
    // Dynamically construct the URL using the portalID and formID variables
    var url = `https://api.hsforms.com/submissions/v3/integration/submit/${portalID}/${formID}`;
    var final_data = JSON.stringify(data);

    fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: final_data
    })
    .then(function(response) {
      if (response.ok) {
        return response.json();
      } else {
        throw new Error('API call failed');
      }
    })
    .then(function(responseData) {
      console.log(JSON.stringify(responseData)); 
    })
    .catch(function(error) {
      console.log(error.message); // Handle any errors
    });
  }

  function addUTMField(fields, fieldName, fieldValue) {
    if (fieldValue) {
      fields.push({
        "objectTypeId": "0-1",
        "name": fieldName,
        "value": fieldValue
      });
    }
  }

  function getCookie(name) {
    var value = "; " + document.cookie;
    var parts = value.split("; " + name + "=");
    if (parts.length === 2) return parts.pop().split(";").shift();
  }

