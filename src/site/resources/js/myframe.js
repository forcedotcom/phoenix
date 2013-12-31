var loc = window.location.toString();
params = loc.split('#')[1];
iframe = document.getElementById('myframe');
iframe.src=iframe.src+'#'+params;
