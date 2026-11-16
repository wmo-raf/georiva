// While an Upstream survey runs off the request, look again a moment later.
// The survey is the worker's, so a reload starts nothing (#667, #695).
document.addEventListener('DOMContentLoaded', function () {
    if (!document.querySelector('[data-survey-waiting]')) return;
    window.setTimeout(function () {
        window.location.reload();
    }, 2000);
});
