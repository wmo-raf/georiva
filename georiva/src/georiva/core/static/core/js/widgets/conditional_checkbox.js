(function () {
    'use strict';

    function togglePanel(checkbox) {
        const targetId = checkbox.dataset.conditionalTarget;
        const section = document.getElementById(targetId);
        if (!section) return;
        section.style.display = checkbox.checked ? '' : 'none';
    }

    function init() {
        document.querySelectorAll('input[data-conditional-target]').forEach(checkbox => {
            checkbox.addEventListener('change', () => togglePanel(checkbox));
            togglePanel(checkbox); // set initial state
        });
    }

    document.addEventListener('DOMContentLoaded', init);
})();