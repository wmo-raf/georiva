(function () {
    'use strict';

    // -------------------------------------------------------------------------
    // Navbar burger toggle (mobile)
    // -------------------------------------------------------------------------
    const burger = document.getElementById('gr-navbar-burger');
    const menu = document.getElementById('gr-navbar-menu');

    if (burger && menu) {
        burger.addEventListener('click', () => {
            const isOpen = menu.classList.toggle('is-open');
            burger.classList.toggle('is-open', isOpen);
            burger.setAttribute('aria-expanded', String(isOpen));
        });
    }

    // -------------------------------------------------------------------------
    // Stats bar — live counts from STAC API
    // -------------------------------------------------------------------------
    const STAC_BASE = window.GEORIVA_STAC_URL || '/stac';

    function setStatValue(id, value) {
        const el = document.getElementById(id);
        if (el) {
            el.textContent = value;
            el.classList.remove('is-loading');
        }
    }

    async function loadStats() {
        try {
            const res = await fetch(`${STAC_BASE}/`);
            if (!res.ok) return;
            const data = await res.json();

            // Collection count from STAC /collections
            const colRes = await fetch(`${STAC_BASE}/collections?limit=1`);
            if (colRes.ok) {
                const colData = await colRes.json();
                // numberMatched is returned by most STAC APIs
                const count = colData.numberMatched ?? colData.collections?.length ?? '—';
                setStatValue('stat-collections', count);
            }
        } catch (e) {
            // Silently fail — static values remain
        }
    }

    // Only load if stats bar exists on page
    if (document.getElementById('stat-collections')) {
        loadStats();
    }

    // -------------------------------------------------------------------------
    // Dataset search — filter collection table rows by name
    // -------------------------------------------------------------------------
    const searchInput = document.getElementById('gr-dataset-search');
    const tableRows = document.querySelectorAll('.gr-collection-row');

    if (searchInput && tableRows.length) {
        searchInput.addEventListener('input', () => {
            const q = searchInput.value.toLowerCase().trim();
            tableRows.forEach(row => {
                const name = row.dataset.name || '';
                const catalog = row.dataset.catalog || '';
                const match = name.toLowerCase().includes(q) || catalog.toLowerCase().includes(q);
                row.style.display = match ? '' : 'none';
            });
        });
    }

    // -------------------------------------------------------------------------
    // Filter tags — filter collection table by time resolution
    // -------------------------------------------------------------------------
    const filterTags = document.querySelectorAll('.gr-filter-tag[data-filter]');

    if (filterTags.length && tableRows.length) {
        filterTags.forEach(tag => {
            tag.addEventListener('click', () => {
                const active = tag.classList.contains('is-active');

                // Clear all active tags
                filterTags.forEach(t => t.classList.remove('is-active'));

                if (!active) {
                    tag.classList.add('is-active');
                    const filterVal = tag.dataset.filter;
                    tableRows.forEach(row => {
                        const match = !filterVal || row.dataset.resolution === filterVal;
                        row.style.display = match ? '' : 'none';
                    });
                } else {
                    // Toggle off — show all
                    tableRows.forEach(row => row.style.display = '');
                }
            });
        });
    }

})();
