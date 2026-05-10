'use strict';

(function () {

    // ── Filter accordion ──────────────────────────────────────────────────────

    document.querySelectorAll('[data-filter-toggle]').forEach(header => {
        header.addEventListener('click', () => {
            const panel = document.getElementById(header.dataset.filterToggle);
            if (panel) panel.classList.toggle('is-open');
        });
    });

    // ── Auto-submit filter form ───────────────────────────────────────────────
    // Variable and run selects submit on change.
    // Date picker selects are excluded — handled by Apply button below.

    const form = document.getElementById('grItemsFilterForm');
    if (form) {
        const pickerSelectIds = ['grPickerYear', 'grPickerMonth', 'grPickerDay', 'grPickerHour'];
        form.addEventListener('change', e => {
            if (!pickerSelectIds.includes(e.target.id)) {
                form.submit();
            }
        });
    }

    // ── Cascading date picker ─────────────────────────────────────────────────

    const picker = document.getElementById('grDatePicker');
    if (!picker) return;

    const endpoint = picker.dataset.endpoint;
    const pickerType = picker.dataset.pickerType;  // 'date' | 'month' | 'number'
    const variable = picker.dataset.variable;
    const current = picker.dataset.current;     // current ?date= value, may be ''

    let defaultYear = parseInt(picker.dataset.defaultYear, 10) || null;
    let defaultMonth = parseInt(picker.dataset.defaultMonth, 10) || null;
    let defaultDay = parseInt(picker.dataset.defaultDay, 10) || null;

    const selYear = document.getElementById('grPickerYear');
    const selMonth = document.getElementById('grPickerMonth');
    const selDay = document.getElementById('grPickerDay');
    const selHour = document.getElementById('grPickerHour');
    const btnApply = document.getElementById('grPickerApply');

    const MONTH_NAMES = [
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
    ];

    const SELECT_ORDER = [selYear, selMonth, selDay, selHour].filter(Boolean);

    // ── Helpers ───────────────────────────────────────────────────────────────

    function fetchValues(params) {
        const url = endpoint + '?' + new URLSearchParams(
            {variable, ...params}
        ).toString();
        return fetch(url).then(r => r.json());
    }

    function populate(select, values, labelFn, selectedValue) {
        const placeholder = select.options[0].cloneNode(true);
        select.innerHTML = '';
        select.appendChild(placeholder);
        values.forEach(v => {
            const opt = document.createElement('option');
            opt.value = v;
            opt.textContent = labelFn(v);
            if (String(v) === String(selectedValue)) opt.selected = true;
            select.appendChild(opt);
        });
        select.disabled = values.length === 0;
    }

    function resetDownstream(fromSelect) {
        const idx = SELECT_ORDER.indexOf(fromSelect);
        SELECT_ORDER.slice(idx + 1).forEach(s => {
            s.selectedIndex = 0;
            s.disabled = true;
        });
        updateApply();
    }

    function updateApply() {
        // Apply is enabled as soon as year has a value.
        // Downstream selects are optional.
        btnApply.disabled = !selYear || selYear.value === '';
    }

    function buildDateString() {
        const y = selYear?.value || null;
        const m = selMonth?.value || null;
        const d = selDay?.value || null;
        const h = selHour?.value || null;

        if (!y) return '';
        if (!m) return y;
        if (!d) return `${y}-${m.padStart(2, '0')}`;

        const base = `${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`;
        return h ? `${base}T${h.padStart(2, '0')}:00:00Z` : base;
    }

    // ── Cascade loaders ───────────────────────────────────────────────────────

    function loadYears(preselect) {
        fetchValues({level: 'years'}).then(({values}) => {
            populate(selYear, values, String, preselect);
            if (preselect && selYear.value === String(preselect)) {
                loadMonths(preselect, defaultMonth);
            }
            updateApply();
        });
    }

    function loadMonths(year, preselect) {
        if (!selMonth) {
            updateApply();
            return;
        }
        fetchValues({level: 'months', year}).then(({values}) => {
            populate(selMonth, values, m => MONTH_NAMES[m - 1], preselect);
            if (preselect && selMonth.value === String(preselect)) {
                loadDays(year, preselect, defaultDay);
            }
            updateApply();
        });
    }

    function loadDays(year, month, preselect) {
        if (!selDay) {
            updateApply();
            return;
        }
        fetchValues({level: 'days', year, month}).then(({values}) => {
            populate(selDay, values, String, preselect);
            if (preselect && selDay.value === String(preselect)) {
                loadHours(year, month, preselect, null);
            }
            updateApply();
        });
    }

    function loadHours(year, month, day, preselect) {
        if (!selHour) {
            updateApply();
            return;
        }
        fetchValues({level: 'hours', year, month, day}).then(({values}) => {
            populate(selHour, values, h => `${String(h).padStart(2, '0')}:00 UTC`, preselect);
            updateApply();
        });
    }

    // ── Event listeners ───────────────────────────────────────────────────────

    selYear.addEventListener('change', () => {
        resetDownstream(selYear);
        if (selYear.value) loadMonths(selYear.value, null);
    });

    selMonth?.addEventListener('change', () => {
        resetDownstream(selMonth);
        if (selMonth.value) loadDays(selYear.value, selMonth.value, null);
    });

    selDay?.addEventListener('change', () => {
        resetDownstream(selDay);
        if (selDay.value) loadHours(selYear.value, selMonth.value, selDay.value, null);
    });

    selHour?.addEventListener('change', updateApply);

    btnApply.addEventListener('click', () => {
        if (btnApply.disabled) return;
        const url = new URL(window.location.href);
        url.searchParams.set('date', buildDateString());
        url.searchParams.delete('p');
        window.location.href = url.toString();
    });

    // ── Init ─────────────────────────────────────────────────────────────────
    // Parse existing ?date= value if present, otherwise default to time_end.

    (function init() {
        let preYear = defaultYear;

        if (current) {
            const parts = current.split(/[-T:]/);
            preYear = parseInt(parts[0], 10) || null;
            defaultMonth = parseInt(parts[1], 10) || null;
            defaultDay = parseInt(parts[2], 10) || null;
        }

        loadYears(preYear);
    })();

})();
