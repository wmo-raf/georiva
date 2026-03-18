import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import './styles.css';

dayjs.extend(utc);

const FLAT_THRESHOLD = 12;

const MONTHS_SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const MONTHS_LONG = [
    'January',
    'February',
    'March',
    'April',
    'May',
    'June',
    'July',
    'August',
    'September',
    'October',
    'November',
    'December'
];

// ─── pure helpers ─────────────────────────────────────────────────────────────

function buildGroups(dates) {
    const g = {};
    dates.forEach((d, i) => {
        const y = d.year(), mo = d.month(), day = d.date(), h = d.hour();
        if (!g[y]) g[y] = {};
        if (!g[y][mo]) g[y][mo] = {};
        if (!g[y][mo][day]) g[y][mo][day] = {};
        if (!g[y][mo][day][h]) g[y][mo][day][h] = [];
        g[y][mo][day][h].push(i);
    });
    return g;
}

function isCtxValid(type, ctx, grouped) {
    if (!ctx) return true;
    const {year, month, day, hour} = ctx;
    switch (type) {
        case 'month':
            return !!grouped[year];
        case 'cal':
            return !!grouped[year]?.[month];
        case 'hour':
            return !!grouped[year]?.[month]?.[day];
        case 'time':
            return !!grouped[year]?.[month]?.[day]?.[hour];
        default:
            return true;
    }
}

function sanitizeStack(stack, grouped, flat) {
    const fallback = [{type: flat ? 'flat' : 'year', ctx: null}];
    if (!stack.length) return fallback;
    const valid = [];
    for (const frame of stack) {
        if (!isCtxValid(frame.type, frame.ctx, grouped)) break;
        valid.push(frame);
    }
    return valid.length ? valid : fallback;
}

// ─── class ────────────────────────────────────────────────────────────────────

class DateTimeSelector {
    constructor(elementId, availableDates, options = {}) {
        this.root = document.getElementById(elementId);
        if (!this.root) throw new Error(`DateTimeSelector: element #${elementId} not found`);

        this.onChange = options.onChange || null;
        this._openDirection = options.openDirection || 'bottom';

        this._stack = [];
        this._savedStack = [];
        this._hasOpened = false;
        this._isOpen = false;

        this._docClickHandler = (e) => {
            if (this._isOpen && !this.root.contains(e.target)) this._closeDropdown();
        };

        this._render();
        this._loadDates(availableDates, options.selectedDate);
        this._syncWidth();
    }

    // ── public API ───────────────────────────────────────────────────────────────

    setDates(availableDates, selectedDate) {
        this._loadDates(availableDates, selectedDate);
        this._savedStack = [];
        this._hasOpened = false;
        if (this._isOpen) {
            this._stack = [];
            this._pushView(this._flat ? 'flat' : 'year', null);
        }
    }

    setDate(isoStr) {
        const idx = this.isoList.indexOf(isoStr);
        if (idx !== -1) this._select(idx);
    }

    getDate() {
        return this.isoList[this.currentIndex];
    }

    previous() {
        if (this.currentIndex > 0) this._select(this.currentIndex - 1);
        this._closeDropdown();
    }

    next() {
        if (this.currentIndex < this.dates.length - 1) this._select(this.currentIndex + 1);
        this._closeDropdown();
    }

    destroy() {
        document.removeEventListener('click', this._docClickHandler);
        window.removeEventListener('scroll', this._scrollHandler);
        window.removeEventListener('resize', this._scrollHandler);
        this.root.innerHTML = '';
    }

    // ── private: data ────────────────────────────────────────────────────────────

    _loadDates(availableDates, selectedDate) {
        this.dates = (availableDates || []).map(d => dayjs.utc(d));
        this.dates.sort((a, b) => a.valueOf() - b.valueOf());
        this.isoList = this.dates.map(d => d.toISOString());
        this._grouped = buildGroups(this.dates);
        this._flat = this.dates.length <= FLAT_THRESHOLD;
        this.currentIndex = this._resolveInitialIndex(selectedDate);
        this._updateBar();
    }

    _resolveInitialIndex(selectedDate) {
        if (!this.dates.length) return 0;
        if (selectedDate) {
            const idx = this.isoList.indexOf(selectedDate);
            if (idx !== -1) return idx;
        }
        return this.dates.length - 1;
    }

    _buildStackForCurrent() {
        if (!this.dates.length) return [];
        if (this._flat) return [{type: 'flat', ctx: null}];
        const d = this.dates[this.currentIndex];
        return [
            {type: 'year', ctx: null},
            {type: 'month', ctx: {year: d.year()}},
            {type: 'cal', ctx: {year: d.year(), month: d.month()}},
            {type: 'hour', ctx: {year: d.year(), month: d.month(), day: d.date()}},
        ];
    }

    // ── private: DOM ─────────────────────────────────────────────────────────────

    _render() {
        this.root.innerHTML = `
      <div class="dts-wrap">
        <div class="dts-bar" id="dts-bar">
          <button class="dts-nav" id="dts-prev" title="Previous">
            <svg viewBox="0 0 24 24"><polyline points="15 18 9 12 15 6"/></svg>
          </button>
          <div class="dts-divider"></div>
          <button class="dts-current" id="dts-current" title="Pick a time">—</button>
          <div class="dts-divider"></div>
          <button class="dts-nav" id="dts-next" title="Next">
            <svg viewBox="0 0 24 24"><polyline points="9 18 15 12 9 6"/></svg>
          </button>
        </div>
        <div class="dts-dropdown" id="dts-dropdown">
          <div class="dts-header">
            <button class="dts-back" id="dts-back" style="visibility:hidden">
              <svg viewBox="0 0 24 24"><polyline points="15 18 9 12 15 6"/></svg>
            </button>
            <span class="dts-title" id="dts-title"></span>
            <div style="width:24px;flex-shrink:0;"></div>
          </div>
          <div class="dts-body" id="dts-body"></div>
        </div>
      </div>`;

        this._bar = this.root.querySelector('#dts-bar');
        this._dropdown = this.root.querySelector('#dts-dropdown');
        this._btnPrev = this.root.querySelector('#dts-prev');
        this._btnNext = this.root.querySelector('#dts-next');
        this._btnCur = this.root.querySelector('#dts-current');
        this._back = this.root.querySelector('#dts-back');
        this._title = this.root.querySelector('#dts-title');
        this._body = this.root.querySelector('#dts-body');

        this._btnPrev.addEventListener('click', (e) => {
            e.stopPropagation();
            this.previous();
        });
        this._btnNext.addEventListener('click', (e) => {
            e.stopPropagation();
            this.next();
        });
        this._btnCur.addEventListener('click', (e) => {
            e.stopPropagation();
            this._toggleDropdown();
        });
        this._back.addEventListener('click', (e) => {
            e.stopPropagation();
            this._popView();
        });
        this._dropdown.addEventListener('click', (e) => e.stopPropagation());
        document.addEventListener('click', this._docClickHandler);
    }

    _syncWidth() {
        // Dropdown width + position derived from bar's viewport rect (position:fixed,
        // so it escapes overflow:hidden and backdrop-filter stacking contexts).
        const reposition = () => {
            const rect = this._bar.getBoundingClientRect();
            this._dropdown.style.width = rect.width + 'px';
            this._dropdown.style.left = rect.left + 'px';
            this._dropdown.style.top = this._openDirection === 'top'
                ? (rect.top - this._dropdown.offsetHeight - 6) + 'px'
                : (rect.bottom + 6) + 'px';
        };

        reposition();

        // Reposition when window scrolls or resizes
        this._reposition = reposition;
        this._scrollHandler = () => {
            if (this._isOpen) reposition();
        };
        window.addEventListener('scroll', this._scrollHandler, {passive: true});
        window.addEventListener('resize', this._scrollHandler, {passive: true});
    }

    // ── private: dropdown lifecycle ──────────────────────────────────────────────

    _toggleDropdown() {
        this._isOpen ? this._closeDropdown() : this._openDropdown();
    }

    _openDropdown() {
        if (!this.dates.length) return;
        this._isOpen = true;
        this._dropdown.classList.add('open');

        if (!this._hasOpened) {
            this._hasOpened = true;
            this._stack = [];
            this._pushView(this._flat ? 'flat' : 'year', null);
        } else {
            const restored = sanitizeStack(this._savedStack, this._grouped, this._flat);
            this._stack = restored;
            this._renderView();
            this._scrollToCurrent();
        }

        // reposition after content is rendered — critical for openDirection:'top'
        // since offsetHeight changes after _pushView/_renderView fills the body
        requestAnimationFrame(() => this._reposition?.());
    }

    _closeDropdown() {
        if (this._stack.length > 0) {
            this._savedStack = this._stack.map(s => ({...s}));
        }
        this._isOpen = false;
        this._dropdown.classList.remove('open');
        this._stack = [];
    }

    _scrollToCurrent() {
        requestAnimationFrame(() => {
            const sel = this._body.querySelector(
                '.dts-year-row.current-year, .dts-month-row.current-month, .dts-flat-item.selected'
            );
            sel?.scrollIntoView({block: 'nearest'});
        });
    }

    // ── private: view stack ───────────────────────────────────────────────────────

    _pushView(type, ctx) {
        this._stack.push({type, ctx});
        this._renderView();
    }

    _popView() {
        if (this._stack.length > 1) {
            this._stack.pop();
            this._renderView();
        }
    }

    _renderView() {
        if (!this._stack.length) return;
        const {type, ctx} = this._stack[this._stack.length - 1];
        this._back.style.visibility = this._stack.length > 1 ? 'visible' : 'hidden';
        ({
            flat: () => this._renderFlat(),
            year: () => this._renderYearView(),
            month: () => this._renderMonthView(ctx),
            cal: () => this._renderCalView(ctx),
            hour: () => this._renderHourView(ctx),
            time: () => this._renderTimeView(ctx),
        })[type]?.();
    }

    // ── private: renderers ────────────────────────────────────────────────────────

    _renderFlat() {
        this._title.textContent = 'Select a time';
        this._body.innerHTML = this.dates.map((d, i) =>
            `<button class="dts-flat-item ${i === this.currentIndex ? 'selected' : ''}" data-idx="${i}">
        ${d.format('YYYY-MM-DD HH:mm')} UTC
      </button>`
        ).join('');
        this._body.querySelectorAll('.dts-flat-item').forEach(btn =>
            btn.addEventListener('click', () => {
                this._select(+btn.dataset.idx);
                this._closeDropdown();
            })
        );
        requestAnimationFrame(() => {
            this._body.querySelector('.dts-flat-item.selected')?.scrollIntoView({block: 'nearest'});
        });
    }

    _renderYearView() {
        this._title.textContent = 'Select a year';
        const curYear = this.dates[this.currentIndex].year();
        const curMonth = this.dates[this.currentIndex].month();
        const years = Object.keys(this._grouped).map(Number).sort();

        this._body.innerHTML = years.map(y => {
            const monthsWithData = new Set(Object.keys(this._grouped[y]).map(Number));
            const ticks = Array.from({length: 12}, (_, m) => {
                let cls = monthsWithData.has(m) ? 'has-data' : '';
                if (y === curYear && m === curMonth) cls += ' active';
                return `<div class="dts-month-tick ${cls}"></div>`;
            }).join('');
            return `<div class="dts-year-row ${y === curYear ? 'current-year' : ''}" data-year="${y}">
        <span class="dts-year-label">${y}</span>
        <div class="dts-month-strip">${ticks}</div>
      </div>`;
        }).join('');

        this._body.querySelectorAll('.dts-year-row').forEach(row =>
            row.addEventListener('click', () => this._pushView('month', {year: +row.dataset.year}))
        );
        requestAnimationFrame(() => {
            this._body.querySelector('.dts-year-row.current-year')?.scrollIntoView({block: 'nearest'});
        });
    }

    _renderMonthView({year}) {
        this._title.textContent = String(year);
        const curYear = this.dates[this.currentIndex].year();
        const curMonth = this.dates[this.currentIndex].month();

        this._body.innerHTML = MONTHS_SHORT.map((name, m) => {
            const daysInMonth = this._grouped[year]?.[m] || {};
            const hasDays = Object.keys(daysInMonth).length > 0;
            const totalDays = new Date(year, m + 1, 0).getDate();
            const ticks = Array.from({length: totalDays}, (_, d) =>
                `<div class="dts-day-tick ${daysInMonth[d + 1] ? 'has-data' : ''}"></div>`
            ).join('');
            const isCur = hasDays && year === curYear && m === curMonth ? 'current-month' : '';
            return `<div class="dts-month-row ${hasDays ? isCur : 'no-data'}" data-month="${m}">
        <span class="dts-month-label">${name}</span>
        <div class="dts-day-strip">${ticks}</div>
      </div>`;
        }).join('');

        this._body.querySelectorAll('.dts-month-row:not(.no-data)').forEach(row =>
            row.addEventListener('click', () => this._pushView('cal', {year, month: +row.dataset.month}))
        );
        requestAnimationFrame(() => {
            this._body.querySelector('.dts-month-row.current-month')?.scrollIntoView({block: 'nearest'});
        });
    }

    _renderCalView({year, month}) {
        this._title.textContent = '';
        const daysWithData = new Set(Object.keys(this._grouped[year]?.[month] || {}).map(Number));
        const firstDay = new Date(year, month, 1).getDay();
        const totalDays = new Date(year, month + 1, 0).getDate();
        const curD = this.dates[this.currentIndex];
        const isCurMonth = curD.year() === year && curD.month() === month;

        let cells = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa']
            .map(d => `<div class="dts-cal-dow">${d}</div>`).join('');
        for (let i = 0; i < firstDay; i++) cells += '<div></div>';
        for (let d = 1; d <= totalDays; d++) {
            const hasData = daysWithData.has(d);
            const isSel = isCurMonth && curD.date() === d;
            cells += `<div class="dts-cal-day ${hasData ? 'has-data' : ''} ${isSel ? 'selected' : ''}" data-day="${d}">${d}</div>`;
        }

        this._body.innerHTML = `
      <div class="dts-cal">
        <div class="dts-cal-triggers">
          <button class="dts-cal-trigger" data-action="goto-year">${year}</button>
          <button class="dts-cal-trigger" data-action="goto-month">${MONTHS_SHORT[month]}</button>
        </div>
        <div class="dts-cal-month-title">${MONTHS_LONG[month]} ${year}</div>
        <div class="dts-cal-grid">${cells}</div>
      </div>`;

        this._body.querySelector('[data-action="goto-year"]').addEventListener('click', () => {
            this._stack = [];
            this._pushView('year', null);
        });
        this._body.querySelector('[data-action="goto-month"]').addEventListener('click', () => {
            this._stack = [];
            this._pushView('year', null);
            this._pushView('month', {year});
        });
        this._body.querySelectorAll('.dts-cal-day.has-data').forEach(el =>
            el.addEventListener('click', () =>
                this._pushView('hour', {year, month, day: +el.dataset.day})
            )
        );
    }

    _renderHourView({year, month, day}) {
        this._title.textContent = dayjs.utc(new Date(Date.UTC(year, month, day))).format('D MMM YYYY');
        const hours = this._grouped[year]?.[month]?.[day] || {};

        this._body.innerHTML = Object.keys(hours).map(Number).sort((a, b) => a - b).map(h => {
            const indices = hours[h];
            const count = indices.length;
            const text = count > 1
                ? `${String(h).padStart(2, '0')}:00 – ${String(h + 1).padStart(2, '0')}:00 <span class="dts-list-sub">(${count} options)</span>`
                : this.dates[indices[0]].format('HH:mm') + ' UTC';
            return `<button class="dts-list-item" data-hour="${h}" data-count="${count}">${text}</button>`;
        }).join('');

        this._body.querySelectorAll('.dts-list-item').forEach(btn =>
            btn.addEventListener('click', () => {
                const h = +btn.dataset.hour, count = +btn.dataset.count;
                if (count === 1) {
                    this._select(this._grouped[year][month][day][h][0]);
                    this._closeDropdown();
                } else {
                    this._pushView('time', {year, month, day, hour: h});
                }
            })
        );
    }

    _renderTimeView({year, month, day, hour}) {
        this._title.textContent = 'Select a time';
        const indices = this._grouped[year]?.[month]?.[day]?.[hour] || [];
        this._body.innerHTML = indices.map(i =>
            `<button class="dts-list-item ${i === this.currentIndex ? 'selected' : ''}" data-idx="${i}">
        ${this.dates[i].format('YYYY-MM-DD HH:mm')} UTC
      </button>`
        ).join('');
        this._body.querySelectorAll('.dts-list-item').forEach(btn =>
            btn.addEventListener('click', () => {
                this._select(+btn.dataset.idx);
                this._closeDropdown();
            })
        );
    }

    // ── private: selection ────────────────────────────────────────────────────────

    _select(index) {
        this.currentIndex = index;
        this._savedStack = this._buildStackForCurrent();
        this._updateBar();
        if (this.onChange) this.onChange(this.isoList[index], index);
    }

    _updateBar() {
        if (!this.dates.length) {
            this._btnCur.textContent = 'No data';
            this._btnPrev.disabled = true;
            this._btnNext.disabled = true;
            return;
        }
        const d = this.dates[this.currentIndex];
        this._btnCur.textContent = d.format('YYYY-MM-DD HH:mm') + ' UTC';
        this._btnPrev.disabled = this.currentIndex === 0;
        this._btnNext.disabled = this.currentIndex === this.dates.length - 1;
    }
}

export default DateTimeSelector;