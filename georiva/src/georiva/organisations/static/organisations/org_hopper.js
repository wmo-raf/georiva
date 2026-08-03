/* Mounts the org-hopper workspace block into Wagtail's sidebar and works its popover.
 *
 * Behaviour only. Everything the block says — which organisations, which one is
 * current, whether there is a popover at all — was decided server-side and
 * arrives as ready-made markup through mount(), which the per-request script at
 * /admin/org-hopper.js calls. See organisations/hopper.py and ADR 0017.
 *
 * The sidebar is a React app that renders after this file runs, and re-renders
 * when it is collapsed or the viewport changes — either of which can drop a node
 * it does not know about. So the block is not inserted once and forgotten: an
 * observer stays on the sidebar and puts it back if it goes.
 */
const georivaOrgHopper = {
    SIDEBAR: '#wagtail-sidebar',
    ANCHOR: '.sidebar-main-menu',

    markup: null,
    observer: null,

    /** Take the server-rendered block and keep it mounted for the page's lifetime. */
    mount(markup) {
        this.markup = markup;
        // One document-level listener for the page, not one per insertion: the
        // block is re-created whenever React drops it, and per-block listeners
        // would pile up on a long-lived admin page.
        document.addEventListener('click', (event) => this.closeUnlessInside(event.target));
        this.watch();
    },

    /** Insert the block now, and again whenever the sidebar re-renders without it. */
    watch() {
        const sidebar = document.querySelector(this.SIDEBAR);
        if (!sidebar) {
            // Only reachable if this script runs before the sidebar element is
            // parsed; deferred scripts do not, but a future include order might.
            document.addEventListener('DOMContentLoaded', () => this.watch(), { once: true });
            return;
        }
        this.insert();
        this.observer = new MutationObserver(() => this.insert());
        this.observer.observe(sidebar, { childList: true, subtree: true });
    },

    /** Put the block above the main menu, unless it is already there. */
    insert() {
        const sidebar = document.querySelector(this.SIDEBAR);
        if (!sidebar || sidebar.querySelector('[data-gr-orghop]')) {
            return;
        }
        const anchor = sidebar.querySelector(this.ANCHOR);
        if (!anchor) {
            return;
        }
        const holder = document.createElement('template');
        holder.innerHTML = this.markup.trim();
        const block = holder.content.firstElementChild;
        if (!block) {
            return;
        }
        anchor.parentNode.insertBefore(block, anchor);
        this.wire(block);
    },

    /** Wire this block's own controls: the toggle, Escape, and the search filter. */
    wire(block) {
        const trigger = block.querySelector('[data-gr-orghop-trigger]');
        const popover = block.querySelector('[data-gr-orghop-popover]');
        if (!trigger || !popover) {
            // Single-org users get a static badge — nothing to open.
            return;
        }

        trigger.addEventListener('click', () => this.setOpen(block, popover.hidden));

        block.addEventListener('keydown', (event) => {
            if (event.key === 'Escape' && !popover.hidden) {
                this.setOpen(block, false);
                trigger.focus();
            }
        });

        const search = block.querySelector('[data-gr-orghop-search]');
        if (search) {
            search.addEventListener('input', () => this.filter(block, search.value));
        }
    },

    closeUnlessInside(target) {
        const block = document.querySelector('[data-gr-orghop]');
        if (block && !block.contains(target)) {
            this.setOpen(block, false);
        }
    },

    setOpen(block, open) {
        const trigger = block.querySelector('[data-gr-orghop-trigger]');
        const popover = block.querySelector('[data-gr-orghop-popover]');
        if (!trigger || !popover) {
            return;
        }
        popover.hidden = !open;
        block.classList.toggle('gr-orghop--open', open);
        trigger.setAttribute('aria-expanded', open ? 'true' : 'false');
        const search = block.querySelector('[data-gr-orghop-search]');
        if (open && search) {
            search.focus();
        }
    },

    filter(block, query) {
        const needle = query.trim().toLowerCase();
        let shown = 0;
        block.querySelectorAll('[data-gr-orghop-item]').forEach((item) => {
            const matches = needle === '' || item.dataset.grOrghopHaystack.includes(needle);
            item.hidden = !matches;
            if (matches) {
                shown += 1;
            }
        });
        const empty = block.querySelector('[data-gr-orghop-empty]');
        if (empty) {
            empty.hidden = shown > 0;
        }
    },
};

window.georivaOrgHopper = georivaOrgHopper;
