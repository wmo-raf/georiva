/**
 * Draws a legend from the render contract's `legend` (#790).
 *
 * Where each colour and tick sits is decided on the server
 * (`core/machine_plane/legend.py`), so every legend reads like the map and like
 * each other: a continuous style is laid out along the encoding curve, a
 * stepped one as equal-width blocks. This only draws it:
 *
 *   legend = {gradient: [[position, [r, g, b, a]], ...],   // 0–1 along the bar
 *             ticks: [[position, value], ...],              // value served
 *             labels: [tick index, ...],                    // which carry text
 *             stepped: true | false,                        // equal-width bands
 *             under: 'clamp' | 'transparent', over: ...}
 *
 * `labels` is the server's pick (#812), and this never adds to it. Only here is
 * the bar's width known, so a stepped legend's labels that would overlap on it
 * thin further, every second, third ... of them with both ends kept, and again
 * whenever the bar is resized; where even the two ends would overlap, the first
 * stands alone. A continuous legend's labels are drawn as picked. A legend with
 * no `labels` labels at most six ticks, as every legend did before #812.
 *
 * An end arrow is drawn only where the end is clamped: values beyond it take
 * the end colour. A transparent end has none, since those values are not drawn.
 *
 *   GeoRivaLegend.render(container, legend);
 *
 * Layout is styled inline; text takes the container's colour and font. The
 * bar is a `.legend-bar`, its gradient `var(--legend-gradient)`, drawn over a
 * checkerboard so a translucent or clear band reads as one: an admin page draws
 * it with `css/legend.css` beside this script, in Wagtail's theme colours; the
 * public item page with its own theme's (`georiva-map.css`).
 */
(function (global) {
    'use strict';

    const BAR_HEIGHT = 8;
    const ARROW_WIDTH = 7;
    /** Pixels of clear space between two neighbouring labels. */
    const LABEL_GAP = 4;
    /** At most this many labels on a legend that does not say which to label. */
    const MAX_LABELS = 6;

    function css(colour) {
        const alpha = colour.length > 3 ? colour[3] / 255 : 1;
        return `rgba(${colour[0]}, ${colour[1]}, ${colour[2]}, ${alpha})`;
    }

    function gradientCSS(gradient) {
        if (!gradient || gradient.length === 0) {
            return 'linear-gradient(to right, #000, #fff)';
        }
        const stops = gradient.map(([position, colour]) => `${css(colour)} ${position * 100}%`);
        return `linear-gradient(to right, ${stops.join(', ')})`;
    }

    function arrow(colour, pointsLeft) {
        const el = document.createElement('span');
        el.setAttribute('aria-hidden', 'true');
        el.style.flex = `0 0 ${ARROW_WIDTH}px`;
        el.style.height = `${BAR_HEIGHT}px`;
        el.style.background = css(colour);
        el.style.clipPath = pointsLeft ? 'polygon(100% 0, 0 50%, 100% 100%)' : 'polygon(0 0, 100% 50%, 0 100%)';
        return el;
    }

    function label(value) {
        const magnitude = Math.abs(value);
        if (magnitude !== 0 && (magnitude < 0.01 || magnitude >= 1e5)) {
            return value.toExponential(1);
        }
        // Up to two decimals, trailing zeros dropped: 0.5, 1, 200, 26.85.
        return String(Number(value.toFixed(magnitude < 10 ? 2 : 1)));
    }

    /** Every `every`-th of `count` indices and the last, in order. */
    function everyNth(count, every) {
        const picked = new Set([count - 1]);
        for (let i = 0; i < count; i += every) {
            picked.add(i);
        }
        return [...picked].sort((a, b) => a - b);
    }

    /** A legend without `labels`: at most six ticks, as before #812. */
    function capped(count) {
        return count ? everyNth(count, Math.max(1, Math.ceil((count - 1) / (MAX_LABELS - 1)))) : [];
    }

    /**
     * Which of the labels to show, given each one's `[left, right]` in pixels:
     * the densest every-nth under which no two overlap, both ends kept; the
     * first alone when even the two ends overlap.
     */
    function fitting(extents) {
        const count = extents.length;
        for (let every = 1; every < count; every++) {
            const picked = everyNth(count, every);
            const clear = picked.every(
                (index, at) => at === 0 || extents[picked[at - 1]][1] + LABEL_GAP <= extents[index][0]
            );
            if (clear) {
                return picked;
            }
        }
        return [0];
    }

    /** Shows the labels that fit `scale`'s width, hiding the rest. */
    function fit(scale, labels) {
        const width = scale.clientWidth;
        if (!width || !labels.length) {
            return;
        }
        const extents = labels.map(({element, position}) => {
            const size = element.getBoundingClientRect().width;
            const at = position * width;
            const left = position <= 0 ? at : position >= 1 ? at - size : at - size / 2;
            return [left, left + size];
        });
        const shown = new Set(fitting(extents));
        labels.forEach(({element}, index) => {
            element.style.visibility = shown.has(index) ? '' : 'hidden';
        });
    }

    function render(container, legend) {
        if (container.legendObserver) {
            container.legendObserver.disconnect();
            container.legendObserver = null;
        }
        container.replaceChildren();
        if (!legend) {
            return;
        }
        const gradient = legend.gradient || [];
        const ticks = legend.ticks || [];
        const clampedUnder = legend.under !== 'transparent';
        const clampedOver = legend.over !== 'transparent';

        const row = document.createElement('div');
        row.style.display = 'flex';
        row.style.alignItems = 'center';

        if (clampedUnder && gradient.length) {
            row.appendChild(arrow(gradient[0][1], true));
        }
        const bar = document.createElement('div');
        bar.className = 'legend-bar';
        bar.style.flex = '1 1 auto';
        bar.style.height = `${BAR_HEIGHT}px`;
        bar.style.setProperty('--legend-gradient', gradientCSS(gradient));
        row.appendChild(bar);
        if (clampedOver && gradient.length) {
            row.appendChild(arrow(gradient[gradient.length - 1][1], false));
        }
        container.appendChild(row);

        // Ticks and labels share the bar's span, between the arrows.
        const scale = document.createElement('div');
        scale.style.position = 'relative';
        scale.style.height = '1.25em';
        scale.style.marginLeft = clampedUnder ? `${ARROW_WIDTH}px` : '0';
        scale.style.marginRight = clampedOver ? `${ARROW_WIDTH}px` : '0';
        scale.style.fontSize = '0.625rem';
        scale.style.fontVariantNumeric = 'tabular-nums';

        const withLabel = new Set(legend.labels || capped(ticks.length));
        const labels = [];
        ticks.forEach(([position, value], index) => {
            const left = `${position * 100}%`;
            const mark = document.createElement('span');
            mark.style.position = 'absolute';
            mark.style.left = left;
            mark.style.top = '0';
            mark.style.width = '1px';
            mark.style.height = '3px';
            mark.style.background = 'currentColor';
            mark.style.opacity = '0.6';
            scale.appendChild(mark);

            if (!withLabel.has(index)) {
                return;
            }
            const text = document.createElement('span');
            text.textContent = label(value);
            text.style.position = 'absolute';
            text.style.left = left;
            text.style.top = '3px';
            text.style.whiteSpace = 'nowrap';
            text.style.transform =
                position <= 0 ? 'none' : position >= 1 ? 'translateX(-100%)' : 'translateX(-50%)';
            scale.appendChild(text);
            labels.push({element: text, position});
        });
        container.appendChild(scale);

        if (!legend.stepped) {
            return;
        }
        fit(scale, labels);
        if (typeof ResizeObserver !== 'undefined') {
            container.legendObserver = new ResizeObserver(() => fit(scale, labels));
            container.legendObserver.observe(scale);
        }
    }

    global.GeoRivaLegend = {render};
})(window);
