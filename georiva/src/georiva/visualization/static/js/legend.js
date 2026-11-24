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
 *             under: 'clamp' | 'transparent', over: ...}
 *
 * An end arrow is drawn only where the end is clamped: values beyond it take
 * the end colour. A transparent end has none, since those values are not drawn.
 *
 *   GeoRivaLegend.render(container, legend);
 *
 * Layout is styled inline; text takes the container's colour and font. The
 * bar is a `.legend-bar` whose background each page draws in its own theme's
 * colours: its gradient, `var(--legend-gradient)`, over a checkerboard, so a
 * translucent or clear band reads as one.
 */
(function (global) {
    'use strict';

    const BAR_HEIGHT = 8;
    const ARROW_WIDTH = 7;
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

    /** Which ticks get a label: at most MAX_LABELS, evenly picked, both ends always. */
    function labelled(count) {
        const every = Math.max(1, Math.ceil((count - 1) / (MAX_LABELS - 1)));
        const picked = new Set([0, count - 1]);
        for (let i = 0; i < count; i += every) {
            picked.add(i);
        }
        return picked;
    }

    function render(container, legend) {
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

        const withLabel = labelled(ticks.length);
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
        });
        container.appendChild(scale);
    }

    global.GeoRivaLegend = {render};
})(window);
