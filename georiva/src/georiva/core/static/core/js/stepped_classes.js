/**
 * Stepped styles as classes (issue #566).
 *
 * A stepped style stores each class as two stops sharing one color — its
 * start and its end — so the edge stays hard under linear interpolation
 * (generate_stops, ADR 0022). People think in classes, not pairs, so the
 * Styling page offers a class table over the same stops. Everything here is
 * pure: stops in, classes out, and back. The page's rows stay the one thing
 * Save reads.
 *
 * A model is {bounds: [b0 … bN], colors: [c0 … cN-1]}: N classes cut by N+1
 * boundaries, class i running bounds[i] → bounds[i + 1]. Boundaries are one
 * joined list, so a gap or an overlap cannot be spelled.
 */
(function (global) {
    'use strict';

    function sameColor(a, b) {
        return String(a).toLowerCase() === String(b).toLowerCase();
    }

    /**
     * The stops as a model, or null when they are not clean class pairs:
     * an even count, each pair one color, each class starting where the
     * previous ended, every class wider than nothing. Anything else is left
     * to the raw stops table — never forced into classes.
     */
    function fromStops(stops) {
        if (!stops || stops.length < 2 || stops.length % 2 !== 0) {
            return null;
        }
        const bounds = [];
        const colors = [];
        for (let i = 0; i < stops.length; i += 2) {
            const start = stops[i];
            const end = stops[i + 1];
            if (!sameColor(start.color, end.color) || !(start.value < end.value)) {
                return null;
            }
            if (i > 0 && stops[i - 1].value !== start.value) {
                return null;
            }
            if (i === 0) {
                bounds.push(start.value);
            }
            bounds.push(end.value);
            colors.push(start.color);
        }
        return {bounds: bounds, colors: colors};
    }

    /** The model as the paired stops generate_stops would write. */
    function toStops(model) {
        const stops = [];
        model.colors.forEach(function (color, i) {
            stops.push({value: model.bounds[i], color: color});
            stops.push({value: model.bounds[i + 1], color: color});
        });
        return stops;
    }

    /** Split class i at its midpoint; the new upper half copies its color. */
    function split(model, i) {
        const mid = (model.bounds[i] + model.bounds[i + 1]) / 2;
        const bounds = model.bounds.slice();
        const colors = model.colors.slice();
        bounds.splice(i + 1, 0, mid);
        colors.splice(i + 1, 0, model.colors[i]);
        return {bounds: bounds, colors: colors};
    }

    /**
     * Merge class i into the one above it, which takes over its range; the
     * first class merges downward instead. The last remaining class stays.
     */
    function remove(model, i) {
        if (model.colors.length < 2) {
            return model;
        }
        const bounds = model.bounds.slice();
        const colors = model.colors.slice();
        if (i === 0) {
            bounds.splice(1, 1);
            colors.splice(0, 1);
        } else {
            bounds.splice(i, 1);
            colors.splice(i, 1);
        }
        return {bounds: bounds, colors: colors};
    }

    /** Boundary k set to value, or null when it would break the ordering. */
    function setBound(model, k, value) {
        if (typeof value !== 'number' || Number.isNaN(value)) {
            return null;
        }
        const below = k > 0 ? model.bounds[k - 1] : -Infinity;
        const above = k < model.bounds.length - 1 ? model.bounds[k + 1] : Infinity;
        if (!(value > below && value < above)) {
            return null;
        }
        const bounds = model.bounds.slice();
        bounds[k] = value;
        return {bounds: bounds, colors: model.colors.slice()};
    }

    /** The same color, fully transparent: #RRGGBB → #RRGGBB00. */
    function transparent(color) {
        const hex = String(color || '').trim().replace('#', '');
        const rgb = /^[0-9a-f]{6}([0-9a-f]{2})?$/i.test(hex) ? hex.slice(0, 6) : '000000';
        return '#' + rgb + '00';
    }

    global.GeoRivaSteppedClasses = {
        fromStops: fromStops,
        toStops: toStops,
        split: split,
        remove: remove,
        setBound: setBound,
        transparent: transparent
    };
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = global.GeoRivaSteppedClasses;
    }
})(typeof window !== 'undefined' ? window : globalThis);
