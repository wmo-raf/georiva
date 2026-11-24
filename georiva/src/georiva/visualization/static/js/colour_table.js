/**
 * The browser half of a style's colour table (#785).
 *
 * Which colour a texture code gets is decided once, on the server, for every
 * renderer: each style has one 256-entry colour table, and Titiler colours its
 * tiles from the same one. The browser therefore never interpolates stops for
 * the map. Its WeatherLayers layer reads codes, not values: it unscales the
 * texture by identity (`LAYER_UNSCALE`) and looks each code up in the table.
 *
 * Only the hover readout needs a value, and gets it by decoding the code
 * through the variable's encoding curve (#787): the render contract's
 * `encoding` ({curve, range}, the range stored) says which fraction of the
 * range the code stands for, and that fraction of `image_unscale` (the range,
 * served) is the served value. Decoded over the stored range first because a
 * `log` code's fraction depends on it; the unit conversion is affine, so the
 * fraction is the same in either unit. No `encoding` decodes linearly.
 *
 *   new WeatherLayers.RasterLayer({
 *       imageUnscale: GeoRivaColourTable.LAYER_UNSCALE,
 *       palette: GeoRivaColourTable.layerPalette(colourTable),
 *       ...
 *   });
 *   onHover: event => tooltip.updatePickingInfo(GeoRivaColourTable.decodePick(event, imageUnscale, encoding))
 */
(function (global) {
    'use strict';

    const TOP_CODE = 255;
    const LAYER_UNSCALE = [0, TOP_CODE];

    /** The table as a WeatherLayers palette over codes: [[code, [r, g, b, a]], ...]. */
    function layerPalette(table) {
        if (!Array.isArray(table) || table.length === 0) {
            // A styleless variable's table: grayscale.
            return [[0, [0, 0, 0, 255]], [TOP_CODE, [255, 255, 255, 255]]];
        }
        return table.map(function (colour, code) {
            return [code, colour];
        });
    }

    /**
     * Which fraction of the range (0–1) a code stands for, through the curve:
     * the inverse of `code = 255 · f(t)`, as core's `colour_table.decode`.
     */
    function fraction(code, encoding) {
        const f = code / TOP_CODE;
        const curve = encoding && encoding.curve;
        if (curve === 'sqrt') {
            return f * f;
        }
        if (curve === 'log') {
            // v = vmin · (vmax/vmin)^f over the stored range, as a fraction of it.
            const ratio = encoding.range[1] / encoding.range[0];
            return (Math.pow(ratio, f) - 1) / (ratio - 1);
        }
        return f;
    }

    /** The value a code stands for over the served range [low, high], through `encoding`'s curve. */
    function decode(code, range, encoding) {
        if (code == null || !Array.isArray(range)) {
            return code;
        }
        return range[0] + fraction(code, encoding) * (range[1] - range[0]);
    }

    /**
     * A picking info whose raster value is decoded from a code into the served
     * value, for TooltipControl and click readouts. The event is not mutated.
     */
    function decodePick(info, range, encoding) {
        if (!info || !info.raster || info.raster.value == null) {
            return info;
        }
        const raster = Object.assign({}, info.raster, {value: decode(info.raster.value, range, encoding)});
        return Object.assign({}, info, {raster: raster});
    }

    global.GeoRivaColourTable = {
        LAYER_UNSCALE: LAYER_UNSCALE,
        layerPalette: layerPalette,
        decode: decode,
        decodePick: decodePick
    };
}(window));
