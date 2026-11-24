/**
 * The browser half of a style's colour table (#785).
 *
 * Which colour a texture code gets is decided once, on the server, for every
 * renderer: each style has one 256-entry colour table, and Titiler colours its
 * tiles from the same one. The browser therefore never interpolates stops for
 * the map. Its WeatherLayers layer reads codes, not values: it unscales the
 * texture by identity (`LAYER_UNSCALE`) and looks each code up in the table.
 *
 * Only the hover readout needs a value, and gets it by decoding the code over
 * the render contract's `image_unscale` (the variable's range, served). Linear
 * for now.
 *
 *   new WeatherLayers.RasterLayer({
 *       imageUnscale: GeoRivaColourTable.LAYER_UNSCALE,
 *       palette: GeoRivaColourTable.layerPalette(colourTable),
 *       ...
 *   });
 *   onHover: event => tooltip.updatePickingInfo(GeoRivaColourTable.decodePick(event, imageUnscale))
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

    /** The value a code stands for over the served range [low, high]. */
    function decode(code, range) {
        if (code == null || !Array.isArray(range)) {
            return code;
        }
        return range[0] + (code / TOP_CODE) * (range[1] - range[0]);
    }

    /**
     * A picking info whose raster value is decoded from a code into the served
     * value, for TooltipControl and click readouts. The event is not mutated.
     */
    function decodePick(info, range) {
        if (!info || !info.raster || info.raster.value == null) {
            return info;
        }
        const raster = Object.assign({}, info.raster, {value: decode(info.raster.value, range)});
        return Object.assign({}, info, {raster: raster});
    }

    global.GeoRivaColourTable = {
        LAYER_UNSCALE: LAYER_UNSCALE,
        layerPalette: layerPalette,
        decode: decode,
        decodePick: decodePick
    };
}(window));
