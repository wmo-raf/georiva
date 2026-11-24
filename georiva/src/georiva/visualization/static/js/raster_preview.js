/**
 * A raster preview map: MapLibre basemap, deck.gl overlay, one WeatherLayers
 * raster layer over Titiler's encoded texture (ADR 0021).
 *
 * The whole point of the shape is that the texture and the colours are
 * independent. The texture holds codes, not colors, and the layer colors each
 * code from the style's colour table on the GPU — the table Titiler colours
 * tiles with (#785), so it is given whole, never worked out from stops here.
 * `setColourTable()` repaints without refetching the texture, which is what
 * lets the styling page follow an operator's unsaved stop edits live.
 * `imageUnscale` is the variable's served range: what the hover readout
 * decodes a code back into a value over.
 *
 * Expects `maplibregl`, `deck`, `WeatherLayers`, `GeoRivaColourTable` and `GeoRivaBasemap` to be
 * loaded already. The basemap is the shared GeoRiva style; switching it flips
 * layer groups rather than reloading, so the deck overlay is built once.
 *
 *   const preview = GeoRivaRasterPreview.create({
 *       container: 'vs-map',
 *       basemapStyleUrl,
 *       textureUrl, bounds, imageUnscale, unit, compactAttribution,
 *       colourTable: [[r, g, b, a], ...],  // 256 entries, one per code
 *       onStatus: (state, message) => { ... },
 *   });
 *   preview.setColourTable(nextTable);
 */
(function (global) {
    'use strict';

    const LAYER_ID = 'georiva-raster-preview';

    function waitForDeck(getDeck) {
        return new Promise(function (resolve) {
            function wait() {
                const instance = getDeck();
                if (instance && instance.getCanvas()) {
                    resolve(instance);
                } else {
                    setTimeout(wait, 50);
                }
            }

            wait();
        });
    }

    function hasWebGL() {
        try {
            const canvas = document.createElement('canvas');
            return Boolean(
                canvas.getContext('webgl2') || canvas.getContext('webgl')
            );
        } catch (error) {
            return false;
        }
    }

    function create(options) {
        const noop = function () {
        };
        const onStatus = options.onStatus || noop;

        if (!hasWebGL()) {
            onStatus('error', 'no-webgl');
            return null;
        }

        let basemap = options.basemap || 'light';
        let colourTable = options.colourTable || null;
        let opacity = typeof options.opacity === 'number' ? options.opacity : 1;
        let image = null;
        let overlay = null;
        let map = null;

        let bounds = options.bounds;
        let textureUrl = options.textureUrl;
        let fitBounds = [[bounds[0], bounds[1]], [bounds[2], bounds[3]]];

        function layer() {
            if (!image) {
                return [];
            }
            return [new WeatherLayers.RasterLayer({
                id: LAYER_ID,
                // Between the basemap and its labels, so the labels stay readable.
                beforeId: GeoRivaBasemap.anchorId,
                image: image,
                bounds: bounds,
                imageSmoothing: true,
                imageInterpolation: 'LINEAR',
                imageUnscale: GeoRivaColourTable.LAYER_UNSCALE,
                palette: GeoRivaColourTable.layerPalette(colourTable),
                opacity: opacity,
                // Picking is what feeds the hover readout. The layer's picking
                // pass writes the code — not the drawn color — so the value
                // survives a stepped table; the readout decodes it.
                pickable: true
            })];
        }

        function repaint() {
            if (overlay) {
                overlay.setProps({layers: layer()});
            }
        }

        async function onLoad() {
            overlay = new deck.MapboxOverlay({interleaved: true, layers: []});
            map.addControl(overlay);

            const instance = await waitForDeck(function () {
                return overlay._deck;
            });

            const tooltip = new WeatherLayers.TooltipControl({
                followCursor: true,
                unitFormat: {unit: options.unit || ''}
            });
            instance.setProps({
                onLoad: function () {
                    const canvas = instance.getCanvas();
                    if (canvas) {
                        tooltip.addTo(canvas.parentElement);
                    }
                },
                onHover: function (event) {
                    tooltip.updatePickingInfo(GeoRivaColourTable.decodePick(event, options.imageUnscale));
                }
            });
            instance.props.onLoad();

            try {
                image = await WeatherLayers.loadTextureData(textureUrl);
            } catch (error) {
                console.error('GeoRiva raster preview: texture failed', error);
                onStatus('error', 'texture-failed');
                return;
            }
            repaint();
            onStatus('ready', '');
        }

        // The map needs the style in hand, so it exists a moment after create()
        // returns. Calls made before then are remembered and applied on load.
        GeoRivaBasemap.load(options.basemapStyleUrl, {basemap: basemap}).then(function (style) {
            map = new maplibregl.Map({
                container: options.container,
                style: style,
                bounds: fitBounds,
                fitBoundsOptions: {padding: 20},
                attributionControl: options.compactAttribution ? {compact: true} : true
            });
            map.addControl(
                new maplibregl.NavigationControl({showCompass: false}), 'top-right'
            );
            map.once('load', function () {
                if (options.compactAttribution) {
                    // MapLibre's compact attribution starts open until the map
                    // is first moved; start it as the small (i) button instead.
                    const attribution = map.getContainer().querySelector('.maplibregl-ctrl-attrib');
                    if (attribution) {
                        attribution.classList.remove('maplibregl-compact-show');
                        attribution.removeAttribute('open');
                    }
                }
                onLoad();
            });
        }).catch(function (error) {
            console.error('GeoRiva raster preview: basemap style failed', error);
            onStatus('error', 'style-failed');
        });

        return {
            setColourTable: function (next) {
                colourTable = next;
                repaint();
            },

            /**
             * Swap the drawn item in place. The colour table, opacity and basemap
             * stay; the old texture stays on screen until the new one has
             * loaded, so stepping through time does not flash empty. Resolves
             * true once drawn, false when the texture failed.
             */
            setTexture: async function (nextUrl, nextBounds) {
                textureUrl = nextUrl;
                if (nextBounds) {
                    bounds = nextBounds;
                    fitBounds = [[bounds[0], bounds[1]], [bounds[2], bounds[3]]];
                }
                if (!overlay) {
                    // Still loading: onLoad reads textureUrl when it gets there.
                    return true;
                }
                try {
                    const next = await WeatherLayers.loadTextureData(nextUrl);
                    if (nextUrl !== textureUrl) {
                        return true;  // a later step superseded this one
                    }
                    image = next;
                } catch (error) {
                    console.error('GeoRiva raster preview: texture failed', error);
                    return false;
                }
                repaint();
                return true;
            },

            setOpacity: function (next) {
                opacity = next;
                repaint();
            },

            setBasemap: function (key) {
                if (GeoRivaBasemap.variants.indexOf(key) === -1 || key === basemap) {
                    return;
                }
                basemap = key;
                if (map) {
                    GeoRivaBasemap.apply(map, {basemap: basemap});
                }
            },

            fit: function () {
                if (map) {
                    map.fitBounds(fitBounds, {padding: 20});
                }
            }
        };
    }

    global.GeoRivaRasterPreview = {create: create};
}(window));
