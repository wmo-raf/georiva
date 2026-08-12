/**
 * A raster preview map: MapLibre basemap, deck.gl overlay, one WeatherLayers
 * raster layer over Titiler's encoded texture (ADR 0021).
 *
 * The whole point of the shape is that the texture and the palette are
 * independent. The texture holds values, not colors — the browser unscales it
 * back to physical units with `imageUnscale` and colors it from the palette on
 * the GPU. So `setPalette()` repaints without refetching anything, which is
 * what lets the styling page follow an operator's unsaved stop edits live.
 *
 * Expects `maplibregl`, `deck` and `WeatherLayers` to be loaded already.
 *
 *   const preview = GeoRivaRasterPreview.create({
 *       container: 'vs-map',
 *       textureUrl, bounds, imageUnscale, unit,
 *       palette: [[value, [r, g, b, a]], ...],
 *       onStatus: (state, message) => { ... },
 *   });
 *   preview.setPalette(nextPalette);
 */
(function (global) {
    'use strict';

    /*
     * Basemaps are source specs rather than whole styles on purpose. Switching
     * with map.setStyle() would work on a bare map, but deck's interleaved
     * overlay lives *inside* the style as a custom layer: replacing the style
     * takes the raster down with it and it does not come back. So one style is
     * built once and only the basemap source is ever swapped, beneath whatever
     * deck has added.
     */
    const BASEMAPS = {
        osm: {
            label: 'OSM',
            source: {
                type: 'raster',
                tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
                tileSize: 256,
                attribution: '© OpenStreetMap contributors'
            }
        },
        satellite: {
            label: 'Satellite',
            source: {
                type: 'raster',
                tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
                tileSize: 256,
                attribution: '© Esri'
            }
        },
        dark: {
            label: 'Dark',
            source: {
                type: 'raster',
                tiles: ['https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'],
                tileSize: 256,
                attribution: '© CARTO'
            }
        },
        light: {
            label: 'Light',
            source: {
                type: 'raster',
                tiles: ['https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'],
                tileSize: 256,
                attribution: '© CARTO'
            }
        }
    };

    const LAYER_ID = 'georiva-raster-preview';
    const BASEMAP_ID = 'georiva-basemap';

    function basemapStyle(key) {
        const sources = {};
        sources[BASEMAP_ID] = BASEMAPS[key].source;
        return {
            version: 8,
            sources: sources,
            layers: [{id: BASEMAP_ID, type: 'raster', source: BASEMAP_ID}]
        };
    }

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

        let basemap = options.basemap || 'osm';
        let palette = options.palette || null;
        let opacity = typeof options.opacity === 'number' ? options.opacity : 1;
        let image = null;
        let overlay = null;
        let tooltip = null;

        const bounds = options.bounds;

        const map = new maplibregl.Map({
            container: options.container,
            style: basemapStyle(basemap),
            bounds: [[bounds[0], bounds[1]], [bounds[2], bounds[3]]],
            fitBoundsOptions: {padding: 20},
            attributionControl: true
        });
        map.addControl(
            new maplibregl.NavigationControl({showCompass: false}), 'top-right'
        );

        function layer() {
            if (!image) {
                return [];
            }
            return [new WeatherLayers.RasterLayer({
                id: LAYER_ID,
                image: image,
                bounds: bounds,
                imageSmoothing: true,
                imageInterpolation: 'LINEAR',
                imageUnscale: options.imageUnscale,
                palette: palette,
                opacity: opacity,
                // Picking is what feeds the hover readout. The layer's picking
                // pass writes the magnitude — not the drawn color — normalized
                // over the palette's own bounds, so the value survives a
                // stepped palette. It is 8-bit and saturates at the outermost
                // stop, which is the same edge the range warning explains.
                pickable: true
            })];
        }

        function repaint() {
            if (overlay) {
                overlay.setProps({layers: layer()});
            }
        }

        map.on('load', async function () {
            overlay = new deck.MapboxOverlay({interleaved: true, layers: []});
            map.addControl(overlay);

            const instance = await waitForDeck(function () {
                return overlay._deck;
            });

            tooltip = new WeatherLayers.TooltipControl({
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
                    tooltip.updatePickingInfo(event);
                }
            });
            instance.props.onLoad();

            try {
                image = await WeatherLayers.loadTextureData(options.textureUrl);
            } catch (error) {
                console.error('GeoRiva raster preview: texture failed', error);
                onStatus('error', 'texture-failed');
                return;
            }
            repaint();
            onStatus('ready', '');
        });

        return {
            map: map,

            setPalette: function (next) {
                palette = next;
                repaint();
            },

            setOpacity: function (next) {
                opacity = next;
                repaint();
            },

            setBasemap: function (key) {
                if (!BASEMAPS[key] || key === basemap) {
                    return;
                }
                basemap = key;
                if (map.getLayer(BASEMAP_ID)) {
                    map.removeLayer(BASEMAP_ID);
                }
                if (map.getSource(BASEMAP_ID)) {
                    map.removeSource(BASEMAP_ID);
                }
                map.addSource(BASEMAP_ID, BASEMAPS[key].source);
                // Back underneath whatever is left, which is deck's own layers.
                const above = map.getStyle().layers;
                map.addLayer(
                    {id: BASEMAP_ID, type: 'raster', source: BASEMAP_ID},
                    above.length ? above[0].id : undefined
                );
            },

            fit: function () {
                map.fitBounds(
                    [[bounds[0], bounds[1]], [bounds[2], bounds[3]]],
                    {padding: 20}
                );
            }
        };
    }

    global.GeoRivaRasterPreview = {basemaps: BASEMAPS, create: create};
}(window));
