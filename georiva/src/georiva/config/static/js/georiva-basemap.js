/**
 * GeoRiva's basemap, one style with every variant in it.
 *
 * The style (static/basemap/georiva-basemap.json, built by
 * scripts/build_basemap_style.py) holds each basemap, its boundaries (borders
 * and coastline) and its labels as layer groups, all hidden. A map is created from a copy
 * with the wanted groups switched on, and from then on a change is a handful of
 * setLayoutProperty calls: no setStyle, so nothing a page added on top
 * (deck's raster, the zonal-stats choropleth) is ever taken down.
 *
 * Data layers insert before `anchorId`, which sits between the basemap and the
 * boundaries and labels, so data draws above the ground and below both.
 *
 * Options, for load() and apply(): `basemap` ('light' | 'dark'), and `labels`
 * and `boundaries`, each shown unless explicitly false.
 */
(function (global) {
    'use strict';

    const GROUP_KEY = 'maplibre:group';

    const VARIANTS = ['light', 'dark'];

    const ANCHOR_ID = 'georiva-data-anchor';

    const styles = {};

    function groupsFor(options) {
        const visible = ['basemap-' + options.basemap];
        if (options.boundaries !== false) {
            visible.push('boundaries-' + options.basemap);
        }
        if (options.labels !== false) {
            visible.push('labels-' + options.basemap);
        }
        return visible;
    }

    function visibilityFor(layer, visibleGroups) {
        return visibleGroups.indexOf(layer.metadata[GROUP_KEY]) !== -1 ? 'visible' : 'none';
    }

    /**
     * Resolves to a style object with the wanted groups visible, ready to hand
     * to `new maplibregl.Map({style})`. The file is fetched once per page.
     */
    function load(url, options) {
        if (!styles[url]) {
            styles[url] = fetch(url).then(function (response) {
                if (!response.ok) {
                    throw new Error('Basemap style failed to load: ' + response.status);
                }
                return response.json();
            });
        }
        return styles[url].then(function (source) {
            const visible = groupsFor(options);
            const style = JSON.parse(JSON.stringify(source));
            style.layers.forEach(function (layer) {
                if (layer.metadata && layer.metadata[GROUP_KEY]) {
                    layer.layout.visibility = visibilityFor(layer, visible);
                }
            });
            return style;
        });
    }

    /** Shows the wanted groups of `basemap`; hides every other group. */
    function apply(map, options) {
        const visible = groupsFor(options);
        map.getStyle().layers.forEach(function (layer) {
            if (layer.metadata && layer.metadata[GROUP_KEY]) {
                map.setLayoutProperty(layer.id, 'visibility', visibilityFor(layer, visible));
            }
        });
    }

    global.GeoRivaBasemap = {
        variants: VARIANTS,
        anchorId: ANCHOR_ID,
        load: load,
        apply: apply
    };
}(window));
