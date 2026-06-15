'use strict';

(function () {

    // ── Config ────────────────────────────────────────────────────────────────

    const CONFIG = JSON.parse(document.getElementById('grItemConfig').textContent);

    const BASEMAPS = {
        dark: {
            name: 'Dark',
            style: {
                version: 8,
                sources: { carto: { type: 'raster', tileSize: 256, attribution: '© CARTO', tiles: ['https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'] } },
                layers: [{ id: 'carto', type: 'raster', source: 'carto' }],
            },
        },
        light: {
            name: 'Light',
            style: {
                version: 8,
                sources: { carto: { type: 'raster', tileSize: 256, attribution: '© CARTO', tiles: ['https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'] } },
                layers: [{ id: 'carto', type: 'raster', source: 'carto' }],
            },
        },
        satellite: {
            name: 'Satellite',
            style: {
                version: 8,
                sources: { satellite: { type: 'raster', tileSize: 256, attribution: '© Esri', tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'] } },
                layers: [{ id: 'satellite', type: 'raster', source: 'satellite' }],
            },
        },
        osm: {
            name: 'OSM',
            style: {
                version: 8,
                sources: { osm: { type: 'raster', tileSize: 256, attribution: '© OpenStreetMap contributors', tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'] } },
                layers: [{ id: 'osm', type: 'raster', source: 'osm' }],
            },
        },
    };

    const BOUNDARY_SOURCE  = 'gr-boundary-source';
    const BOUNDARY_FILL    = 'gr-boundary-fill';
    const BOUNDARY_LINE    = 'gr-boundary-line';
    const BOUNDARY_HOVER   = 'gr-boundary-hover';
    const LIGHT_BASEMAPS   = new Set(['light', 'osm']);

    // ── State ─────────────────────────────────────────────────────────────────

    let map            = null;
    let deckOverlay    = null;
    let deckgl         = null;
    let tooltipControl = null;
    let currentRasterLayer = null;
    let currentBasemap = 'dark';
    let currentVarSlug = CONFIG.activeVarSlug;
    let layerMode      = 'raster';   // 'raster' | 'boundaries' | 'both'
    let activeLevel    = CONFIG.boundaryStatsLevels?.[0] ?? null;
    let hoveredBoundaryId = null;
    let edrData        = null;
    let parameterNames = {};
    let currentPalette = null;

    // ── Map init ──────────────────────────────────────────────────────────────

    map = new maplibregl.Map({
        container: 'grMap',
        style: BASEMAPS[currentBasemap].style,
        center: [0, 20],
        zoom: 2,
        attributionControl: false,
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right');

    map.on('mousemove', e => {
        const el = document.getElementById('grMapCoords');
        if (el) el.textContent = `${e.lngLat.lat.toFixed(4)}, ${e.lngLat.lng.toFixed(4)}`;
    });

    map.on('load', async () => {
        deckOverlay = new deck.MapboxOverlay({ interleaved: true, layers: [] });
        map.addControl(deckOverlay);
        deckgl = await waitForDeck(() => deckOverlay._deck);

        setupBasemapSelector();
        setupOpacitySlider();
        setupVariableButtons();
        setupLayerSwitcher();
        setupLevelSelector();
        updateTheme(currentBasemap);

        await loadEdrMetadata();
    });

    function waitForDeck(getDeck) {
        return new Promise(resolve => {
            (function wait() {
                const d = getDeck();
                if (d && d.getCanvas()) resolve(d);
                else setTimeout(wait, 100);
            })();
        });
    }

    // ── EDR metadata ──────────────────────────────────────────────────────────

    async function loadEdrMetadata() {
        try {
            const res = await fetch(CONFIG.edrUrl);
            if (!res.ok) throw new Error(`EDR fetch failed: ${res.status}`);
            edrData = await res.json();
        } catch (e) {
            console.error('Failed to load EDR metadata:', e);
            return;
        }

        parameterNames = edrData.parameter_names || {};

        const bbox = edrData.extent?.spatial?.bbox?.[0];
        if (bbox?.length === 4) {
            map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 60, duration: 500 });
        }

        await applyLayerMode();
    }

    // ── Layer mode orchestration ──────────────────────────────────────────────

    async function applyLayerMode() {
        showLoading(true);

        if (layerMode === 'raster') {
            removeBoundaryLayer();
            await loadRasterLayer();
        } else {
            clearRasterLayer();
            removeBoundaryLayer();
            await loadBoundaryLayer();
        }

        showLoading(false);
    }

    // ── Raster layer (WeatherLayers PNG) ──────────────────────────────────────

    function buildAssetUrl(varSlug) {
        const dt = new Date(CONFIG.itemTime);
        const Y  = dt.getUTCFullYear();
        const m  = String(dt.getUTCMonth() + 1).padStart(2, '0');
        const d  = String(dt.getUTCDate()).padStart(2, '0');
        const HH = String(dt.getUTCHours()).padStart(2, '0');
        const MM = String(dt.getUTCMinutes()).padStart(2, '0');
        const SS = String(dt.getUTCSeconds()).padStart(2, '0');

        let filename = `${varSlug}_${HH}${MM}${SS}`;

        if (CONFIG.referenceTime) {
            const rt = new Date(CONFIG.referenceTime);
            const refStr = [
                rt.getUTCFullYear(),
                String(rt.getUTCMonth() + 1).padStart(2, '0'),
                String(rt.getUTCDate()).padStart(2, '0'),
                'T',
                String(rt.getUTCHours()).padStart(2, '0'),
                String(rt.getUTCMinutes()).padStart(2, '0'),
                String(rt.getUTCSeconds()).padStart(2, '0'),
            ].join('');
            filename += `__ref${refStr}`;
        }

        return `${CONFIG.minioBase}/${CONFIG.catalogSlug}/${CONFIG.collectionSlug}/${varSlug}/${Y}/${m}/${d}/${filename}.png`;
    }

    async function loadRasterLayer() {
        if (!deckOverlay || !currentVarSlug) return;

        const param      = parameterNames[currentVarSlug];
        const xg         = param?.['x-georiva'] || {};
        const palette    = xg.palette || [[0, [0, 0, 0]], [1, [255, 255, 255]]];
        const paletteMin = xg.palette_min ?? xg.value_min ?? 0;
        const paletteMax = xg.palette_max ?? xg.value_max ?? 1;
        const units      = param?.unit?.symbol || '';
        const url        = buildAssetUrl(currentVarSlug);

        currentPalette = { palette, paletteMin, paletteMax };
        clearRasterLayer();

        try {
            const image   = await WeatherLayers.loadTextureData(url);
            const opacity = parseInt(document.getElementById('grOpacitySlider').value, 10) / 100;
            const bbox    = edrData?.extent?.spatial?.bbox?.[0] || [-180, -90, 180, 90];

            currentRasterLayer = new WeatherLayers.RasterLayer({
                id: 'georiva-raster',
                image,
                bounds: [bbox[0], bbox[1], bbox[2], bbox[3]],
                imageSmoothing: true,
                imageInterpolation: 'LINEAR',
                imageUnscale: [paletteMin, paletteMax],
                palette,
                opacity,
                visible: true,
                pickable: true,
            });

            deckOverlay.setProps({ layers: [currentRasterLayer] });
            initTooltip(units);
            updateLegend(param, xg, paletteMin, paletteMax);
        } catch (err) {
            console.error('WeatherLayers load failed:', url, err);
            clearRasterLayer();
        }
    }

    function clearRasterLayer() {
        currentRasterLayer = null;
        if (deckOverlay) deckOverlay.setProps({ layers: [] });
    }

    // ── Boundary choropleth layer (Martin vector tiles) ───────────────────────

    function buildMartinTileUrl(level) {
        const params = new URLSearchParams({
            variable:    currentVarSlug,
            time:        CONFIG.itemTime,
            admin_level: level,
        });
        if (CONFIG.referenceTime) params.set('reference_time', CONFIG.referenceTime);
        return `${CONFIG.martinBase}/boundary_stats/{z}/{x}/{y}?${params.toString()}`;
    }

    function buildChoroplethColorExpression(palette, paletteMin, paletteMax) {
        // Build a MapLibre interpolate expression from the WeatherLayers palette
        // [[value, [r,g,b]], ...] → ['interpolate', ['linear'], ['get', 'mean'], val, 'rgb(...)', ...]
        if (!palette?.length) {
            return ['interpolate', ['linear'], ['get', 'mean'], paletteMin, '#000000', paletteMax, '#ffffff'];
        }

        const range = paletteMax - paletteMin || 1;
        const stops = [];
        for (const [val, color] of palette) {
            // Palette values are in data units — pass them directly
            const rgba = color.length === 4
                ? `rgba(${color[0]},${color[1]},${color[2]},${color[3] / 255})`
                : `rgb(${color[0]},${color[1]},${color[2]})`;
            stops.push(val, rgba);
        }

        return ['interpolate', ['linear'], ['coalesce', ['get', 'mean'], paletteMin], ...stops];
    }

    async function loadBoundaryLayer() {
        if (!activeLevel) return;

        removeBoundaryLayer();

        const tileUrl = buildMartinTileUrl(activeLevel);
        const { palette, paletteMin, paletteMax } = currentPalette || { palette: null, paletteMin: 0, paletteMax: 1 };
        const colorExpr = buildChoroplethColorExpression(palette, paletteMin, paletteMax);

        map.addSource(BOUNDARY_SOURCE, {
            type: 'vector',
            tiles: [tileUrl],
            minzoom: 0,
            maxzoom: 14,
            promoteId: { boundary_stats: 'boundary_id' },
        });

        // Fill — choropleth
        map.addLayer({
            id: BOUNDARY_FILL,
            type: 'fill',
            source: BOUNDARY_SOURCE,
            'source-layer': 'boundary_stats',
            paint: {
                'fill-color': colorExpr,
                'fill-opacity': [
                    'case',
                    ['boolean', ['feature-state', 'hovered'], false], 0.85,
                    0.65,
                ],
            },
        });

        // Outline
        map.addLayer({
            id: BOUNDARY_LINE,
            type: 'line',
            source: BOUNDARY_SOURCE,
            'source-layer': 'boundary_stats',
            paint: {
                'line-color': 'rgba(255,255,255,0.4)',
                'line-width': 0.75,
            },
        });

        // Hover highlight (thicker outline)
        map.addLayer({
            id: BOUNDARY_HOVER,
            type: 'line',
            source: BOUNDARY_SOURCE,
            'source-layer': 'boundary_stats',
            paint: {
                'line-color': 'rgba(255,255,255,0.9)',
                'line-width': ['case', ['boolean', ['feature-state', 'hovered'], false], 2, 0],
            },
        });

        setupBoundaryTooltip();
    }

    function removeBoundaryLayer() {
        [BOUNDARY_HOVER, BOUNDARY_LINE, BOUNDARY_FILL].forEach(id => {
            if (map.getLayer(id)) map.removeLayer(id);
        });
        if (map.getSource(BOUNDARY_SOURCE)) map.removeSource(BOUNDARY_SOURCE);
        removeBoundaryTooltip();
    }

    // ── Boundary tooltip ──────────────────────────────────────────────────────

    let boundaryTooltipEl = null;

    function setupBoundaryTooltip() {
        if (!boundaryTooltipEl) {
            boundaryTooltipEl = document.createElement('div');
            boundaryTooltipEl.className = 'gr-boundary-tooltip';
            boundaryTooltipEl.style.display = 'none';
            document.getElementById('grMapCanvas').appendChild(boundaryTooltipEl);
        }

        map.on('mousemove', BOUNDARY_FILL, onBoundaryHover);
        map.on('mouseleave', BOUNDARY_FILL, onBoundaryLeave);
    }

    function removeBoundaryTooltip() {
        map.off('mousemove', BOUNDARY_FILL, onBoundaryHover);
        map.off('mouseleave', BOUNDARY_FILL, onBoundaryLeave);
        if (boundaryTooltipEl) boundaryTooltipEl.style.display = 'none';
        if (hoveredBoundaryId !== null && map.getSource(BOUNDARY_SOURCE)) {
            map.setFeatureState(
                { source: BOUNDARY_SOURCE, sourceLayer: 'boundary_stats', id: hoveredBoundaryId },
                { hovered: false }
            );
        }
        hoveredBoundaryId = null;
    }

    function onBoundaryHover(e) {
        if (!e.features?.length) return;
        map.getCanvas().style.cursor = 'pointer';

        const feature = e.features[0];
        const props   = feature.properties;

        // Feature state for hover highlight
        if (hoveredBoundaryId !== null) {
            try {
                map.setFeatureState(
                    { source: BOUNDARY_SOURCE, sourceLayer: 'boundary_stats', id: hoveredBoundaryId },
                    { hovered: false }
                );
            } catch (e) {}
        }
        hoveredBoundaryId = feature.id;
        if (hoveredBoundaryId != null) {
            map.setFeatureState(
                { source: BOUNDARY_SOURCE, sourceLayer: 'boundary_stats', id: hoveredBoundaryId },
                { hovered: true }
            );
        }

        // Boundary name — use the deepest non-null name available
        const name = props.name_3 || props.name_2 || props.name_1 || props.name_0 || 'Unknown';
        const param = parameterNames[currentVarSlug];
        const units = param?.unit?.symbol || '';
        const mean  = props.mean != null ? Number(props.mean).toFixed(2) : '—';

        if (boundaryTooltipEl) {
            boundaryTooltipEl.innerHTML = `
                <div class="gr-boundary-tooltip-name">${name}</div>
                <div class="gr-boundary-tooltip-stat">
                    <span class="gr-boundary-tooltip-label">Mean</span>
                    <span class="gr-boundary-tooltip-value">${mean} ${units}</span>
                </div>
            `;
            boundaryTooltipEl.style.display = 'block';
            boundaryTooltipEl.style.left = `${e.point.x + 12}px`;
            boundaryTooltipEl.style.top  = `${e.point.y - 10}px`;
        }
    }

    function onBoundaryLeave() {
        map.getCanvas().style.cursor = '';
        if (hoveredBoundaryId !== null && map.getSource(BOUNDARY_SOURCE)) {
            try {
                map.setFeatureState(
                    { source: BOUNDARY_SOURCE, sourceLayer: 'boundary_stats', id: hoveredBoundaryId },
                    { hovered: false }
                );
            } catch (e) {}
        }
        hoveredBoundaryId = null;
        if (boundaryTooltipEl) boundaryTooltipEl.style.display = 'none';
    }

    // ── WeatherLayers tooltip ─────────────────────────────────────────────────

    function initTooltip(units) {
        removeTooltip();
        tooltipControl = new WeatherLayers.TooltipControl({
            followCursor: true,
            unitFormat: { unit: units || '' },
        });
        deckgl.setProps({
            onLoad: () => {
                const canvas = deckgl.getCanvas();
                if (canvas) tooltipControl.addTo(canvas.parentElement);
            },
            onHover: event => tooltipControl.updatePickingInfo(event),
        });
        deckgl.props.onLoad();
    }

    function removeTooltip() {
        if (tooltipControl) {
            try { tooltipControl.remove(); } catch (e) {}
            tooltipControl = null;
        }
    }

    // ── Legend ────────────────────────────────────────────────────────────────

    function updateLegend(param, xg, paletteMin, paletteMax) {
        document.getElementById('grLegend').style.display = 'block';
        document.getElementById('grLegendTitle').textContent  = param?.label || currentVarSlug;
        document.getElementById('grLegendMin').textContent    = Number(paletteMin).toFixed(1);
        document.getElementById('grLegendMax').textContent    = Number(paletteMax).toFixed(1);
        document.getElementById('grLegendUnits').textContent  = param?.unit?.symbol || '';
        document.getElementById('grLegendScale').style.background = buildGradientCSS(xg.palette || [], paletteMin, paletteMax);
    }

    function buildGradientCSS(palette, paletteMin, paletteMax) {
        if (!palette?.length) return 'linear-gradient(to right, #000, #fff)';
        const range = paletteMax - paletteMin || 1;
        const stops = palette.map(([val, color]) => {
            const pct  = ((val - paletteMin) / range) * 100;
            const rgba = color.length === 4
                ? `rgba(${color[0]},${color[1]},${color[2]},${color[3] / 255})`
                : `rgb(${color[0]},${color[1]},${color[2]})`;
            return `${rgba} ${pct.toFixed(1)}%`;
        });
        return `linear-gradient(to right, ${stops.join(', ')})`;
    }

    // ── Layer mode switcher ───────────────────────────────────────────────────

    function setupLayerSwitcher() {
        document.querySelectorAll('.gr-map-layer-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const mode = btn.dataset.mode;
                if (mode === layerMode) return;
                layerMode = mode;

                document.querySelectorAll('.gr-map-layer-btn').forEach(b =>
                    b.classList.remove('is-active')
                );
                btn.classList.add('is-active');

                // Show/hide level selector
                const levelSel = document.getElementById('grLevelSelector');
                if (levelSel) {
                    levelSel.style.display = mode === 'raster' ? 'none' : 'block';
                }

                await applyLayerMode();
            });
        });
    }

    // ── Level selector ────────────────────────────────────────────────────────

    function setupLevelSelector() {
        document.querySelectorAll('.gr-map-level-pill').forEach(pill => {
            pill.addEventListener('click', async () => {
                const level = parseInt(pill.dataset.level, 10);
                if (level === activeLevel) return;
                activeLevel = level;

                document.querySelectorAll('.gr-map-level-pill').forEach(p =>
                    p.classList.remove('is-active')
                );
                pill.classList.add('is-active');

                if (layerMode !== 'raster') {
                    removeBoundaryLayer();
                    await loadBoundaryLayer();
                }
            });
        });
    }

    // ── Variable buttons ──────────────────────────────────────────────────────

    function setupVariableButtons() {
        document.querySelectorAll('.gr-map-var-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const slug = btn.dataset.variableSlug;
                if (slug === currentVarSlug) return;

                document.querySelectorAll('.gr-map-var-btn').forEach(b =>
                    b.classList.remove('gr-map-var-btn--active')
                );
                btn.classList.add('gr-map-var-btn--active');
                currentVarSlug = slug;

                const url = new URL(window.location.href);
                url.searchParams.set('variable', slug);
                history.replaceState(null, '', url.toString());

                await applyLayerMode();
            });
        });
    }

    // ── Opacity ───────────────────────────────────────────────────────────────

    function setupOpacitySlider() {
        const slider = document.getElementById('grOpacitySlider');
        slider.addEventListener('input', async () => {
            document.getElementById('grOpacityVal').textContent = `${slider.value}%`;
            if (layerMode !== 'boundaries') await loadRasterLayer();
        });
    }

    // ── Theme ─────────────────────────────────────────────────────────────────

    function updateTheme(bm) {
        const canvas = document.getElementById('grMapCanvas');
        if (canvas) canvas.classList.toggle('gr-map-canvas--light', LIGHT_BASEMAPS.has(bm));
    }

    // ── Basemap ───────────────────────────────────────────────────────────────

    function setupBasemapSelector() {
        const btn  = document.getElementById('grBasemapBtn');
        const menu = document.getElementById('grBasemapMenu');

        btn.addEventListener('click', e => {
            e.stopPropagation();
            menu.classList.toggle('gr-open');
        });

        document.addEventListener('click', () => menu.classList.remove('gr-open'));

        menu.querySelectorAll('.gr-map-basemap-option').forEach(opt => {
            opt.addEventListener('click', e => {
                e.stopPropagation();
                const bm = opt.dataset.basemap;
                if (bm === currentBasemap) { menu.classList.remove('gr-open'); return; }

                menu.querySelectorAll('.gr-map-basemap-option').forEach(o =>
                    o.classList.remove('gr-bm-active')
                );
                opt.classList.add('gr-bm-active');
                document.getElementById('grBasemapLabel').textContent = BASEMAPS[bm].name;
                currentBasemap = bm;
                menu.classList.remove('gr-open');
                updateTheme(bm);

                map.setStyle(BASEMAPS[bm].style);
                map.once('style.load', async () => {
                    if (currentRasterLayer) deckOverlay.setProps({ layers: [currentRasterLayer] });
                    // Re-add boundary layer after style reload
                    if (layerMode !== 'raster') await loadBoundaryLayer();
                });
            });
        });
    }

    // ── Loading ───────────────────────────────────────────────────────────────

    function showLoading(visible) {
        const el = document.getElementById('grMapLoading');
        if (el) el.style.display = visible ? 'flex' : 'none';
    }

    // ── Filter toggle (sidebar-filter cards in the map panel) ─────────────────

    document.querySelectorAll('[data-filter-toggle]').forEach(function (header) {
        header.addEventListener('click', function () {
            const panel = document.getElementById(header.dataset.filterToggle);
            if (panel) panel.classList.toggle('is-open');
        });
    });

})();
