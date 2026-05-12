'use strict';

(function () {

    // ── Config ────────────────────────────────────────────────────────────────

    const CONFIG = JSON.parse(document.getElementById('grItemConfig').textContent);

    const BASEMAPS = {
        dark: {
            name: 'Dark',
            style: {
                version: 8,
                sources: {
                    carto: {
                        type: 'raster',
                        tileSize: 256,
                        attribution: '© CARTO',
                        tiles: ['https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'],
                    },
                },
                layers: [{ id: 'carto', type: 'raster', source: 'carto' }],
            },
        },
        light: {
            name: 'Light',
            style: {
                version: 8,
                sources: {
                    carto: {
                        type: 'raster',
                        tileSize: 256,
                        attribution: '© CARTO',
                        tiles: ['https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'],
                    },
                },
                layers: [{ id: 'carto', type: 'raster', source: 'carto' }],
            },
        },
        satellite: {
            name: 'Satellite',
            style: {
                version: 8,
                sources: {
                    satellite: {
                        type: 'raster',
                        tileSize: 256,
                        attribution: '© Esri',
                        tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
                    },
                },
                layers: [{ id: 'satellite', type: 'raster', source: 'satellite' }],
            },
        },
        osm: {
            name: 'OSM',
            style: {
                version: 8,
                sources: {
                    osm: {
                        type: 'raster',
                        tileSize: 256,
                        attribution: '© OpenStreetMap contributors',
                        tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
                    },
                },
                layers: [{ id: 'osm', type: 'raster', source: 'osm' }],
            },
        },
    };

    // ── State ─────────────────────────────────────────────────────────────────

    let map            = null;
    let deckOverlay    = null;
    let deckgl         = null;
    let tooltipControl = null;
    let currentRasterLayer = null;
    let currentBasemap = 'dark';
    let currentVarSlug = CONFIG.activeVarSlug;
    let edrData        = null;
    let parameterNames = {};

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
    // Load once — palette, units, bbox, value range all come from EDR.

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

        // Fit map to collection bbox
        const bbox = edrData.extent?.spatial?.bbox?.[0];
        if (bbox?.length === 4) {
            map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 60, duration: 500 });
        }

        await loadLayer();
    }

    // ── Asset URL ─────────────────────────────────────────────────────────────
    // Same path convention as the collection page.

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

    // ── WeatherLayers render ──────────────────────────────────────────────────

    async function loadLayer() {
        if (!deckOverlay || !currentVarSlug) return;

        const param    = parameterNames[currentVarSlug];
        const xg       = param?.['x-georiva'] || {};
        const palette  = xg.palette || [[0, [0, 0, 0]], [1, [255, 255, 255]]];
        const paletteMin = xg.palette_min ?? xg.value_min ?? 0;
        const paletteMax = xg.palette_max ?? xg.value_max ?? 1;
        const units    = param?.unit?.symbol || '';
        const url      = buildAssetUrl(currentVarSlug);

        showLoading(true);
        clearLayer();

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
            clearLayer();
        } finally {
            map.once('idle', () => showLoading(false));
        }
    }

    function clearLayer() {
        currentRasterLayer = null;
        if (deckOverlay) deckOverlay.setProps({ layers: [] });
    }

    // ── Tooltip ───────────────────────────────────────────────────────────────

    function initTooltip(units) {
        if (tooltipControl) {
            try { tooltipControl.remove(); } catch (e) {}
            tooltipControl = null;
        }
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

                // Update URL so back button / sharing works
                const url = new URL(window.location.href);
                url.searchParams.set('variable', slug);
                history.replaceState(null, '', url.toString());

                await loadLayer();
            });
        });
    }

    // ── Opacity ───────────────────────────────────────────────────────────────

    function setupOpacitySlider() {
        const slider = document.getElementById('grOpacitySlider');
        slider.addEventListener('input', async () => {
            document.getElementById('grOpacityVal').textContent = `${slider.value}%`;
            if (currentVarSlug) await loadLayer();
        });
    }

    // ── Theme ─────────────────────────────────────────────────────────────────
    // Light basemaps (light, osm) get light-themed overlays.
    // Dark basemaps (dark, satellite) keep the default dark overlays.

    const LIGHT_BASEMAPS = new Set(['light', 'osm']);

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
                if (bm === currentBasemap) {
                    menu.classList.remove('gr-open');
                    return;
                }
                menu.querySelectorAll('.gr-map-basemap-option').forEach(o =>
                    o.classList.remove('gr-bm-active')
                );
                opt.classList.add('gr-bm-active');
                document.getElementById('grBasemapLabel').textContent = BASEMAPS[bm].name;
                currentBasemap = bm;
                menu.classList.remove('gr-open');
                updateTheme(bm);

                map.setStyle(BASEMAPS[bm].style);
                map.once('style.load', () => {
                    if (currentRasterLayer) deckOverlay.setProps({ layers: [currentRasterLayer] });
                });
            });
        });
    }

    // ── Loading ───────────────────────────────────────────────────────────────

    function showLoading(visible) {
        const el = document.getElementById('grMapLoading');
        if (el) el.style.display = visible ? 'flex' : 'none';
    }

})();
