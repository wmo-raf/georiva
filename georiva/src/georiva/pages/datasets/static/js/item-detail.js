'use strict';

class ItemDetailMap {

    // ── Constants ──────────────────────────────────────────────────────────

    static BASEMAPS = {
        dark: {
            name: 'Dark',
            style: {
                version: 8,
                sources: {
                    carto: {
                        type: 'raster',
                        tileSize: 256,
                        attribution: '© CARTO',
                        tiles: ['https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png']
                    }
                },
                layers: [{id: 'carto', type: 'raster', source: 'carto'}],
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
                        tiles: ['https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png']
                    }
                },
                layers: [{id: 'carto', type: 'raster', source: 'carto'}],
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
                        tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}']
                    }
                },
                layers: [{id: 'satellite', type: 'raster', source: 'satellite'}],
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
                        tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png']
                    }
                },
                layers: [{id: 'osm', type: 'raster', source: 'osm'}],
            },
        },
    };

    static BOUNDARY_SOURCE = 'gr-boundary-source';
    static BOUNDARY_FILL = 'gr-boundary-fill';
    static BOUNDARY_LINE = 'gr-boundary-line';
    static BOUNDARY_HOVER = 'gr-boundary-hover';
    static LIGHT_BASEMAPS = new Set(['light', 'osm']);

    // ── Constructor ────────────────────────────────────────────────────────

    constructor(config) {
        this.config = config;

        // Map
        this.map = null;
        this.deckOverlay = null;
        this.deckgl = null;
        this.tooltipControl = null;
        this.currentRasterLayer = null;
        this.currentBasemap = 'dark';
        this.currentVarSlug = config.activeVarSlug;
        this.layerMode = 'raster';
        this.activeLevel = config.boundaryStatsLevels?.[0] ?? null;
        this.currentPalette = null;

        // EDR / parameters
        this.edrData = null;
        this.parameterNames = {};

        // Boundary hover
        this.hoveredBoundaryId = null;
        this.boundaryTooltipEl = null;
        this.lastPickInfo = null;

        // Timeseries
        this.tsMarker = null;
        this.tsChart = null;
        this.tsPoint = null;
        this.currentPopup = null;

        // Stable references for map.on / map.off symmetry
        this._onBoundaryHover = this._handleBoundaryHover.bind(this);
        this._onBoundaryLeave = this._handleBoundaryLeave.bind(this);

        this._initMap();
        this._initFilterToggles();
    }

    // ── Map initialisation ─────────────────────────────────────────────────

    _initMap() {
        this.map = new maplibregl.Map({
            container: 'grMap',
            style: ItemDetailMap.BASEMAPS[this.currentBasemap].style,
            center: [0, 20],
            zoom: 2,
            attributionControl: false,
        });

        this.map.addControl(new maplibregl.NavigationControl({showCompass: false}), 'bottom-right');

        this.map.on('mousemove', e => {
            const el = document.getElementById('grMapCoords');
            if (el) el.textContent = `${e.lngLat.lat.toFixed(4)}, ${e.lngLat.lng.toFixed(4)}`;
        });

        this.map.on('load', () => this._onMapLoad());
    }

    async _onMapLoad() {
        this.deckOverlay = new deck.MapboxOverlay({interleaved: true, layers: []});
        this.map.addControl(this.deckOverlay);
        this.deckgl = await this._waitForDeck(() => this.deckOverlay._deck);

        this._setupBasemapSelector();
        this._setupOpacitySlider();
        this._setupVariableButtons();
        this._setupLayerSwitcher();
        this._setupLevelSelector();
        this._setupMapClickAnalysis();
        this._setupLegendToggle();
        this._updateTheme(this.currentBasemap);

        document.getElementById('grTsClose').addEventListener('click', () => this._closeTsPanel());
        window.addEventListener('resize', () => {
            if (this.tsChart) this.tsChart.resize();
        });

        await this._loadEdrMetadata();
    }

    _waitForDeck(getDeck) {
        return new Promise(resolve => {
            (function poll() {
                const d = getDeck();
                if (d && d.getCanvas()) resolve(d);
                else setTimeout(poll, 100);
            })();
        });
    }

    // ── EDR metadata ───────────────────────────────────────────────────────

    async _loadEdrMetadata() {
        try {
            const res = await fetch(this.config.edrUrl);
            if (!res.ok) throw new Error(`EDR fetch failed: ${res.status}`);
            this.edrData = await res.json();
        } catch (e) {
            console.error('Failed to load EDR metadata:', e);
            return;
        }

        this.parameterNames = this.edrData.parameter_names || {};

        const bbox = this.edrData.extent?.spatial?.bbox?.[0];
        if (bbox?.length === 4) {
            this.map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], {padding: 60, duration: 500});
        }

        await this._applyLayerMode();
    }

    // ── Layer mode orchestration ────────────────────────────────────────────

    async _applyLayerMode() {
        this._showLoading(true);

        if (this.layerMode === 'raster') {
            this._removeBoundaryLayer();
            await this._loadRasterLayer();
        } else {
            this._clearRasterLayer();
            this._removeBoundaryLayer();
            await this._loadBoundaryLayer();
        }

        this._showLoading(false);
    }

    // ── Raster layer (WeatherLayers PNG) ───────────────────────────────────

    _buildAssetUrl(varSlug) {
        const dt = new Date(this.config.itemTime);
        const Y = dt.getUTCFullYear();
        const m = String(dt.getUTCMonth() + 1).padStart(2, '0');
        const d = String(dt.getUTCDate()).padStart(2, '0');
        const HH = String(dt.getUTCHours()).padStart(2, '0');
        const MM = String(dt.getUTCMinutes()).padStart(2, '0');
        const SS = String(dt.getUTCSeconds()).padStart(2, '0');

        let filename = `${varSlug}_${HH}${MM}${SS}`;

        if (this.config.referenceTime) {
            const rt = new Date(this.config.referenceTime);
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

        return `${this.config.minioBase}/${this.config.catalogSlug}/${this.config.collectionSlug}/${varSlug}/${Y}/${m}/${d}/${filename}.png`;
    }

    async _loadRasterLayer() {
        if (!this.deckOverlay || !this.currentVarSlug) return;

        const param = this.parameterNames[this.currentVarSlug];
        const xg = param?.['x-georiva'] || {};
        const palette = xg.palette || [[0, [0, 0, 0]], [1, [255, 255, 255]]];
        const paletteMin = xg.palette_min ?? xg.value_min ?? 0;
        const paletteMax = xg.palette_max ?? xg.value_max ?? 1;
        const units = param?.unit?.symbol || '';
        const url = this._buildAssetUrl(this.currentVarSlug);

        this.currentPalette = {palette, paletteMin, paletteMax};
        this._clearRasterLayer();

        try {
            const image = await WeatherLayers.loadTextureData(url);
            const opacity = parseInt(document.getElementById('grOpacitySlider').value, 10) / 100;
            const bbox = this.edrData?.extent?.spatial?.bbox?.[0] || [-180, -90, 180, 90];

            this.currentRasterLayer = new WeatherLayers.RasterLayer({
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

            this.deckOverlay.setProps({layers: [this.currentRasterLayer]});
            this._initTooltip(units);
            this._updateLegend(param, xg, paletteMin, paletteMax);
        } catch (err) {
            console.error('WeatherLayers load failed:', url, err);
            this._clearRasterLayer();
        }
    }

    _clearRasterLayer() {
        this.currentRasterLayer = null;
        if (this.deckOverlay) this.deckOverlay.setProps({layers: []});
    }

    // ── Boundary choropleth layer (Martin vector tiles) ────────────────────

    _buildMartinTileUrl(level) {
        const params = new URLSearchParams({
            variable: this.currentVarSlug,
            time: this.config.itemTime,
            admin_level: level,
        });
        if (this.config.referenceTime) params.set('reference_time', this.config.referenceTime);
        return `${this.config.martinBase}/boundary_stats/{z}/{x}/{y}?${params.toString()}`;
    }

    _buildChoroplethColorExpression(palette, paletteMin, paletteMax) {
        if (!palette?.length) {
            return ['interpolate', ['linear'], ['get', 'mean'], paletteMin, '#000000', paletteMax, '#ffffff'];
        }
        const stops = [];
        for (const [val, color] of palette) {
            const rgba = color.length === 4
                ? `rgba(${color[0]},${color[1]},${color[2]},${color[3] / 255})`
                : `rgb(${color[0]},${color[1]},${color[2]})`;
            stops.push(val, rgba);
        }
        return ['interpolate', ['linear'], ['coalesce', ['get', 'mean'], paletteMin], ...stops];
    }

    async _loadBoundaryLayer() {
        if (!this.activeLevel) return;

        this._removeBoundaryLayer();

        const {BOUNDARY_SOURCE, BOUNDARY_FILL, BOUNDARY_LINE, BOUNDARY_HOVER} = ItemDetailMap;
        const tileUrl = this._buildMartinTileUrl(this.activeLevel);
        const {palette, paletteMin, paletteMax} = this.currentPalette || {palette: null, paletteMin: 0, paletteMax: 1};
        const colorExpr = this._buildChoroplethColorExpression(palette, paletteMin, paletteMax);

        this.map.addSource(BOUNDARY_SOURCE, {
            type: 'vector',
            tiles: [tileUrl],
            minzoom: 0,
            maxzoom: 14,
            promoteId: {boundary_stats: 'boundary_id'},
        });

        this.map.addLayer({
            id: BOUNDARY_FILL,
            type: 'fill',
            source: BOUNDARY_SOURCE,
            'source-layer': 'boundary_stats',
            paint: {
                'fill-color': colorExpr,
                'fill-opacity': ['case', ['boolean', ['feature-state', 'hovered'], false], 0.85, 0.65],
            },
        });

        this.map.addLayer({
            id: BOUNDARY_LINE,
            type: 'line',
            source: BOUNDARY_SOURCE,
            'source-layer': 'boundary_stats',
            paint: {'line-color': 'rgba(255,255,255,0.4)', 'line-width': 0.75},
        });

        this.map.addLayer({
            id: BOUNDARY_HOVER,
            type: 'line',
            source: BOUNDARY_SOURCE,
            'source-layer': 'boundary_stats',
            paint: {
                'line-color': 'rgba(255,255,255,0.9)',
                'line-width': ['case', ['boolean', ['feature-state', 'hovered'], false], 2, 0],
            },
        });

        this._setupBoundaryTooltip();
    }

    _removeBoundaryLayer() {
        const {BOUNDARY_SOURCE, BOUNDARY_FILL, BOUNDARY_LINE, BOUNDARY_HOVER} = ItemDetailMap;
        [BOUNDARY_HOVER, BOUNDARY_LINE, BOUNDARY_FILL].forEach(id => {
            if (this.map.getLayer(id)) this.map.removeLayer(id);
        });
        if (this.map.getSource(BOUNDARY_SOURCE)) this.map.removeSource(BOUNDARY_SOURCE);
        this._removeBoundaryTooltip();
    }

    // ── Boundary hover tooltip ─────────────────────────────────────────────

    _setupBoundaryTooltip() {
        if (!this.boundaryTooltipEl) {
            this.boundaryTooltipEl = document.createElement('div');
            this.boundaryTooltipEl.className = 'gr-boundary-tooltip';
            this.boundaryTooltipEl.style.display = 'none';
            document.getElementById('grMapCanvas').appendChild(this.boundaryTooltipEl);
        }
        this.map.on('mousemove', ItemDetailMap.BOUNDARY_FILL, this._onBoundaryHover);
        this.map.on('mouseleave', ItemDetailMap.BOUNDARY_FILL, this._onBoundaryLeave);
    }

    _removeBoundaryTooltip() {
        this.map.off('mousemove', ItemDetailMap.BOUNDARY_FILL, this._onBoundaryHover);
        this.map.off('mouseleave', ItemDetailMap.BOUNDARY_FILL, this._onBoundaryLeave);
        if (this.boundaryTooltipEl) this.boundaryTooltipEl.style.display = 'none';
        if (this.hoveredBoundaryId !== null && this.map.getSource(ItemDetailMap.BOUNDARY_SOURCE)) {
            this.map.setFeatureState(
                {source: ItemDetailMap.BOUNDARY_SOURCE, sourceLayer: 'boundary_stats', id: this.hoveredBoundaryId},
                {hovered: false}
            );
        }
        this.hoveredBoundaryId = null;
    }

    _handleBoundaryHover(e) {
        if (!e.features?.length) return;
        this.map.getCanvas().style.cursor = 'pointer';

        const feature = e.features[0];
        const props = feature.properties;

        if (this.hoveredBoundaryId !== null) {
            try {
                this.map.setFeatureState(
                    {source: ItemDetailMap.BOUNDARY_SOURCE, sourceLayer: 'boundary_stats', id: this.hoveredBoundaryId},
                    {hovered: false}
                );
            } catch (_) {
            }
        }
        this.hoveredBoundaryId = feature.id;
        if (this.hoveredBoundaryId != null) {
            this.map.setFeatureState(
                {source: ItemDetailMap.BOUNDARY_SOURCE, sourceLayer: 'boundary_stats', id: this.hoveredBoundaryId},
                {hovered: true}
            );
        }

        const name = props.name_3 || props.name_2 || props.name_1 || props.name_0 || 'Unknown';
        const param = this.parameterNames[this.currentVarSlug];
        const units = param?.unit?.symbol || '';
        const mean = props.mean != null ? Number(props.mean).toFixed(2) : '—';

        if (this.boundaryTooltipEl) {
            this.boundaryTooltipEl.innerHTML = `
                <div class="gr-boundary-tooltip-name">${name}</div>
                <div class="gr-boundary-tooltip-stat">
                    <span class="gr-boundary-tooltip-label">Mean</span>
                    <span class="gr-boundary-tooltip-value">${mean} ${units}</span>
                </div>
            `;
            this.boundaryTooltipEl.style.display = 'block';
            this.boundaryTooltipEl.style.left = `${e.point.x + 12}px`;
            this.boundaryTooltipEl.style.top = `${e.point.y - 10}px`;
        }
    }

    _handleBoundaryLeave() {
        this.map.getCanvas().style.cursor = '';
        if (this.hoveredBoundaryId !== null && this.map.getSource(ItemDetailMap.BOUNDARY_SOURCE)) {
            try {
                this.map.setFeatureState(
                    {source: ItemDetailMap.BOUNDARY_SOURCE, sourceLayer: 'boundary_stats', id: this.hoveredBoundaryId},
                    {hovered: false}
                );
            } catch (_) {
            }
        }
        this.hoveredBoundaryId = null;
        if (this.boundaryTooltipEl) this.boundaryTooltipEl.style.display = 'none';
    }

    // ── WeatherLayers raster tooltip ───────────────────────────────────────

    _initTooltip(units) {
        this._removeTooltip();
        this.tooltipControl = new WeatherLayers.TooltipControl({
            followCursor: true,
            unitFormat: {unit: units || ''},
        });
        this.deckgl.setProps({
            onLoad: () => {
                const canvas = this.deckgl.getCanvas();
                if (canvas) this.tooltipControl.addTo(canvas.parentElement);
            },
            onHover: event => {
                this.tooltipControl.updatePickingInfo(event);
                this.lastPickInfo = event;
            },
        });
        this.deckgl.props.onLoad();
    }

    _removeTooltip() {
        if (this.tooltipControl) {
            try {
                this.tooltipControl.remove();
            } catch (_) {
            }
            this.tooltipControl = null;
        }
    }

    // ── Legend ─────────────────────────────────────────────────────────────

    _updateLegend(param, xg, paletteMin, paletteMax) {
        document.getElementById('grLegend').style.display = 'block';
        document.getElementById('grLegendTitle').textContent = param?.label || this.currentVarSlug;
        document.getElementById('grLegendMin').textContent = Number(paletteMin).toFixed(1);
        document.getElementById('grLegendMax').textContent = Number(paletteMax).toFixed(1);
        document.getElementById('grLegendUnits').textContent = param?.unit?.symbol || '';
        document.getElementById('grLegendScale').style.background =
            this._buildGradientCSS(xg.palette || [], paletteMin, paletteMax);
    }

    _buildGradientCSS(palette, paletteMin, paletteMax) {
        if (!palette?.length) return 'linear-gradient(to right, #000, #fff)';
        const range = paletteMax - paletteMin || 1;
        const stops = palette.map(([val, color]) => {
            const pct = ((val - paletteMin) / range) * 100;
            const rgba = color.length === 4
                ? `rgba(${color[0]},${color[1]},${color[2]},${color[3] / 255})`
                : `rgb(${color[0]},${color[1]},${color[2]})`;
            return `${rgba} ${pct.toFixed(1)}%`;
        });
        return `linear-gradient(to right, ${stops.join(', ')})`;
    }

    _setupLegendToggle() {
        const legend = document.getElementById('grLegend');
        const header = document.getElementById('grLegendHeader');
        if (!legend || !header) return;

        if (window.matchMedia('(max-width: 768px)').matches) {
            legend.classList.add('is-collapsed');
        }

        header.addEventListener('click', () => legend.classList.toggle('is-collapsed'));
    }

    // ── Layer mode switcher ────────────────────────────────────────────────

    _setupLayerSwitcher() {
        document.querySelectorAll('.gr-map-layer-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const mode = btn.dataset.mode;
                if (mode === this.layerMode) return;
                this.layerMode = mode;

                document.querySelectorAll('.gr-map-layer-btn').forEach(b => b.classList.remove('is-active'));
                btn.classList.add('is-active');

                const levelSel = document.getElementById('grLevelSelector');
                if (levelSel) levelSel.style.display = mode === 'raster' ? 'none' : 'block';

                await this._applyLayerMode();
            });
        });
    }

    // ── Level selector ─────────────────────────────────────────────────────

    _setupLevelSelector() {
        document.querySelectorAll('.gr-map-level-pill').forEach(pill => {
            pill.addEventListener('click', async () => {
                const level = parseInt(pill.dataset.level, 10);
                if (level === this.activeLevel) return;
                this.activeLevel = level;

                document.querySelectorAll('.gr-map-level-pill').forEach(p => p.classList.remove('is-active'));
                pill.classList.add('is-active');

                if (this.layerMode !== 'raster') {
                    this._removeBoundaryLayer();
                    await this._loadBoundaryLayer();
                }
            });
        });
    }

    // ── Variable buttons ───────────────────────────────────────────────────

    _setupVariableButtons() {
        document.querySelectorAll('.gr-map-var-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const slug = btn.dataset.variableSlug;
                if (slug === this.currentVarSlug) return;

                document.querySelectorAll('.gr-map-var-btn').forEach(b => b.classList.remove('gr-map-var-btn--active'));
                btn.classList.add('gr-map-var-btn--active');
                this.currentVarSlug = slug;

                const url = new URL(window.location.href);
                url.searchParams.set('variable', slug);
                history.replaceState(null, '', url.toString());

                await this._applyLayerMode();

                if (this.tsPoint) await this._fetchAndRenderTimeseries(this.tsPoint.lat, this.tsPoint.lng);
            });
        });
    }

    // ── Opacity slider ─────────────────────────────────────────────────────

    _setupOpacitySlider() {
        const slider = document.getElementById('grOpacitySlider');
        slider.addEventListener('input', async () => {
            document.getElementById('grOpacityVal').textContent = `${slider.value}%`;
            if (this.layerMode !== 'boundaries') await this._loadRasterLayer();
        });
    }

    // ── Basemap selector ───────────────────────────────────────────────────

    _setupBasemapSelector() {
        const btn = document.getElementById('grBasemapBtn');
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
                if (bm === this.currentBasemap) {
                    menu.classList.remove('gr-open');
                    return;
                }

                menu.querySelectorAll('.gr-map-basemap-option').forEach(o => o.classList.remove('gr-bm-active'));
                opt.classList.add('gr-bm-active');
                document.getElementById('grBasemapLabel').textContent = ItemDetailMap.BASEMAPS[bm].name;
                this.currentBasemap = bm;
                menu.classList.remove('gr-open');
                this._updateTheme(bm);

                this.map.setStyle(ItemDetailMap.BASEMAPS[bm].style);
                this.map.once('style.load', async () => {
                    if (this.currentRasterLayer) this.deckOverlay.setProps({layers: [this.currentRasterLayer]});
                    if (this.layerMode !== 'raster') await this._loadBoundaryLayer();
                });
            });
        });
    }

    // ── Theme (dark / light basemap) ───────────────────────────────────────

    _updateTheme(bm) {
        const canvas = document.getElementById('grMapCanvas');
        if (canvas) canvas.classList.toggle('gr-map-canvas--light', ItemDetailMap.LIGHT_BASEMAPS.has(bm));
    }

    // ── Loading indicator ──────────────────────────────────────────────────

    _showLoading(visible) {
        const el = document.getElementById('grMapLoading');
        if (el) el.style.display = visible ? 'flex' : 'none';
    }

    // ── Sidebar filter card toggles ────────────────────────────────────────

    _initFilterToggles() {
        document.querySelectorAll('[data-filter-toggle]').forEach(header => {
            header.addEventListener('click', () => {
                const panel = document.getElementById(header.dataset.filterToggle);
                if (panel) panel.classList.toggle('is-open');
            });
        });
    }

    // ── Point timeseries analysis ──────────────────────────────────────────

    _setupMapClickAnalysis() {
        this.map.on('click', e => {
            if (this.layerMode !== 'raster') return;

            const lat = e.lngLat.lat;
            const lng = e.lngLat.lng;

            // Prefer stored hover info (desktop); fall back to pickObject (touch)
            let value = this.lastPickInfo?.object?.value ?? null;
            if (value == null) {
                const pick = this.deckgl.pickObject({x: e.point.x, y: e.point.y, radius: 2});
                value = pick?.object?.value ?? null;
            }

            const units = this.parameterNames[this.currentVarSlug]?.unit?.symbol || '';
            this._showClickPopup(lat, lng, value, units);
        });
    }

    _showClickPopup(lat, lng, value, units) {
        if (this.currentPopup) this.currentPopup.remove();

        const valStr = value != null
            ? `${Number(value).toFixed(2)}${units ? ' ' + units : ''}`
            : '—';

        const html = `
            <div class="gr-ts-popup">
                <div class="gr-ts-popup-row">
                    <span class="gr-ts-popup-label">Lat</span>
                    <span class="gr-ts-popup-val">${lat.toFixed(4)}</span>
                </div>
                <div class="gr-ts-popup-row">
                    <span class="gr-ts-popup-label">Lon</span>
                    <span class="gr-ts-popup-val">${lng.toFixed(4)}</span>
                </div>
                <div class="gr-ts-popup-row">
                    <span class="gr-ts-popup-label">Value</span>
                    <span class="gr-ts-popup-val">${valStr}</span>
                </div>
                <button class="gr-ts-popup-btn" id="grTsAnalyzeBtn" type="button">
                    <i class="bi bi-graph-up"></i> Analyze
                </button>
            </div>
        `;

        this.currentPopup = new maplibregl.Popup({
            closeButton: true,
            closeOnClick: false,
            maxWidth: '200px',
            className: 'gr-map-popup',
        })
            .setLngLat([lng, lat])
            .setHTML(html)
            .addTo(this.map);

        setTimeout(() => {
            const btn = document.getElementById('grTsAnalyzeBtn');
            if (btn) {
                btn.addEventListener('click', () => {
                    this.currentPopup.remove();
                    this.currentPopup = null;
                    this._runAnalysis(lat, lng);
                });
            }
        }, 0);
    }

    _runAnalysis(lat, lng) {
        this.tsPoint = {lat, lng};

        if (this.tsMarker) this.tsMarker.remove();
        this.tsMarker = new maplibregl.Marker({color: '#00c9b1'})
            .setLngLat([lng, lat])
            .addTo(this.map);

        this._fetchAndRenderTimeseries(lat, lng);
    }

    async _fetchAndRenderTimeseries(lat, lng) {
        const panel = document.getElementById('grTimeseriesPanel');
        const loading = document.getElementById('grTsLoading');
        const chartEl = document.getElementById('grTsChart');
        const errEl = document.getElementById('grTsError');

        panel.style.display = 'block';
        loading.style.display = 'flex';
        chartEl.style.visibility = 'hidden';
        errEl.style.display = 'none';

        const param = this.parameterNames[this.currentVarSlug];
        document.getElementById('grTsVarName').textContent = param?.label || this.currentVarSlug;
        document.getElementById('grTsCoords').textContent = `${lat.toFixed(4)}, ${lng.toFixed(4)}`;

        const variable = `${this.config.catalogSlug}/${this.config.collectionSlug}/${this.currentVarSlug}`;
        const url = new URL('/api/analysis/timeseries/point/', window.location.origin);
        url.searchParams.set('variable', variable);
        url.searchParams.set('lat', lat);
        url.searchParams.set('lon', lng);

        try {
            const res = await fetch(url.toString());
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();

            loading.style.display = 'none';
            chartEl.style.visibility = 'visible';
            this._renderTimeseriesChart(data);

            panel.scrollIntoView({behavior: 'smooth', block: 'start'});
        } catch (err) {
            console.error('Timeseries fetch failed:', err);
            loading.style.display = 'none';
            chartEl.style.visibility = 'hidden';
            errEl.style.display = 'flex';
        }
    }

    _renderTimeseriesChart(responseData) {
        const chartEl = document.getElementById('grTsChart');

        if (!this.tsChart) {
            this.tsChart = echarts.init(chartEl);
        }

        const units = responseData.units || '';
        const seriesData = (responseData.data || []).map(d => [d.time, d.value]);
        const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

        this.tsChart.setOption({
            backgroundColor: 'transparent',
            grid: {left: '72px', right: '24px', top: '36px', bottom: '48px'},
            tooltip: {
                trigger: 'axis',
                formatter: params => {
                    const p = params[0];
                    if (!p) return '';
                    const d = new Date(p.axisValue);
                    const label = `${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]} ${d.getUTCFullYear()}`;
                    const val = p.value[1] != null
                        ? `${Number(p.value[1]).toFixed(2)}${units ? ' ' + units : ''}`
                        : '—';
                    return `${label}<br/><strong>${val}</strong>`;
                },
            },
            xAxis: {
                type: 'time',
                axisLabel: {
                    fontSize: 11,
                    formatter: value => {
                        const d = new Date(value);
                        return `${MONTHS[d.getUTCMonth()]} ${d.getUTCFullYear()}`;
                    },
                },
            },
            yAxis: {
                type: 'value',
                name: units,
                nameLocation: 'end',
                nameGap: 8,
                nameTextStyle: {fontSize: 11, color: '#94a3b8'},
                axisLabel: {fontSize: 11},
            },
            series: [{
                type: 'line',
                data: seriesData,
                connectNulls: true,
                symbol: 'none',
                lineStyle: {color: '#00c9b1', width: 2},
                markLine: {
                    silent: true,
                    animation: false,
                    symbol: ['none', 'none'],
                    data: [{
                        xAxis: this.config.itemTime,
                        label: {formatter: 'Selected', position: 'insideEndTop', color: '#00c9b1', fontSize: 11},
                        lineStyle: {color: '#00c9b1', width: 2, type: 'dashed'},
                    }],
                },
            }],
        }, true);
    }

    _closeTsPanel() {
        document.getElementById('grTimeseriesPanel').style.display = 'none';
        if (this.tsMarker) {
            this.tsMarker.remove();
            this.tsMarker = null;
        }
        if (this.tsChart) {
            this.tsChart.dispose();
            this.tsChart = null;
        }
        if (this.currentPopup) {
            this.currentPopup.remove();
            this.currentPopup = null;
        }
        this.tsPoint = null;
    }
}

// ── Boot ───────────────────────────────────────────────────────────────────

new ItemDetailMap(JSON.parse(document.getElementById('grItemConfig').textContent));
