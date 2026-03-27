from __future__ import annotations

import json


def _route_label(route: dict) -> str:
    """Build a human-readable label for the route dropdown."""
    parts = []
    if route.get("date"):
        try:
            from datetime import datetime

            dt = datetime.strptime(route["date"], "%Y-%m-%d")
            parts.append(dt.strftime("%b %d"))
        except (ValueError, TypeError):
            parts.append(route["date"])
    wtype = route.get("workout_type")
    if wtype:
        parts.append(wtype)
    extras = []
    if route.get("distance_mi"):
        extras.append(f"{route['distance_mi']:.1f} mi")
    if route.get("duration_min"):
        extras.append(f"{int(route['duration_min'])} min")
    if extras:
        parts.append(f"({', '.join(extras)})")
    return " \u2014 ".join(parts) if parts else route["name"].replace(".gpx", "")


def render_route_map(routes: list[dict]) -> str:
    """Generate a single Leaflet map with a dropdown to switch between routes."""
    if not routes:
        return (
            '<div class="map-container">'
            '<p style="padding:20px;color:#6b7280;">No workout routes with GPS data in this period.</p>'
            "</div>"
        )

    # Build route data and metadata as JSON for JS
    route_data = {}
    route_meta = {}
    route_profiles = {}
    for route in routes:
        name = route["name"].replace(".gpx", "")
        points = [[p["lat"], p["lon"]] for p in route["points"] if p["lat"] and p["lon"]]
        if points:
            route_data[name] = points
            route_meta[name] = {
                "label": _route_label(route),
                "type": route.get("workout_type"),
                "date": route.get("date"),
                "distance": route.get("distance_mi"),
                "duration": route.get("duration_min"),
            }
            # Compute elevation gain from GPS trackpoints
            ele_vals = [p["ele"] for p in route["points"] if p.get("ele") is not None]
            if len(ele_vals) > 1:
                gain = sum(max(0, ele_vals[i] - ele_vals[i - 1]) for i in range(1, len(ele_vals)))
                route_meta[name]["ele_gain_ft"] = round(gain * 3.28084, 0)  # meters → feet
            # Build elevation/speed profile data for chart
            profile = []
            for p in route["points"]:
                if p.get("ele") is not None:
                    profile.append(
                        {
                            "ele_ft": round(p["ele"] * 3.28084, 0),
                            "speed_mph": round(p["speed"] * 2.23694, 1) if p.get("speed") else None,
                        },
                    )
            if profile:
                route_profiles[name] = profile

    if not route_data:
        return '<div class="map-container"><p style="padding:20px;color:#6b7280;">No valid GPS data.</p></div>'

    route_names = list(route_data.keys())
    routes_json = json.dumps(route_data)
    meta_json = json.dumps(route_meta)
    profiles_json = json.dumps(route_profiles)

    colors = [
        "#2196F3",
        "#4CAF50",
        "#FF5722",
        "#9C27B0",
        "#FF9800",
        "#00BCD4",
        "#E91E63",
        "#795548",
        "#607D8B",
        "#3F51B5",
        "#8BC34A",
        "#F44336",
        "#009688",
        "#673AB7",
        "#CDDC39",
        "#03A9F4",
    ]

    options_html = "".join(f'<option value="{n}">{route_meta[n]["label"]}</option>' for n in route_names)

    return f"""<div class="map-container">
    <div class="map-controls">
        <label for="routeSelect">Route:</label>
        <select id="routeSelect" onchange="showRoute(this.value)">
            <option value="__all__">Show All ({len(route_names)} routes)</option>
            {options_html}
        </select>
    </div>
    <div class="map-wrap">
        <div id="routeMap"></div>
        <div id="routeHud" class="route-hud"></div>
    </div>
    <div id="routeProfile" style="display:none;padding:8px 0;"></div>
    <script>
    var routeData = {routes_json};
    var routeMeta = {meta_json};
    var routeProfiles = {profiles_json};
    var routeColors = {json.dumps(colors)};
    var routeNames = {json.dumps(route_names)};
    var map = null;
    var currentLayers = [];
    var startMarker = null;
    var endMarker = null;

    function initRouteMap() {{
        if (map) return;
        map = L.map('routeMap', {{ zoomControl: true }});
        L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}@2x.png', {{
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
            maxZoom: 19
        }}).addTo(map);
        showRoute('__all__');
    }}

    function clearLayers() {{
        currentLayers.forEach(function(l) {{ map.removeLayer(l); }});
        currentLayers = [];
        if (startMarker) {{ map.removeLayer(startMarker); startMarker = null; }}
        if (endMarker) {{ map.removeLayer(endMarker); endMarker = null; }}
    }}

    function updateStats(name) {{
        var el = document.getElementById('routeHud');
        if (name === '__all__') {{
            el.style.display = 'none';
            return;
        }}
        var m = routeMeta[name];
        if (!m) {{ el.style.display = 'none'; return; }}
        function row(label, val) {{
            return '<div class="hud-row"><span class="hud-label">' +
                label + '</span><span class="hud-val">' + val + '</span></div>';
        }}
        var title = m.type || 'Workout';
        if (m.date) title += ' &middot; ' + m.date;
        var html = '<div class="hud-title">' + title + '</div>';
        if (m.distance) html += row('Distance', m.distance.toFixed(1) + ' mi');
        if (m.duration) html += row('Duration', Math.round(m.duration) + ' min');
        if (m.distance && m.duration && m.duration > 0) {{
            var pace = m.duration / m.distance;
            var pMin = Math.floor(pace);
            var pSec = Math.round((pace - pMin) * 60);
            html += row('Pace', pMin + ':' + (pSec < 10 ? '0' : '') + pSec + ' /mi');
        }}
        if (m.ele_gain_ft) html += row('Elevation', Math.round(m.ele_gain_ft) + ' ft');
        el.innerHTML = html;
        el.style.display = 'block';
    }}

    function showRoute(name) {{
        if (!map) return;
        clearLayers();
        updateStats(name);
        var bounds = L.latLngBounds();
        if (name === '__all__') {{
            routeNames.forEach(function(n, i) {{
                var pts = routeData[n];
                var m = routeMeta[n] || {{}};
                if (pts && pts.length > 1) {{
                    var line = L.polyline(pts, {{
                        color: routeColors[i % routeColors.length],
                        weight: 2.5,
                        opacity: 0.7
                    }}).addTo(map);
                    line.bindTooltip(m.label || n, {{ sticky: true }});
                    currentLayers.push(line);
                    pts.forEach(function(p) {{ bounds.extend(p); }});
                }}
            }});
        }} else {{
            var pts = routeData[name];
            var m = routeMeta[name] || {{}};
            if (pts && pts.length > 1) {{
                var line = L.polyline(pts, {{
                    color: '#2196F3',
                    weight: 3.5,
                    opacity: 0.85
                }}).addTo(map);
                currentLayers.push(line);
                var startInfo = '<b>Start</b>';
                if (m.type) startInfo += '<br>' + m.type;
                if (m.date) startInfo += '<br>' + m.date;
                startMarker = L.circleMarker(pts[0], {{
                    radius: 7, color: '#4CAF50', fillColor: '#4CAF50', fillOpacity: 0.9, weight: 2
                }}).addTo(map).bindPopup(startInfo);
                var endInfo = '<b>End</b>';
                if (m.distance) endInfo += '<br>' + m.distance.toFixed(1) + ' mi';
                if (m.duration) endInfo += '<br>' + Math.round(m.duration) + ' min';
                endMarker = L.circleMarker(pts[pts.length - 1], {{
                    radius: 7, color: '#FF5722', fillColor: '#FF5722', fillOpacity: 0.9, weight: 2
                }}).addTo(map).bindPopup(endInfo);
                pts.forEach(function(p) {{ bounds.extend(p); }});
            }}
        }}
        if (bounds.isValid()) {{ map.fitBounds(bounds, {{ padding: [30, 30] }}); }}

        // Elevation/speed profile chart
        var profEl = document.getElementById('routeProfile');
        if (name !== '__all__' && routeProfiles[name] && routeProfiles[name].length > 2 && typeof d3 !== 'undefined') {{
            profEl.style.display = 'block';
            profEl.innerHTML = '';
            var prof = routeProfiles[name];
            var pw = profEl.clientWidth || 600, ph = 160;
            var pm = {{top: 20, right: 50, bottom: 30, left: 50}};
            var piw = pw - pm.left - pm.right, pih = ph - pm.top - pm.bottom;
            var svg = d3.select(profEl).append('svg').attr('width', pw).attr('height', ph);
            var g = svg.append('g').attr('transform', 'translate(' + pm.left + ',' + pm.top + ')');
            var xP = d3.scaleLinear().domain([0, prof.length - 1]).range([0, piw]);
            var eleVals = prof.map(function(d) {{ return d.ele_ft; }});
            var yEle = d3.scaleLinear().domain([d3.min(eleVals) * 0.95, d3.max(eleVals) * 1.05]).range([pih, 0]).nice();
            g.append('g').attr('transform', 'translate(0,' + pih + ')').call(d3.axisBottom(xP).ticks(0).tickSizeOuter(0));
            g.append('g').call(d3.axisLeft(yEle).ticks(4).tickSizeOuter(0));
            g.append('text').attr('x', -pih / 2).attr('y', -38).attr('transform', 'rotate(-90)')
                .attr('text-anchor', 'middle').attr('fill', 'var(--muted)').attr('font-size', '10px').text('Elevation (ft)');
            var area = d3.area().x(function(d, i) {{ return xP(i); }})
                .y0(pih).y1(function(d) {{ return yEle(d.ele_ft); }}).curve(d3.curveMonotoneX);
            g.append('path').datum(prof).attr('d', area).attr('fill', '#4CAF50').attr('opacity', 0.2);
            g.append('path').datum(prof).attr('d', d3.line().x(function(d, i) {{ return xP(i); }})
                .y(function(d) {{ return yEle(d.ele_ft); }}).curve(d3.curveMonotoneX))
                .attr('fill', 'none').attr('stroke', '#4CAF50').attr('stroke-width', 1.5);
            var spdVals = prof.filter(function(d) {{ return d.speed_mph; }}).map(function(d) {{ return d.speed_mph; }});
            if (spdVals.length > 2) {{
                var ySpd = d3.scaleLinear().domain([0, d3.max(spdVals) * 1.1]).range([pih, 0]).nice();
                g.append('g').attr('transform', 'translate(' + piw + ',0)').call(d3.axisRight(ySpd).ticks(4).tickSizeOuter(0));
                g.append('text').attr('x', pih / 2).attr('y', -piw - 38).attr('transform', 'rotate(90)')
                    .attr('text-anchor', 'middle').attr('fill', 'var(--muted)').attr('font-size', '10px').text('Speed (mph)');
                g.append('path').datum(prof.filter(function(d) {{ return d.speed_mph; }}))
                    .attr('d', d3.line().defined(function(d) {{ return d.speed_mph; }})
                        .x(function(d, i) {{ return xP(i); }}).y(function(d) {{ return ySpd(d.speed_mph); }})
                        .curve(d3.curveMonotoneX))
                    .attr('fill', 'none').attr('stroke', '#FF9800').attr('stroke-width', 1.2).attr('opacity', 0.8);
            }}
            // Legend
            var lg = svg.append('g').attr('transform', 'translate(' + pm.left + ',10)');
            lg.append('rect').attr('width', 12).attr('height', 3).attr('fill', '#4CAF50').attr('y', 2);
            lg.append('text').attr('x', 16).attr('y', 8).attr('font-size', '10px').attr('fill', 'var(--muted)').text('Elevation');
            if (spdVals.length > 2) {{
                lg.append('rect').attr('x', 80).attr('width', 12).attr('height', 3).attr('fill', '#FF9800').attr('y', 2);
                lg.append('text').attr('x', 96).attr('y', 8).attr('font-size', '10px').attr('fill', 'var(--muted)').text('Speed');
            }}
            // Hover tooltip
            var hoverLine = g.append('line').attr('y1', 0).attr('y2', pih)
                .attr('stroke', 'var(--muted)').attr('stroke-width', 1).attr('stroke-dasharray', '3,3').style('display', 'none');
            var hoverDot = g.append('circle').attr('r', 4).attr('fill', '#4CAF50').style('display', 'none');
            var tip = d3.select(profEl).append('div')
                .style('position', 'absolute').style('background', 'var(--card)').style('border', '1px solid var(--border)')
                .style('border-radius', '6px').style('padding', '6px 10px').style('font-size', '11px')
                .style('pointer-events', 'none').style('display', 'none').style('box-shadow', 'var(--shadow)');
            g.append('rect').attr('width', piw).attr('height', pih).attr('fill', 'none').attr('pointer-events', 'all')
                .on('mousemove', function(event) {{
                    var mx = d3.pointer(event)[0];
                    var idx = Math.round(xP.invert(mx));
                    idx = Math.max(0, Math.min(idx, prof.length - 1));
                    var d = prof[idx];
                    hoverLine.attr('x1', xP(idx)).attr('x2', xP(idx)).style('display', null);
                    hoverDot.attr('cx', xP(idx)).attr('cy', yEle(d.ele_ft)).style('display', null);
                    var html = '<b>' + Math.round(d.ele_ft) + ' ft</b>';
                    if (d.speed_mph) html += '<br>' + d.speed_mph + ' mph';
                    tip.html(html).style('display', null)
                        .style('left', (pm.left + xP(idx) + 12) + 'px').style('top', (pm.top + yEle(d.ele_ft) - 10) + 'px');
                }})
                .on('mouseleave', function() {{
                    hoverLine.style('display', 'none');
                    hoverDot.style('display', 'none');
                    tip.style('display', 'none');
                }});
        }} else {{
            profEl.style.display = 'none';
        }}
    }}

    // Auto-init if routes tab is already visible
    if (document.getElementById('tab-routes').classList.contains('active')) {{
        initRouteMap();
        window._mapInitialized = true;
    }}
    </script>
</div>"""
