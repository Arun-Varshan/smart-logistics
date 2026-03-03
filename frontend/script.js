// --- CONFIGURATION ---
console.log("Smart Logistics ERP: Script v1.0.6 loading...");

// DEPLOYMENT: Replace the empty string below with your Render Backend URL (e.g., "https://my-app.onrender.com")
const DEPLOYED_BACKEND_URL = "https://your-backend-url-here.onrender.com"; 

// Automatically sync with the backend port 5000 on the same host for local dev
const API_URL = window.API_URL || (
    (location.hostname === "localhost" || location.hostname === "127.0.0.1")
        ? "http://127.0.0.1:5000" 
        : DEPLOYED_BACKEND_URL
);
console.log("Logistics ERP System: Targeting API at", API_URL);

// --- HELPERS ---
const qs = s => document.querySelector(s);
const qsa = s => Array.from(document.querySelectorAll(s));

let accessToken = localStorage.getItem("wiztric_access_token") || null;
let currentTenantId = localStorage.getItem("wiztric_tenant_id") || null;
let currentRole = localStorage.getItem("wiztric_role") || null;
let lastParcelCount = 0;
let lastStream = null;

// Dark theme default
const savedTheme = localStorage.getItem("wiztric_theme") || "dark";
document.documentElement.dataset.theme = savedTheme;

// --- ROLE GUARD ---
const ROLE_PATHS = {
    "HUB_MANAGER": "/manager/",
    "QOS": "/qos/",
    "ROBOTICS": "/robots/",
    "DELIVERY": "/delivery/",
    "ADMIN": "/admin/",
    "FINANCE": "/finance/"
};

function enforceRoleGuard() {
    const path = location.pathname;
    const role = localStorage.getItem("wiztric_role");
    const token = localStorage.getItem("wiztric_access_token");

    // Allow landing, login, and customer portal
    if (path === "/" || path.endsWith("index.html") || path.endsWith("login.html") || path.endsWith("customer.html")) return;

    if (!token || !role) {
        window.location.href = "/login.html";
        return;
    }

    // Safely access ROLE_PATHS
    const allowed = ROLE_PATHS[role];
    let isAllowed = false;
    if (allowed) {
        if (Array.isArray(allowed)) isAllowed = allowed.some(p => path.includes(p));
        else isAllowed = path.includes(allowed);
    }

    if (!isAllowed && role !== "ADMIN") {
        console.warn("Unauthorized access attempt by", role, "to", path);
        // Resolve redirect path based on role
        const role_paths = {
            "HUB_MANAGER": "/manager/dashboard.html",
            "QOS": "/qos/dashboard.html",
            "ROBOTICS": "/robots/dashboard.html",
            "DELIVERY": "/delivery/dashboard.html",
            "ADMIN": "/admin/dashboard.html",
            "FINANCE": "/finance/dashboard.html"
        };
        const redirect = role_paths[role] || "/login.html";
        window.location.href = redirect;
    }
}

// --- INITIAL GUARD ---
// Immediate check before any UI logic runs
enforceRoleGuard();

async function apiFetch(path, options = {}) {
    const headers = options.headers ? { ...options.headers } : {};
    if (accessToken) headers["Authorization"] = `Bearer ${accessToken}`;
    if (currentTenantId) headers["X-Tenant-Id"] = currentTenantId;
    if (currentRole) headers["X-Role"] = currentRole;

    const url = new URL(`${API_URL}${path}`, window.location.origin);
    if (currentTenantId) url.searchParams.set("tenant_id", currentTenantId);
    if (!headers["Authorization"] && accessToken) url.searchParams.set("access_token", accessToken);

    return fetch(url.toString(), { ...options, headers });
}

// --- INITIALIZATION ---
function initApp() {
    if (window._erpInitialized) return;
    window._erpInitialized = true;
    console.log("Initializing Smart Logistics Hub ERP...");
    initSystemEventBridge();

    // Initial UI Gating (Hide until data)
    // Pre-render from cached stream first to avoid flicker on page switches
    try {
        const cached = localStorage.getItem("wiztric_last_stream");
        if (cached) {
            const data = JSON.parse(cached);
            const hasData = data.parcels && data.parcels.length > 0;
            toggleVisualGating(hasData);
            handleStreamPayload(data);
        }
    } catch (e) {}
    // Then fetch fresh data in background
    apiFetch("/api/stream/simulation").then(r=>r.json()).then(data => {
        const hasData = data.parcels && data.parcels.length > 0;
        toggleVisualGating(hasData);
        // Always handle payload to show 0s/empty states correctly
        handleStreamPayload(data);
    }).catch(() => toggleVisualGating(false));
    
    // Global UI logic
    const logoutBtn = document.getElementById("logout-btn");
    if (logoutBtn) {
        logoutBtn.onclick = () => { 
            console.log("Logging out...");
            localStorage.clear(); 
            sessionStorage.clear();
            window.location.replace("/index.html"); 
        };
    }

    // New features initialization
    if (document.getElementById("godownMap")) renderGodownMap();
    if (document.getElementById("notificationsList")) {
        fetchNotifications();
        setInterval(fetchNotifications, 30000);
    }
    if (document.getElementById("financeSummary")) fetchFinanceSummary();

    // Page-specific initialization
    const path = location.pathname;
    if (path.includes("/robots/")) initRoboticsPage();
    if (path.includes("/qos/")) initQosPage();
    if (path.includes("/manager/intake.html")) {
        // Any specific intake init?
    }
    if (path.includes("/manager/delivery.html")) {
        fetchVehicleFleet();
    }

    const toggle = document.getElementById("hubpulse-toggle");
    const panel = document.getElementById("hubpulse-panel");
    const messages = document.getElementById("hubpulse-messages");
    if (toggle && panel) toggle.onclick = () => panel.classList.toggle("open");

    if (messages && panel && currentRole === "HUB_MANAGER") {
        (async () => {
            try {
                const res = await apiFetch("/api/ai/advisor_greeting");
                const data = await res.json();
                messages.innerHTML = `<div class="hubpulse-msg ai">${data.greeting}</div>`;
                // panel.classList.add("open"); // Don't auto-open, keep it clean
                if (document.getElementById("plForecastChart")) updatePlanningForecastCharts(data.forecast);
            } catch (e) {}
        })();
    }

    // Triggers
    const procBtn = document.getElementById("initiateProcessingBtn");
    if (procBtn) {
        procBtn.onclick = async () => {
            const res = await apiFetch("/api/admin/initiate_processing", { method: "POST" });
            const data = await res.json();
            alert(data.message || data.error);
        };
    }
    const delBtn = document.getElementById("initiateDeliveryBtn");
    if (delBtn) {
        delBtn.onclick = async () => {
            const res = await apiFetch("/api/admin/initiate_delivery", { method: "POST" });
            const data = await res.json();
            alert(data.message || data.error);
        };
    }

    // Intake
    const uploadBtn = document.getElementById("intakeUploadBtn");
    if (uploadBtn) {
        uploadBtn.onclick = async () => {
            const fileInput = document.getElementById("intakeCsvFile");
            if (!fileInput.files[0]) return;
            const formData = new FormData();
            formData.append("file", fileInput.files[0]);
            
            const statusEl = document.getElementById("intakeUploadStatus");
            statusEl.innerHTML = `<span style="color:var(--warning)">Uploading...</span>`;
            
            try {
                const res = await apiFetch("/upload_csv", { method: "POST", body: formData });
                const data = await res.json();
                
                if (res.ok && data.details) {
                    // Update KPI Cards immediately
                    if (data.details.stats) {
                        const s = data.details.stats;
                        document.getElementById("kpi-weight").textContent = s.weight + " kg";
                        document.getElementById("kpi-volume").textContent = s.volume;
                        document.getElementById("kpi-prio").textContent = s.priority_load + "%";
                        document.getElementById("kpi-date").textContent = new Date().toLocaleDateString();
                    }

                    // Show creative success message
                    statusEl.innerHTML = `
                        <div class="panel" style="margin-top:10px; border-left:4px solid var(--success); animation: fadeIn 0.5s;">
                            <div style="color:var(--success); font-weight:700; margin-bottom:5px;">✓ BATCH RECEIVED</div>
                            <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px; font-size:12px;">
                                <div><span style="color:var(--muted)">Batch ID:</span> <br/>${data.details.batch_id}</div>
                                <div><span style="color:var(--muted)">Count:</span> <br/>${data.details.count} Parcels</div>
                                <div><span style="color:var(--muted)">Vehicle:</span> <br/>${data.details.vehicle_id}</div>
                                <div><span style="color:var(--muted)">Time:</span> <br/>${data.details.timestamp}</div>
                            </div>
                        </div>
                    `;
                    toggleVisualGating(true);
                    apiFetch("/api/stream/simulation").then(r=>r.json()).then(handleStreamPayload);
                } else {
                    statusEl.textContent = data.message || data.error;
                }
            } catch (e) {
                statusEl.textContent = "Upload failed: " + e.message;
            }
        };
    }
}

async function loadQosReports() {
    const tableBody = document.getElementById("qrTableBody");
    const emptyMsg = document.getElementById("qrEmpty");
    if (!tableBody) return;

    try {
        const res = await apiFetch("/api/qos/scans");
        const data = await res.json();
        const scans = data.scans || [];

        if (scans.length === 0) {
            tableBody.innerHTML = "";
            if (emptyMsg) emptyMsg.style.display = "block";
            return;
        }

        if (emptyMsg) emptyMsg.style.display = "none";
        tableBody.innerHTML = scans.map(s => `
            <div class="table-row">
                <div>${s.timestamp.replace("T", " ")}</div>
                <div style="font-family:monospace; font-weight:600;">${s.parcel_id}</div>
                <div>
                    <span class="badge ${s.damaged ? (s.severity === 'severe' ? 'critical-damage' : 'minor-damage') : 'received-at-hub'}">
                        ${s.damaged ? (s.severity?.toUpperCase() || 'DAMAGED') : 'SAFE'}
                    </span>
                </div>
                <div>${((s.confidence || 0) * 100).toFixed(1)}%</div>
                <div>
                    ${s.annotated_url ? `<a href="${s.annotated_url}" target="_blank" class="btn btn-secondary" style="padding:4px 8px; font-size:10px;">View Image</a>` : ''}
                </div>
            </div>
        `).join("");
    } catch (err) {
        console.error("Failed to load QOS scans:", err);
    }
}

async function uploadDamagePhotos() {
    const input = document.getElementById("qosDamagePhotos");
    const container = document.getElementById("damageAlerts");
    if (!input || !input.files.length || !container) return;

    const formData = new FormData();
    for (const f of input.files) formData.append("images", f);

    container.innerHTML = `<div style="grid-column: 1/-1; text-align:center; padding:20px;">
        <div class="spinner"></div><br/>AI model analyzing photos...
    </div>`;

    try {
        const res = await apiFetch("/predict_damage", {
            method: "POST",
            body: formData
        });
        const data = await res.json();
        const results = data.results || [];

        container.innerHTML = results.map(r => `
            <div class="panel" style="padding:16px;">
                <div style="display:flex; justify-content:space-between; align-items:start; margin-bottom:12px;">
                    <div>
                        <div style="font-weight:700; font-size:14px;">${r.parcel_id}</div>
                        <div style="font-size:11px; color:var(--muted);">${r.timestamp}</div>
                    </div>
                    <span class="badge ${r.damaged ? (r.severity === 'severe' ? 'critical-damage' : 'minor-damage') : 'received-at-hub'}">
                        ${r.damaged ? r.severity.toUpperCase() : 'SAFE'}
                    </span>
                </div>
                ${r.annotated_url ? 
                    `<img src="${r.annotated_url}" style="width:100%; height:180px; object-fit:cover; border-radius:12px; border:1px solid rgba(255,255,255,0.1);">` : 
                    `<div style="width:100%; height:180px; background:rgba(0,0,0,0.2); border-radius:12px; display:grid; place-items:center; color:var(--muted); font-size:12px;">No image available</div>`
                }
                <div style="margin-top:12px; font-size:12px; display:flex; justify-content:space-between;">
                    <span>Confidence:</span>
                    <span style="font-weight:700; color:var(--primary);">${(r.confidence * 100).toFixed(1)}%</span>
                </div>
            </div>
        `).join("");

        // Reset input
        input.value = "";
    } catch (err) {
        console.error("QOS Upload Error:", err);
        container.innerHTML = `<div style="grid-column: 1/-1; color:var(--danger); text-align:center;">Failed to connect to AI model. Please check backend.</div>`;
    }
}

function toggleVisualGating(hasData) {
    // We removed the aggressive overlay to allow the user to see the dashboard with 0 values
    const panels = document.querySelectorAll(".panel, #twinCanvas, #deliveryMap");
    panels.forEach(p => {
        p.style.opacity = "1";
        const overlay = p.querySelector(".gated-overlay");
        if (overlay) overlay.remove();
    });
    
    const kpis = document.querySelectorAll(".kpi-card");
    kpis.forEach(k => {
        k.style.opacity = "1";
        k.style.filter = "none";
    });
}

// --- CHARTING DEFAULTS ---
if (window.Chart) {
    Chart.defaults.color = 'rgba(255, 255, 255, 0.7)';
    Chart.defaults.borderColor = 'rgba(255, 255, 255, 0.1)';
    Chart.defaults.elements.line.tension = 0.4;
    Chart.defaults.plugins.legend.labels.color = 'rgba(255, 255, 255, 0.8)';
    Chart.defaults.scales.linear.grid = { color: 'rgba(255, 255, 255, 0.05)' };
    Chart.defaults.scales.category.grid = { color: 'rgba(255, 255, 255, 0.05)' };
}

// --- DATA STREAM ---
let _eventSource = null;

function handleStreamPayload(data) {
    lastStream = data;
    try { localStorage.setItem("wiztric_last_stream", JSON.stringify(data)); } catch (e) {}
    const parcels = data.parcels || [];
    const hasData = parcels.length > 0;
    
    // Update DB Status Tag if present
    const dbTag = document.getElementById("db-status-tag");
    if (dbTag && data.db_type) {
        dbTag.textContent = data.db_type.toUpperCase() + " ACTIVE";
        dbTag.style.color = data.db_type === "postgres" ? "var(--success)" : "var(--warning)";
        dbTag.style.borderColor = data.db_type === "postgres" ? "var(--success)" : "var(--warning)";
    }

    // Dispatch custom event for page-specific listeners
    window.dispatchEvent(new CustomEvent('wiztric-stream-update', { detail: data }));

    toggleVisualGating(hasData);
    
    // Update Flow State (Hub Dashboard)
    if (data.flow_state) {
        Object.entries(data.flow_state).forEach(([k, v]) => {
            const el = document.getElementById(`flow-${k}`);
            if (el) el.textContent = v;
        });
    }

    // Update KPIs across dashboards
    const adParcels = document.getElementById("adParcels");
    if (adParcels) adParcels.textContent = parcels.length;

    const intakeBody = document.getElementById("intakeStreamBody");
    if (intakeBody) {
        if (parcels.length === 0) {
            intakeBody.innerHTML = `<div style="padding:40px; color:var(--muted); text-align:center;">No manifest data for today.</div>`;
        } else {
            intakeBody.innerHTML = parcels.slice(0, 20).map(p => `
                <div class="table-row">
                    <div>${p.id}</div>
                    <div>${p.zone}</div>
                    <div>${p.priority}</div>
                    <div>${p.destination_city}</div>
                    <div style="font-weight:700;">₹${p.amount_to_pay || 0}</div>
                    <div>${p.godown || '—'}</div>
                    <div class="badge received-at-hub">${p.status}</div>
                </div>
            `).join("");
        }
    }

    // Digital Twin Update
    if (document.getElementById("twinCanvas")) {
        // Ensure stream data is passed to twin state
        if (data.twin && data.twin.robots) {
            applyTwinStateFromStream(data.twin, parcels, data.robot_utilization);
        } else {
            // Fallback if twin data is missing in payload but we have parcels
            console.warn("Twin data missing in stream payload, using fallback.");
        }
    }
    
    // Delivery Map Update
    const mapEl = document.getElementById("deliveryMap");
    if (mapEl) {
        if (window.renderDeliveryNetwork) {
            window.renderDeliveryNetwork(data.delivery_fleet || {});
        }
    }

    updateDeptViews();
    fetchVehicleFleet();
}

async function fetchVehicleFleet() {
    const container = document.getElementById("vehicleFleetBody");
    const maintContainer = document.getElementById("maintenanceBody");
    if (!container && !maintContainer) return;
    try {
        const res = await apiFetch("/api/transport/vehicles");
        const vehicles = await res.json();
        
        if (container) {
            if (vehicles.length === 0) {
                container.innerHTML = `<div style="padding:40px; color:var(--muted); text-align:center;">No vehicles registered in fleet.</div>`;
            } else {
                container.innerHTML = vehicles.map(v => `
                    <div class="table-row">
                        <div style="font-weight:700; color:var(--primary);">${v.vehicle_id}</div>
                        <div>${v.vehicle_number}</div>
                        <div>${v.driver_name}</div>
                        <div><span class="badge ${v.current_status === 'IDLE' ? 'received-at-hub' : 'out-for-delivery'}">${v.current_status}</span></div>
                        <div style="font-size:11px; color:var(--muted);">${v.last_active ? v.last_active.split('T')[0] : 'Never'}</div>
                    </div>
                `).join("");
            }
        }

        if (maintContainer) {
            maintContainer.innerHTML = vehicles.map(v => `
                <div class="table-row" style="font-size: 13px;">
                    <div style="font-weight:600;">${v.vehicle_number}</div>
                    <div>
                        <div style="font-size:10px; color:var(--muted);">Fuel</div>
                        <div style="height:4px; background:rgba(255,255,255,0.1); border-radius:2px; width:60px; margin-top:4px;">
                            <div style="height:100%; background:${v.fuel_level < 30 ? 'var(--danger)' : 'var(--success)'}; width:${v.fuel_level}%; border-radius:2px;"></div>
                        </div>
                    </div>
                    <div>
                        <span style="color:${v.maintenance_status === 'OK' ? 'var(--success)' : 'var(--warning)'}">${v.maintenance_status}</span>
                    </div>
                    <div style="font-size:11px; color:var(--muted);">Svc: ${v.last_service_date.split('T')[0]}</div>
                </div>
            `).join("");
        }
    } catch (e) { console.error(e); }
}

function startSimulationStream() {
    if (!accessToken) return;
    if (window._simStreamActive) return;
    window._simStreamActive = true;
    
    if (typeof _eventSource !== 'undefined' && _eventSource) _eventSource.close();
    
    const url = new URL(`${API_URL}/api/stream/simulation/sse`, window.location.origin);
    url.searchParams.set("access_token", accessToken);
    
    _eventSource = new EventSource(url.toString());
    _eventSource.onmessage = (e) => {
        try {
            const data = JSON.parse(e.data);
            handleStreamPayload(data);
        } catch (err) {
            console.error("SSE Parse Error:", err);
        }
    };
    _eventSource.onerror = () => { 
        console.warn("SSE Connection lost. Reconnecting...");
        window._simStreamActive = false;
        _eventSource.close(); 
        setTimeout(startSimulationStream, 5000); 
    };
}

// --- CHARTING ---
const _chartInstances = {};

// Subscribe to system events for real-time alerts
function initSystemEventBridge() {
    // Prevent multiple connections
    if (window._eventBridgeActive) return;
    window._eventBridgeActive = true;

    const url = new URL(`${API_URL}/api/stream/events`, window.location.origin);
    const ev = new EventSource(url.toString());
    ev.onmessage = (e) => {
        try {
            const payload = JSON.parse(e.data);
            if (payload.type === "PARCEL_BATCH_RECEIVED") {
                // Trigger global refresh
                apiFetch("/api/stream/simulation").then(r=>r.json()).then(handleStreamPayload);
                alert(`ERP SYSTEM ALERT: ${payload.data.message}`);
            } else if (payload.type === "FINANCE_PAYMENT") {
                fetchFinanceSummary();
            } else if (payload.type === "TRANSPORT_LOG") {
                // Refresh fleet panel
                fetchVehicleFleet();
                // Append to delivery logs panel if present
                const logs = document.getElementById("transportLogs");
                if (logs) {
                    const d = payload.data;
                    const line = `[${new Date().toLocaleTimeString()}] DISPATCH ${d.vehicle_id} -> ${d.city}, Parcels: ${d.parcel_count}, ₹${Math.round(d.total_amount)}`;
                    const div = document.createElement("div");
                    div.textContent = line;
                    logs.prepend(div);
                }
            } else if (payload.type === "PARCEL_DELIVERED") {
                const logs = document.getElementById("transportLogs");
                if (logs) {
                    const d = payload.data;
                    const line = `[${new Date().toLocaleTimeString()}] DELIVERED ${d.parcel_id} via ${d.vehicle_id} (${d.city})`;
                    const div = document.createElement("div");
                    div.textContent = line;
                    logs.prepend(div);
                }
            }
        } catch (err) {}
    };
    ev.onerror = () => {
        window._eventBridgeActive = false;
        ev.close();
        setTimeout(initSystemEventBridge, 10000);
    };
}

async function fetchTransportTracking() {
    try {
        const res = await apiFetch("/api/transport/tracking");
        const data = await res.json();
        const body = document.getElementById("transportBody");
        if (!body) return;
        if (data.length === 0) {
            body.innerHTML = `<div style="padding:20px; text-align:center; color:var(--muted);">No active transports.</div>`;
            return;
        }
        body.innerHTML = data.map(t => `
            <div class="table-row">
                <div>${t.vehicle_id}</div>
                <div>${t.destination}</div>
                <div>${t.parcel_count}</div>
                <div style="font-weight:700;">₹${Math.round(t.total_amount || 0).toLocaleString()}</div>
                <div><span class="badge ${t.status === 'DELIVERED' ? 'received-at-hub' : 'out-for-delivery'}">${t.status}</span></div>
            </div>
        `).join("");
    } catch (e) { console.error(e); }
}

function updatePlanningForecastCharts(data) {
    if (!window.Chart || !data) return;
    
    const forecast = data.forecast || data; // Handle both direct forecast and summary object
    if (forecast.total <= 0 && !data.history) return;

    const fcEl = document.getElementById("plForecastChart");
    if (fcEl && forecast.total > 0) {
        if (_chartInstances["plForecast"]) _chartInstances["plForecast"].destroy();
        _chartInstances["plForecast"] = new Chart(fcEl.getContext("2d"), {
            type: "bar",
            data: { labels: ["Forecasted Total"], datasets: [{ label: "Parcels", data: [forecast.total], backgroundColor: "#2563EB" }] },
            options: { responsive: true, maintainAspectRatio: false }
        });
    }

    const trEl = document.getElementById("plTrendChart");
    if (trEl) {
        let labels = [];
        let trendData = [];
        
        if (data.history && data.history.length > 0) {
            // Use real historical data
            const sortedHistory = [...data.history].reverse();
            labels = sortedHistory.map(b => b.created_at.split('T')[0]);
            trendData = sortedHistory.map(b => b.total_parcels);
            
            // Add prediction points
            const lastVal = trendData[trendData.length - 1];
            labels.push("Tomorrow (Pred)");
            trendData.push(forecast.tomorrow_volume || forecast.total);
            
            // Add 3 more days of linear trend
            for (let i = 2; i <= 4; i++) {
                labels.push(`Day ${i+1} (Pred)`);
                const diff = (forecast.tomorrow_volume || forecast.total) - trendData[trendData.length - 2];
                trendData.push(Math.round(trendData[trendData.length - 1] + diff * 0.5));
            }
        } else {
            // Fallback to synthetic trend if no history
            labels = ["Today", "Tomorrow", "Day 3", "Day 4", "Day 5"];
            const base = forecast.tomorrow_volume || forecast.total || 1000;
            trendData = [
                Math.round(base * 0.9),
                base,
                Math.round(base * 1.1),
                Math.round(base * 1.05),
                Math.round(base * 1.2)
            ];
        }

        if (_chartInstances["plTrend"]) _chartInstances["plTrend"].destroy();
        _chartInstances["plTrend"] = new Chart(trEl.getContext("2d"), {
            type: "line",
            data: {
                labels: labels,
                datasets: [{
                    label: "Parcel Volume Trend",
                    data: trendData,
                    borderColor: "#3b82f6",
                    backgroundColor: "rgba(59, 130, 246, 0.1)",
                    fill: true,
                    tension: 0.4,
                    pointRadius: 5,
                    pointBackgroundColor: "#3b82f6"
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: { beginAtZero: false, grid: { color: "rgba(255,255,255,0.05)" } },
                    x: { grid: { display: false } }
                }
            }
        });
    }

    const zdEl = document.getElementById("plZoneDistChart");
    if (zdEl && forecast.distribution) {
        if (_chartInstances["plZoneDist"]) _chartInstances["plZoneDist"].destroy();
        _chartInstances["plZoneDist"] = new Chart(zdEl.getContext("2d"), {
            type: "pie",
            data: { labels: Object.keys(forecast.distribution), datasets: [{ data: Object.values(forecast.distribution), backgroundColor: ["#ff6b6b", "#33d17a", "#f5a623", "#a9b0c7", "#5b8cff", "#4de1c9"] }] },
            options: { responsive: true, maintainAspectRatio: false }
        });
    }
}

function initRoboticsPage() {
    apiFetch("/api/robotics/summary").then(r=>r.json()).then(s=>{
        const setText = (id,val)=>{ const el=document.getElementById(id); if(el) el.textContent=val; };
        setText("rbActive", s.active||0);
        setText("rbIdle", s.idle||0);
        setText("rbUtilPct", (s.utilization||0) + "%");
        setText("rbMaint", s.maintenance_forecast||0);
        if (s.co2_saved) setText("rbCO2", s.co2_saved + "kg");
    });
}

function initQosPage() {
    apiFetch("/api/qos/summary").then(r=>r.json()).then(s=>{
        const setText = (id,val)=>{ const el=document.getElementById(id); if(el) el.textContent=val; };
        setText("qosDamageRatio", (s.damaged_ratio||0) + "%");
        setText("qosSafeCount", s.safe_count||0);
        setText("qosCriticalCount", s.critical_count||0);
    });
}

function updateDeptViews() {
    // If no stream data yet, show loading/empty states
    const parcels = (lastStream && lastStream.parcels) || [];
    const util = (lastStream && lastStream.robot_utilization) || {};
    const noData = parcels.length === 0;
    const intakeBody = document.getElementById("intakeStreamBody");
    if (intakeBody && noData) {
        intakeBody.innerHTML = `<div style="padding:40px; color:var(--muted); text-align:center;">No manifest data for today. Please upload a CSV at the Receiving Station.</div>`;
    }

    // Hub Manager Dashboard
    const adParcels = document.getElementById("adParcels");
    if (adParcels) {
        adParcels.textContent = parcels.length;
        const adForecast = document.getElementById("adForecast");
        if (adForecast) {
            apiFetch("/api/planning/summary").then(r=>r.json()).then(f => {
                const fc = f.tomorrow_volume || 0;
                adForecast.textContent = fc;
                const adVariance = document.getElementById("adVariance");
                if (adVariance && fc > 0) {
                    const varPct = Math.round(((parcels.length - fc) / fc) * 100);
                    adVariance.textContent = (varPct >= 0 ? "+" : "") + varPct + "%";
                    adVariance.style.color = Math.abs(varPct) > 15 ? "var(--danger)" : "var(--success)";
                }
            });
        }
        
        // Flow indicators
        ["RECEIVED", "INSPECTION", "ZONE_ALLOCATION", "IN_TRANSIT", "DELIVERED"].forEach(k => {
            const el = document.getElementById(`flow-${k}`);
            if (el) {
                const val = (lastStream && lastStream.flow_state) ? (lastStream.flow_state[k] || "0") : "0";
                // Animation effect for incrementing numbers
                if (el.textContent !== val.toString()) {
                    el.style.color = "var(--primary)";
                    setTimeout(() => el.style.color = "var(--text)", 500);
                }
                el.textContent = val;
            }
        });

        // Recent Activity Table
        const adTable = document.getElementById("adActivityTable");
        if (adTable) {
            if (parcels.length > 0) {
                adTable.innerHTML = parcels.slice(0, 10).map(p => `
                    <div class="table-row">
                        <div>${p.id}</div><div>${p.zone}</div><div><span class="badge ${p.status.toLowerCase().replace(/_/g,'-')}">${p.status}</span></div><div>${p.created_at.split('T')[1].split('.')[0]}</div>
                    </div>
                `).join("");
            } else {
                adTable.innerHTML = `<div style="color:var(--muted); text-align:center; padding:40px;">No parcel activity found. Upload a manifest to begin.</div>`;
            }
        }
    }

    // QOS Dashboard
    const qosAwaiting = document.getElementById("qosAwaitingBody");
    if (qosAwaiting) {
        const list = parcels.filter(p => p.status === "AWAITING_INSPECTION");
        qosAwaiting.innerHTML = list.length ? list.slice(0, 10).map(p => `
            <div class="table-row">
                <div>${p.id}</div><div>${p.priority}</div><div>${p.status}</div>
            </div>
        `).join("") : `<div style="padding:20px; color:var(--muted); text-align:center;">${noData ? "Waiting for manifest upload..." : "All inspections complete."}</div>`;
    }

    // Robotics Dashboard
    const rbTable = document.getElementById("rbActiveTable");
    if (rbTable) {
        const robots = lastStream.robots || [];
        rbTable.innerHTML = robots.length ? robots.slice(0, 10).map(r => `
            <div class="table-row">
                <div>${r.id}</div><div>${r.status}</div><div>${r.current_zone || "Dock"}</div>
            </div>
        `).join("") : `<div style="padding:20px; color:var(--muted); text-align:center;">Robotics system standby.</div>`;
    }

    // Delivery Dashboard
    const dlTransit = document.getElementById("dlTransit");
    if (dlTransit) {
        apiFetch("/api/delivery/summary").then(r=>r.json()).then(s=>{
            dlTransit.textContent = s.total_in_transit || 0;
            const ready = document.getElementById("dlReady"); if (ready) ready.textContent = s.total_ready || 0;
            const del = document.getElementById("dlDelivered"); if (del) del.textContent = s.total_delivered || 0;
            
            const dlSummary = document.getElementById("dlSummary");
            if (dlSummary) {
                if (noData) {
                    dlSummary.innerHTML = `<div style="padding:20px; color:var(--muted); text-align:center;">Waiting for daily manifest...</div>`;
                } else {
                    dlSummary.innerHTML = `<div style="padding:10px;">
                        <div style="color:var(--success); font-weight:700;">Operational Status: ACTIVE</div>
                        <div style="margin-top:10px; font-size:13px; color:var(--muted);">
                            Fleet is currently dispatching ${s.total_in_transit} parcels across the region.
                        </div>
                    </div>`;
                }
            }
        });
    }

    // Render Dynamic Charts
    renderDashboardCharts(parcels, util);
    
    // QOS Analytics Page Specific
    if (document.getElementById("qosTrendChart")) {
        renderQosAnalyticsCharts(parcels);
    }
    
    // Hub Manager Chart Specific
    if (document.getElementById("adGodownChart")) {
        renderGodownChart(parcels);
    }
}

function renderGodownChart(parcels) {
    if (!window.Chart || parcels.length === 0) return;
    const el = document.getElementById("adGodownChart");
    if (!el) return;
    
    const zones = {};
    parcels.forEach(p => { zones[p.zone] = (zones[p.zone] || 0) + 1; });
    
    if (_chartInstances["adGodown"]) _chartInstances["adGodown"].destroy();
    _chartInstances["adGodown"] = new Chart(el.getContext("2d"), {
        type: 'bar',
        data: {
            labels: Object.keys(zones),
            datasets: [{ label: 'Zone Occupancy', data: Object.values(zones), backgroundColor: '#3b82f6', borderRadius: 4 }]
        },
        options: { 
            responsive: true, 
            maintainAspectRatio: false, 
            animation: false,
            scales: {
                x: { grid: { display: false } },
                y: { grid: { display: false } }
            }
        }
    });
}

function renderQosAnalyticsCharts(parcels) {
    if (!window.Chart || parcels.length === 0) return;
    
    const render = (id, type, labels, data, colors) => {
        const el = document.getElementById(id);
        if (!el) return;
        if (_chartInstances[id]) _chartInstances[id].destroy();
        _chartInstances[id] = new Chart(el.getContext("2d"), {
            type,
            data: { labels, datasets: [{ data, backgroundColor: colors }] },
            options: { responsive: true, maintainAspectRatio: false, animation: false }
        });
    };

    // Zone-wise Quality
    const zones = {};
    parcels.forEach(p => { zones[p.zone] = (zones[p.zone] || 0) + (p.damage_flag || 0); });
    render("qosZoneChart", "bar", Object.keys(zones), Object.values(zones), "#3b82f6");

    // Priority Impact
    const prios = {};
    parcels.forEach(p => { prios[p.priority] = (prios[p.priority] || 0) + (p.damage_flag || 0); });
    render("qosPriorityChart", "pie", Object.keys(prios), Object.values(prios), ["#ff6b6b", "#f5a623", "#4de1c9"]);

    // Dummy trends for visualization
    render("qosTrendChart", "line", ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], [5, 8, 3, 12, 4, 6, 2], "#22c55e");
    render("qosRobotChart", "doughnut", ["R-1", "R-2", "R-3", "Others"], [2, 1, 4, 8], ["#3b82f6", "#2dd4bf", "#f59e0b", "#94a3b8"]);
}

function renderDashboardCharts(parcels, util) {
    if (!window.Chart) return;

    // 1. Robotics Pie Chart
    const rbPieEl = document.getElementById("robotPie");
    if (rbPieEl) {
        const ctx = rbPieEl.getContext("2d");
        if (_chartInstances["rbPie"]) _chartInstances["rbPie"].destroy();
        _chartInstances["rbPie"] = new Chart(ctx, {
            type: 'pie',
            data: {
                labels: ['Active', 'Idle'],
                datasets: [{
                    data: [util.active || 0, util.idle || 0],
                    backgroundColor: ['#4de1c9', '#7aa2ff']
                }]
            },
            options: { responsive: true, maintainAspectRatio: false, animation: false, plugins: { legend: { position: 'bottom' } } }
        });
    }

    // 2. QOS Severity Bar
    const qosBarEl = document.getElementById("qosSeverityBar");
    if (qosBarEl && parcels.length > 0) {
        const ctx = qosBarEl.getContext("2d");
        const dmg = parcels.filter(p => p.damage_flag === 1 || p.quality_status?.includes("DAMAGE"));
        const counts = { MINOR: 0, CRITICAL: 0, SAFE: parcels.length - dmg.length };
        dmg.forEach(p => {
            const status = p.quality_status || "MINOR";
            if (status.includes("CRITICAL")) counts.CRITICAL++;
            else counts.MINOR++;
        });

        if (_chartInstances["qosBar"]) _chartInstances["qosBar"].destroy();
        _chartInstances["qosBar"] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['Safe', 'Minor', 'Critical'],
                datasets: [{
                    label: 'Parcels',
                    data: [counts.SAFE, counts.MINOR, counts.CRITICAL],
                    backgroundColor: ['#33d17a', '#f5a623', '#ff6b6b'],
                    borderRadius: 4
                }]
            },
            options: { 
                responsive: true, 
                maintainAspectRatio: false, 
                animation: false, 
                plugins: { legend: { display: false } },
                scales: {
                    x: { grid: { display: false } },
                    y: { grid: { display: false } }
                }
            }
        });
    }

    // 3. Delivery City Distribution
    const dlCityEl = document.getElementById("lgGodownChart");
    if (dlCityEl && parcels.length > 0) {
        const ctx = dlCityEl.getContext("2d");
        const cities = {};
        parcels.forEach(p => {
            const c = p.destination_city || "Unknown";
            cities[c] = (cities[c] || 0) + 1;
        });

        if (_chartInstances["dlCity"]) _chartInstances["dlCity"].destroy();
        _chartInstances["dlCity"] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: Object.keys(cities),
                datasets: [{
                    label: 'Parcels',
                    data: Object.values(cities),
                    backgroundColor: '#3b82f6',
                    borderRadius: 4
                }]
            },
            options: { 
                indexAxis: 'y',
                responsive: true, 
                maintainAspectRatio: false, 
                animation: false,
                scales: {
                    x: { grid: { display: false } },
                    y: { grid: { display: false } }
                }
            }
        });
    }
}

// --- DIGITAL TWIN SIMULATION ---
const _twinState = {
    robots: {}, // robotId -> {x, y, targetX, targetY, ...}
    zones: {},
    dock: null,
    backlog: {},
    parcels: [],
    lastUpdate: 0
};

function applyTwinStateFromStream(twin, parcels, util) {
    if (!twin) return;
    
    _twinState.zones = twin.zones || {};
    _twinState.dock = twin.dock || null;
    _twinState.backlog = twin.backlog || {};
    _twinState.parcels = parcels || [];
    _twinState.lastUpdate = Date.now();

    // Sync robots
    if (twin.robots) {
        twin.robots.forEach(r => {
            if (!_twinState.robots[r.id]) {
                // New robot
                _twinState.robots[r.id] = { ...r, targetX: r.x, targetY: r.y };
            } else {
                // Update existing robot
                const entry = _twinState.robots[r.id];
                entry.targetX = r.x;
                entry.targetY = r.y;
                entry.status = r.status;
                entry.parcel_id = r.parcel_id;
                entry.color = r.color;
                entry.path = r.path;
            }
        });
        
        // Remove old robots
        const currentIds = twin.robots.map(r => r.id);
        Object.keys(_twinState.robots).forEach(id => {
            if (!currentIds.includes(id)) delete _twinState.robots[id];
        });
    }

    // Start the render loop if not running
    if (!window._twinLoopActive) {
        window._twinLoopActive = true;
        requestAnimationFrame(renderTwinLoop);
    }
}

function renderTwinLoop() {
    const canvas = document.getElementById("twinCanvas");
    if (!canvas) {
        window._twinLoopActive = false;
        return;
    }
    
    const ctx = canvas.getContext("2d");
    function getTwinBounds() {
        let minX = 0, minY = 0, maxX = 800, maxY = 320;
        if (_twinState.dock) {
            const d = _twinState.dock;
            minX = Math.min(minX, d.x);
            minY = Math.min(minY, d.y);
            maxX = Math.max(maxX, d.x + d.w);
            maxY = Math.max(maxY, d.y + d.h);
        }
        Object.values(_twinState.zones || {}).forEach(z => {
            minX = Math.min(minX, z.x);
            minY = Math.min(minY, z.y);
            maxX = Math.max(maxX, z.x + z.w);
            maxY = Math.max(maxY, z.y + z.h);
        });
        return { x: minX, y: minY, w: Math.max(1, maxX - minX), h: Math.max(1, maxY - minY) };
    }
    const bounds = getTwinBounds();
    const FLOOR_W = bounds.w;
    const FLOOR_H = bounds.h;
    const ORIGIN_X = bounds.x;
    const ORIGIN_Y = bounds.y;
    
    const rect = canvas.getBoundingClientRect();
    if (canvas.width !== rect.width || canvas.height !== rect.height) {
        canvas.width = rect.width;
        canvas.height = rect.height;
    }
    const W = canvas.width;
    const H = canvas.height;
    
    const padding = 20;
    const scaleX = (W - padding * 2) / FLOOR_W;
    const scaleY = (H - padding * 2) / FLOOR_H;
    // Increased max scale for "bigger" visualization
    const _isFS = !!document.fullscreenElement;
    const scale = Math.min(scaleX, scaleY, _isFS ? 3.0 : 2.0);
    // Center and normalize origin so entire world fits in view
    const offsetX = (W - (FLOOR_W * scale)) / 2 - (ORIGIN_X * scale);
    const offsetY = (H - (FLOOR_H * scale)) / 2 - (ORIGIN_Y * scale);

    ctx.clearRect(0, 0, W, H);
    ctx.fillStyle = "#0c1324";
    ctx.fillRect(0, 0, W, H);
    
    ctx.save();
    ctx.translate(offsetX, offsetY);
    ctx.scale(scale, scale);

    // Subtle Grid
    ctx.strokeStyle = "rgba(77, 225, 201, 0.03)";
    ctx.lineWidth = 1/scale;
    for(let x=0; x<=FLOOR_W; x+=40) { ctx.beginPath(); ctx.moveTo(x,0); ctx.lineTo(x,FLOOR_H); ctx.stroke(); }
    for(let y=0; y<=FLOOR_H; y+=40) { ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(FLOOR_W,y); ctx.stroke(); }

    // Dock
    if (_twinState.dock) {
        const d = _twinState.dock;
        ctx.fillStyle = "rgba(255, 255, 255, 0.02)";
        ctx.fillRect(d.x, d.y, d.w, d.h);
        ctx.strokeStyle = "rgba(255, 255, 255, 0.1)";
        ctx.strokeRect(d.x, d.y, d.w, d.h);
        ctx.fillStyle = "rgba(255, 255, 255, 0.3)";
        ctx.font = `bold ${Math.max(8, 10/scale)}px Inter`;
        ctx.fillText("INTAKE DOCK", d.x + 5, d.y + 15);
    }

    // Zones
    Object.entries(_twinState.zones).forEach(([name, z]) => {
        ctx.strokeStyle = z.color || "#4de1c9";
        ctx.lineWidth = 2/scale;
        ctx.strokeRect(z.x, z.y, z.w, z.h);
        const grad = ctx.createLinearGradient(z.x, z.y, z.x, z.y + z.h);
        grad.addColorStop(0, (z.color || "#4de1c9") + "22");
        grad.addColorStop(1, (z.color || "#4de1c9") + "05");
        ctx.fillStyle = grad;
        ctx.fillRect(z.x, z.y, z.w, z.h);
        ctx.fillStyle = z.color || "#4de1c9";
        ctx.font = `bold ${11/scale}px Inter`;
        ctx.fillText(name.toUpperCase(), z.x + 8, z.y - 8);
        const load = _twinState.backlog[name] || 0;
        if (load > 0) {
            ctx.fillStyle = "rgba(255,255,255,0.5)";
            ctx.font = `${10/scale}px Inter`;
            ctx.fillText(`Load: ${load}`, z.x + 8, z.y + 20);
        }
    });

    // Fluid "Water-like" interpolation for robots
    const lerpFactor = 0.08; // Slower for more fluid movement
    const friction = 0.92;  // High friction for gliding effect
    
    Object.values(_twinState.robots).forEach(r => {
        // Initialize velocity if not present
        if (r.vx === undefined) { r.vx = 0; r.vy = 0; }
        
        // Target direction force
        const dx = r.targetX - r.x;
        const dy = r.targetY - r.y;
        
        // Smooth acceleration towards target
        r.vx += dx * lerpFactor;
        r.vy += dy * lerpFactor;
        
        // Apply friction/drag for water-like gliding
        r.vx *= friction;
        r.vy *= friction;
        
        // Update position
        r.x += r.vx;
        r.y += r.vy;
        
        const isMoving = Math.abs(r.vx) + Math.abs(r.vy) > 0.1;
        
        // Motion Trail (Subtle)
        if (isMoving && r.path && r.path.length > 0) {
            ctx.beginPath();
            ctx.strokeStyle = "rgba(255, 255, 255, 0.05)";
            ctx.setLineDash([5, 5]);
            ctx.moveTo(r.x, r.y);
            r.path.forEach(pt => ctx.lineTo(pt[0], pt[1]));
            ctx.stroke();
            ctx.setLineDash([]);
        }

        ctx.save();
        ctx.translate(r.x, r.y);
        
        if (isMoving) {
            ctx.shadowBlur = 15;
            ctx.shadowColor = r.color || "#4de1c9";
        }
        
        const desiredPixel = _isFS ? 40 : 32;
        const robotSize = desiredPixel / scale;
        const rx = -robotSize/2;
        const ry = -robotSize/2;
        
        // Main Body (Rounded Box)
        ctx.fillStyle = (r.status === "idle" || r.status === "charging") ? "#1e293b" : (r.color || "#4de1c9");
        ctx.beginPath();
        ctx.roundRect(rx, ry, robotSize, robotSize, 4);
        ctx.fill();
        ctx.strokeStyle = "rgba(255,255,255,0.8)";
        ctx.lineWidth = 1.5/scale;
        ctx.stroke();
        
        // Robot "Face" / Direction Indicator
        ctx.fillStyle = "#0f172a";
        ctx.fillRect(rx + robotSize*0.2, ry + 2, robotSize*0.6, robotSize*0.3);
        
        // LED Eyes
        ctx.fillStyle = isMoving ? "#22c55e" : "#ef4444";
        ctx.beginPath();
        ctx.arc(rx + robotSize*0.35, ry + robotSize*0.3, 1.5, 0, Math.PI*2);
        ctx.arc(rx + robotSize*0.65, ry + robotSize*0.3, 1.5, 0, Math.PI*2);
        ctx.fill();

        // Robot ID Label
        ctx.shadowBlur = 0;
        ctx.fillStyle = "#fff";
        ctx.font = `bold ${Math.max(6, (_isFS ? 10 : 8)/scale)}px Inter`;
        ctx.textAlign = "center";
        ctx.fillText(r.id.replace("R-",""), 0, robotSize*0.4);
        
        // Battery Bar (Subtle)
        const bw = robotSize * 0.8;
        const bh = 2;
        ctx.fillStyle = "rgba(0,0,0,0.3)";
        ctx.fillRect(-bw/2, ry - 4, bw, bh);
        ctx.fillStyle = r.battery > 30 ? "#22c55e" : "#ef4444";
        ctx.fillRect(-bw/2, ry - 4, bw * (r.battery/100), bh);

        if (r.parcel_id) {
            ctx.fillStyle = "#f5a623";
            ctx.beginPath();
            const pw = _isFS ? 14 : 10;
            const ph = _isFS ? 10 : 7;
            ctx.roundRect(-pw/2, ry - ph - 6, pw, ph, 1);
            ctx.fill();
            ctx.strokeStyle = "#fff";
            ctx.lineWidth = 0.5/scale;
            ctx.stroke();
            ctx.fillStyle = "#f5a623";
            ctx.font = `bold ${Math.max(6, (_isFS ? 9 : 7)/scale)}px Inter`;
            ctx.fillText(r.parcel_id, 0, ry - ph - 8);
        }
        ctx.restore();
    });

    ctx.restore();
    if (window._twinLoopActive) {
        requestAnimationFrame(renderTwinLoop);
    }
}

// --- GODOWN MAP ---
const GODOWN_COORDS = {
    "Delhi": { x: 400, y: 100 },
    "Mumbai": { x: 200, y: 400 },
    "Bangalore": { x: 300, y: 550 },
    "Chennai": { x: 450, y: 550 },
    "Kolkata": { x: 700, y: 250 },
    "Hyderabad": { x: 350, y: 450 },
    "Pune": { x: 210, y: 420 },
    "Ahmedabad": { x: 180, y: 300 }
};

function renderGodownMap() {
    const canvas = document.getElementById("godownMap");
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const W = canvas.width = canvas.offsetWidth;
    const H = canvas.height = canvas.offsetHeight;
    
    // Calculate godown stats from current stream data
    const stats = {};
    if (lastStream && lastStream.parcels) {
        lastStream.parcels.forEach(p => {
            const city = p.destination_city || "Unknown";
            stats[city] = (stats[city] || 0) + 1;
        });
    }

    ctx.clearRect(0, 0, W, H);
    ctx.fillStyle = "#0c1324";
    ctx.fillRect(0, 0, W, H);

    // Subtle Connections
    ctx.strokeStyle = "rgba(77, 225, 201, 0.1)";
    ctx.lineWidth = 1;
    const hub = { x: W/2, y: H/2 };
    
    Object.entries(GODOWN_COORDS).forEach(([city, pos]) => {
        const px = (pos.x / 800) * W;
        const py = (pos.y / 600) * H;
        const count = stats[city] || 0;
        
        ctx.beginPath();
        ctx.moveTo(hub.x, hub.y);
        ctx.lineTo(px, py);
        ctx.stroke();

        // Animated Pulse based on load
        const pulse = (Math.sin(Date.now() / 500) + 1) / 2;
        const size = 15 + (count * 2) + pulse * 10;
        ctx.fillStyle = `rgba(77, 225, 201, ${0.05 + (count > 0 ? 0.1 : 0)})`;
        ctx.beginPath();
        ctx.arc(px, py, size, 0, Math.PI*2);
        ctx.fill();

        // Godown Icon
        ctx.fillStyle = count > 0 ? "#4de1c9" : "#334155";
        ctx.beginPath();
        ctx.arc(px, py, 6, 0, Math.PI*2);
        ctx.fill();
        ctx.strokeStyle = "#fff";
        ctx.lineWidth = 2;
        ctx.stroke();

        ctx.fillStyle = "#fff";
        ctx.font = "bold 10px Inter";
        ctx.textAlign = "center";
        ctx.fillText(`${city.toUpperCase()} (${count})`, px, py + 25);
    });

    // Central Hub
    ctx.fillStyle = "#f5a623";
    ctx.beginPath();
    ctx.arc(hub.x, hub.y, 10, 0, Math.PI*2);
    ctx.fill();
    ctx.fillStyle = "#fff";
    ctx.fillText("CENTRAL HUB", hub.x, hub.y - 15);

    requestAnimationFrame(renderGodownMap);
 }
 
 // --- NOTIFICATIONS ---
 async function fetchNotifications() {
     const container = document.getElementById("notificationsList");
     if (!container) return;
     try {
         const res = await apiFetch("/api/notifications");
         const data = await res.json();
         if (data.length === 0) {
             container.innerHTML = `<div style="padding:20px; color:var(--muted); text-align:center;">No new notifications.</div>`;
             return;
         }
         container.innerHTML = data.map(n => `
             <div class="notification-item ${n.read_status ? '' : 'unread'}" style="padding:12px; border-bottom:1px solid rgba(255,255,255,0.05); border-left: 3px solid ${n.read_status ? 'transparent' : 'var(--primary)'}">
                 <div style="font-size:13px; font-weight:600;">${n.message}</div>
                 <div style="font-size:11px; color:var(--muted); margin-top:4px;">${n.created_at.replace("T", " ")}</div>
             </div>
         `).join("");
     } catch (e) { console.error(e); }
 }
 
 // --- FINANCE ---
 async function fetchFinanceSummary() {
     const container = document.getElementById("financeSummary");
     if (!container) return;
     try {
         const res = await apiFetch("/api/finance/summary");
         const data = await res.json();
         const total = data.reduce((acc, curr) => acc + curr.total, 0);
         container.innerHTML = `
             <div class="kpi-grid">
                 <div class="kpi-card"><div class="kpi-label">Total Revenue</div><div class="kpi-value">₹${total.toLocaleString()}</div></div>
                 ${data.map(d => `<div class="kpi-card"><div class="kpi-label">${d.payment_mode}</div><div class="kpi-value">₹${d.total.toLocaleString()}</div></div>`).join("")}
             </div>
         `;
     } catch (e) { console.error(e); }
 }
