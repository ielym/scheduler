/* ============================================================
   Sched Web 控制台 · 前端应用（无依赖，自包含）v2
   数据源：/api/overview · /api/tasks · /api/jobs · /api/attempts
           /api/events/recent · /api/metrics · /api/export · SSE /api/stream
   ============================================================ */
"use strict";

/* ---------------- 工具 ---------------- */
const $ = (sel, root) => (root || document).querySelector(sel);
const $$ = (sel, root) => Array.from((root || document).querySelectorAll(sel));

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, c =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function el(html) {
  const t = document.createElement("template");
  t.innerHTML = html.trim();
  return t.content.firstElementChild;
}

function debounce(fn, ms) {
  let t;
  return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch (e) {
    try {
      const ta = document.createElement("textarea");
      ta.value = text; ta.style.position = "fixed"; ta.style.opacity = "0";
      document.body.appendChild(ta); ta.select();
      document.execCommand("copy"); ta.remove();
      return true;
    } catch (e2) { return false; }
  }
}

function copyBtn(text, title = "复制") {
  const b = el(`<button class="copy-btn" title="${esc(title)}"><svg class="ic" viewBox="0 0 24 24"><rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V6a2 2 0 0 1 2-2h9"/></svg></button>`);
  b.onclick = async e => {
    e.stopPropagation();
    if (await copyText(text)) {
      b.classList.add("done");
      setTimeout(() => b.classList.remove("done"), 1200);
      toast("已复制到剪贴板");
    } else toast("复制失败", false);
  };
  return b;
}

/* ---------------- 格式化 ---------------- */
const fmt = {
  num(n) {
    if (n == null || isNaN(n)) return "—";
    return Number(n).toLocaleString("en-US");
  },
  pct(n, digits = 1) {
    if (n == null || isNaN(n)) return "—";
    return n.toFixed(digits) + "%";
  },
  bytes(b) {
    if (b == null || isNaN(b)) return "—";
    const u = ["B", "KB", "MB", "GB", "TB"];
    let i = 0;
    while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
    return (i === 0 ? b : b.toFixed(1)) + " " + u[i];
  },
  dur(sec) {
    if (sec == null || isNaN(sec) || sec < 0) return "—";
    if (sec < 1) return "<1秒";
    const d = Math.floor(sec / 86400), h = Math.floor(sec % 86400 / 3600),
      m = Math.floor(sec % 3600 / 60), s = Math.floor(sec % 60);
    if (d) return `${d}天${h}时`;
    if (h) return `${h}时${m}分`;
    if (m) return `${m}分${s}秒`;
    return `${s}秒`;
  },
  durShort(sec) {
    if (sec == null || isNaN(sec)) return "—";
    if (sec < 10) return sec.toFixed(1) + "秒";
    return fmt.dur(sec);
  },
  time(iso) {
    if (!iso) return "—";
    return new Date(iso).toLocaleTimeString("zh-CN", { timeZone: S.tz, hour12: false });
  },
  dateTime(iso) {
    if (!iso) return "—";
    const d = new Date(iso);
    return d.toLocaleDateString("zh-CN", { timeZone: S.tz, year: "numeric", month: "2-digit", day: "2-digit" })
      + " " + d.toLocaleTimeString("zh-CN", { timeZone: S.tz, hour12: false });
  },
  rel(iso) {
    if (!iso) return "—";
    const diff = (Date.now() - new Date(iso).getTime()) / 1000;
    if (diff < 5) return "刚刚";
    if (diff < 60) return Math.floor(diff) + " 秒前";
    if (diff < 3600) return Math.floor(diff / 60) + " 分钟前";
    if (diff < 86400) return Math.floor(diff / 3600) + " 小时前";
    return Math.floor(diff / 86400) + " 天前";
  },
};

const STATUS_LIST = ["QUEUED", "STARTING", "RUNNING", "PAUSING", "PAUSED", "BACKOFF",
  "SUCCEEDED", "FAILED", "RECOVERING", "RECOVERY_BLOCKED", "CANCELLED"];
const STATUS_CN = {
  QUEUED: "排队", STARTING: "启动中", RUNNING: "运行中", PAUSING: "暂停中",
  PAUSED: "已暂停", BACKOFF: "退避等待", SUCCEEDED: "已成功", FAILED: "已失败",
  RECOVERING: "恢复核实", RECOVERY_BLOCKED: "恢复受阻", CANCELLED: "已取消",
  RUNNING_ACTIVE: "运行中", WAITING: "等待中", FINISHED: "已完成", ATTENTION: "需处理",
};
/* 运维四分法：等待细分（排队/退避/暂停/恢复核实）全部自动恢复归"等待中"；
   RECOVERY_BLOCKED 不自愈，与 FAILED 同归"需处理"（唯一需要人工的类别） */
const STATUS_GROUPS = {
  RUNNING_ACTIVE: { cls: "st-RUNNING" },
  WAITING: { cls: "st-QUEUED" },
  FINISHED: { cls: "st-SUCCEEDED" },
  ATTENTION: { cls: "st-FAILED" },
};
const GROUP_STATES = {
  RUNNING_ACTIVE: ["STARTING", "RUNNING", "PAUSING"],
  WAITING: ["QUEUED", "BACKOFF", "PAUSED", "RECOVERING"],
  FINISHED: ["SUCCEEDED", "CANCELLED"],
  ATTENTION: ["FAILED", "RECOVERY_BLOCKED"],
};
const groupOf = st => Object.keys(GROUP_STATES).find(g => GROUP_STATES[g].includes(st));
const TERMINAL = new Set(["SUCCEEDED", "FAILED", "CANCELLED"]);
const LIVE = new Set(["STARTING", "RUNNING", "PAUSING"]);
const REQUEUEABLE = st => !LIVE.has(st) && st !== "SUCCEEDED" && st !== "QUEUED";
const STATUS_COLOR = {
  QUEUED: "var(--c-queued)", STARTING: "var(--c-starting)", RUNNING: "var(--c-running)",
  PAUSING: "var(--c-pausing)", PAUSED: "var(--c-paused)", BACKOFF: "var(--c-backoff)",
  SUCCEEDED: "var(--c-succeeded)", FAILED: "var(--c-failed)",
  RECOVERING: "var(--c-recovering)", RECOVERY_BLOCKED: "var(--c-recovery-blocked)",
  CANCELLED: "var(--c-cancelled)",
};
const badge = st => `<span class="badge s-${esc(st)}"><span class="bdot"></span>${esc(STATUS_CN[st] || st)}</span>`;
/* 大类徽章（运维口径）：行级显示四分法状态，悬停显示调度器原始状态 */
const GROUP_BADGE_CLS = { RUNNING_ACTIVE: "s-RUNNING", WAITING: "s-QUEUED", FINISHED: "s-SUCCEEDED", ATTENTION: "s-FAILED" };
const groupBadge = st => {
  const g = groupOf(st) || st;
  return `<span class="badge ${GROUP_BADGE_CLS[g] || ""}" title="调度状态：${esc(STATUS_CN[st] || st)}"><span class="bdot"></span>${esc(STATUS_CN[g] || g)}</span>`;
};
/* 尝试已终结（有完整起止）的状态：终态 + 恢复受阻 */
const ATTEMPT_ENDED = st => TERMINAL.has(st) || st === "RECOVERY_BLOCKED";
/* 时间列口径：运行中=本次开始；等待中=无（下次尝试未定，历史见执行尝试）；已终结=开始+结束 */
const jobTimes = j => {
  if (LIVE.has(j.status)) return j.started_at ? fmt.dateTime(j.started_at) : "—";
  if (ATTEMPT_ENDED(j.status))
    return `${j.started_at ? fmt.dateTime(j.started_at) : "—"}${j.finished_at ? `<br>${fmt.dateTime(j.finished_at)}` : ""}`;
  return "—";
};
const EVENT_CN = {
  started: "已启动", progress: "进展", error: "错误",
  done: "完成", ack: "控制确认",
};
const EV_KIND_CN = {
  attempt_launched: "启动执行", attempt_finalized: "执行结束",
  checkpoint_restored: "检查点恢复", recover_requeued: "重启回收重排",
  recover_confirmed: "恢复确认成功", producer_crash: "生产者崩溃",
  pause_requested: "请求暂停", pause_grace_exceeded: "暂停宽限超时",
  protect_released: "保护解除", protect_ended: "保护轮结束",
  job_requeued: "作业重排", job_cancelled: "作业取消",
  observability_changed: "观测可用性变化",
};
const EV_ICON = {
  producer_crash: "⚠", pause_requested: "⏸", pause_grace_exceeded: "⚠",
  protect_released: "🛡", protect_ended: "🛡", job_requeued: "↻",
  job_cancelled: "✕",
};

/* ---------------- 全局状态 ---------------- */
const S = {
  tz: "Asia/Shanghai",
  overview: null,
  tasksCache: [],
  history: [],            // 客户端实时采样 [{t, cpuPct, memMb, ioBps, queued, live}]
  lastResource: null,
  metrics: { range: "24h", server: [], fetchedAt: 0 },  // 服务端持久化图表历史
  eventsCache: [],        // 控制动作事件流
  view: null,
  viewTimers: [],
  fresh: {},              // 每视图数据新鲜度 {view: ts}
  jobsFilter: { status: "", task: "", q: "", sort: "recent", page: 1, limit: 50 },
  jobsSel: new Set(),     // 批量重排选择
  taskSort: { key: "priority", dir: -1 },
  attFilter: { status: "", task: "", page: 1, limit: 100 },
  detail: null,           // 打开中的作业详情 {jobId, idx, source, attState:{}, timer}
  sse: null, sseFailed: 0, polling: false,
};

const HIST_MAX = 220;

/* ---------------- 鉴权 ---------------- */
function getToken() { return localStorage.getItem("sched-token") || ""; }

let _loginResolve = null;
function showLogin() {
  const ov = $("#login-overlay");
  ov.hidden = false;
  $("#login-err").textContent = "";
  setTimeout(() => $("#login-token").focus(), 50);
  if (!_loginResolve) {
    _loginResolve = {};
    _loginResolve.promise = new Promise(r => { _loginResolve.resolve = r; });
  }
  return _loginResolve.promise;
}
$("#login-submit").onclick = async () => {
  const tk = $("#login-token").value.trim();
  if (!tk) return;
  const r = await fetch("/api/health", { headers: { Authorization: "Bearer " + tk } });
  if (r.ok) {
    localStorage.setItem("sched-token", tk);
    $("#login-overlay").hidden = true;
    $("#login-token").value = "";
    if (_loginResolve) { _loginResolve.resolve(tk); _loginResolve = null; }
  } else {
    $("#login-err").textContent = "令牌无效，请重试";
  }
};
$("#login-token").addEventListener("keydown", e => { if (e.key === "Enter") $("#login-submit").click(); });

/* ---------------- API ---------------- */
async function api(path, opts, retried) {
  const headers = Object.assign({ "Content-Type": "application/json" }, opts && opts.headers);
  const tk = getToken();
  if (tk) headers.Authorization = "Bearer " + tk;
  const r = await fetch(path, Object.assign({}, opts, { headers }));
  if (r.status === 401 && !retried) {
    await showLogin();
    return api(path, opts, true);
  }
  const data = await r.json().catch(() => ({}));
  if (!r.ok && data.error) throw new Error(data.error);
  if (!r.ok && data.detail) throw new Error(data.detail);
  return data;
}

/* ---------------- Sparkline（时间轴点 {t, v}） ---------------- */
class Spark {
  constructor(canvas, color, opts = {}) {
    this.cv = canvas;
    this.ctx = canvas.getContext("2d");
    this.color = color;
    this.slim = !!opts.slim;
    this.fmtV = opts.fmt || (v => fmt.num(v));
    this._pts = null;
    this._hoverX = null;
    this._bound = false;
    canvas.addEventListener("mousemove", e => {
      if (!this._pts || this._pts.length < 2) return;
      const r = canvas.getBoundingClientRect();
      this._hoverX = e.clientX - r.left;
      this._redraw();
    });
    canvas.addEventListener("mouseleave", () => {
      if (this._hoverX == null) return;
      this._hoverX = null;
      this._redraw();
    });
  }
  draw(points) { this._pts = points; this._hoverX = null; this._redraw(); }
  _redraw() {
    const cv = this.cv, ctx = this.ctx;
    if (!cv.isConnected) return;
    const dpr = window.devicePixelRatio || 1;
    const w = cv.clientWidth, h = cv.clientHeight || 68;
    if (!w) return;
    cv.width = w * dpr; cv.height = h * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    const points = this._pts;
    if (!points || points.length < 2) {
      ctx.fillStyle = "rgba(148,163,184,0.35)";
      ctx.font = "10px ui-monospace, monospace";
      ctx.fillText("采集中…", 4, h / 2);
      return;
    }
    const t0 = points[0].t, tN = points[points.length - 1].t || 1;
    let min = Infinity, max = -Infinity;
    points.forEach(p => { if (p.v != null) { min = Math.min(min, p.v); max = Math.max(max, p.v); } });
    if (!isFinite(min)) return;
    if (max === min) max = min + 1;
    const pad = (max - min) * 0.18;
    min = Math.max(0, min - pad); max = max + pad;
    const X = p => 2 + ((p.t - t0) / Math.max(0.001, tN - t0)) * (w - 4);
    const Y = v => h - 3 - ((v - min) / (max - min)) * (h - 8);
    if (!this.slim) {
      const grad = ctx.createLinearGradient(0, 0, 0, h);
      grad.addColorStop(0, this.color + "3d");
      grad.addColorStop(1, this.color + "00");
      ctx.beginPath();
      ctx.moveTo(X(points[0]), Y(points[0].v ?? 0));
      points.forEach(p => ctx.lineTo(X(p), Y(p.v ?? 0)));
      ctx.lineTo(X(points[points.length - 1]), h);
      ctx.lineTo(X(points[0]), h);
      ctx.closePath();
      ctx.fillStyle = grad;
      ctx.fill();
    }
    ctx.beginPath();
    let started = false;
    points.forEach(p => {
      if (p.v == null) return;
      if (!started) { ctx.moveTo(X(p), Y(p.v)); started = true; }
      else ctx.lineTo(X(p), Y(p.v));
    });
    ctx.strokeStyle = this.color;
    ctx.lineWidth = this.slim ? 1.4 : 1.6;
    ctx.lineJoin = "round";
    ctx.stroke();
    const lastP = [...points].reverse().find(p => p.v != null);
    if (lastP) {
      const lx = X(lastP), ly = Y(lastP.v);
      ctx.beginPath();
      ctx.arc(lx, ly, 2.6, 0, 7);
      ctx.fillStyle = this.color;
      ctx.fill();
    }
    /* 悬停取值：crosshair + 最近点 + 时间/数值气泡 */
    if (this._hoverX != null) {
      const tHover = t0 + ((this._hoverX - 2) / Math.max(1, w - 4)) * (tN - t0);
      let best = null;
      for (const p of points) {
        if (p.v == null) continue;
        if (!best || Math.abs(p.t - tHover) < Math.abs(best.t - tHover)) best = p;
      }
      if (best) {
        const hx = X(best), hy = Y(best.v);
        ctx.strokeStyle = "rgba(148,163,184,0.5)";
        ctx.lineWidth = 1;
        ctx.setLineDash([3, 3]);
        ctx.beginPath();
        ctx.moveTo(hx, 0);
        ctx.lineTo(hx, h);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.beginPath();
        ctx.arc(hx, hy, 3.6, 0, 7);
        ctx.fillStyle = this.color;
        ctx.fill();
        ctx.lineWidth = 1.4;
        ctx.strokeStyle = "rgba(255,255,255,0.85)";
        ctx.stroke();
        const hh = String(new Date(best.t * 1000).getHours()).padStart(2, "0");
        const mm = String(new Date(best.t * 1000).getMinutes()).padStart(2, "0");
        const ss = String(new Date(best.t * 1000).getSeconds()).padStart(2, "0");
        const label = `${hh}:${mm}:${ss}  ${this.fmtV(best.v)}`;
        ctx.font = "10.5px ui-monospace, monospace";
        const tw = ctx.measureText(label).width + 14;
        let bx = Math.min(Math.max(2, hx - tw / 2), w - tw - 2);
        const by = 3, bh = 20;
        ctx.beginPath();
        if (ctx.roundRect) ctx.roundRect(bx, by, tw, bh, 5);
        else ctx.rect(bx, by, tw, bh);
        ctx.fillStyle = "rgba(13,18,28,0.93)";
        ctx.fill();
        ctx.strokeStyle = "rgba(148,163,184,0.35)";
        ctx.lineWidth = 1;
        ctx.stroke();
        ctx.fillStyle = "#e6edf6";
        ctx.fillText(label, bx + 7, by + 14);
      }
    }
  }
}

/* ---------------- Toast / Modal / Help ---------------- */
function toast(msg, ok = true) {
  const t = el(`<div class="toast ${ok ? "ok" : "err"}"><span class="t-dot"></span><span class="t-msg">${esc(msg)}</span></div>`);
  $("#toasts").appendChild(t);
  setTimeout(() => { t.classList.add("leaving"); setTimeout(() => t.remove(), 300); }, 3600);
}

function closeModal() {
  if ($("#modal").dataset.closable === "0") return;   // 批量操作进行中不可关闭
  $("#modal-backdrop").hidden = true;
  $("#modal").innerHTML = "";
  $("#modal").dataset.closable = "1";
}
function confirmModal({ title, body, confirmText = "确认", danger = false, reason = false, onConfirm }) {
  const m = $("#modal");
  m.dataset.closable = "1";
  m.innerHTML = `
    <h3>${esc(title)}</h3>
    <div class="m-body">${body}</div>
    ${reason ? `<div class="m-reason"><input class="input" id="modal-reason" type="text" placeholder="操作原因（可选）" maxlength="200"></div>` : ""}
    <div class="m-actions">
      <button class="btn ghost" data-act="cancel">取消</button>
      <button class="btn ${danger ? "danger" : "primary"}" data-act="ok">${esc(confirmText)}</button>
    </div>`;
  $("#modal-backdrop").hidden = false;
  m.querySelectorAll("[data-act]").forEach(b => b.onclick = () => {
    if (b.dataset.act !== "ok") return closeModal();
    const rsn = reason ? ($("#modal-reason").value || "").trim() : "";
    closeModal();
    onConfirm(rsn);
  });
  if (reason) setTimeout(() => $("#modal-reason").focus(), 40);
}
/* 进度模态：批量操作期间不可关闭；update() 刷新进度，done() 切到结果态 */
function progressModal(title) {
  const m = $("#modal");
  m.dataset.closable = "0";
  m.innerHTML = `
    <h3>${esc(title)}</h3>
    <div class="m-body">
      <div class="pg-bar-track" style="height:6px;border-radius:3px;background:var(--surface-3);overflow:hidden;margin:10px 0 12px">
        <div class="pg-bar-fill" style="height:100%;width:0%;border-radius:3px;background:linear-gradient(90deg,#6d9bff,#547df2);transition:width .3s var(--ease)"></div>
      </div>
      <div class="pg-text" style="font-family:var(--mono);font-size:12px;color:var(--text-2)">准备中…</div>
    </div>`;
  $("#modal-backdrop").hidden = false;
  return {
    update(cur, total, okN, failN) {
      $(".pg-bar-fill", m).style.width = (total ? Math.round(cur / total * 100) : 100) + "%";
      $(".pg-text", m).textContent = `${cur} / ${total} · 成功 ${okN} · 失败 ${failN}`;
    },
    done(okN, failN, fails) {
      m.dataset.closable = "1";
      const failList = fails && fails.length
        ? `<div style="margin-top:10px;max-height:120px;overflow:auto;font-size:11px;font-family:var(--mono);color:var(--c-failed)">
            ${fails.slice(0, 8).map(f => `<div class="truncate" title="${esc(f.detail)}">${esc(f.target)} — ${esc(f.detail)}</div>`).join("")}
            ${fails.length > 8 ? `<div class="dim">…共 ${fails.length} 个失败</div>` : ""}</div>` : "";
      m.innerHTML = `
        <h3>批量操作完成</h3>
        <div class="m-body">
          <div style="display:flex;gap:16px;font-family:var(--mono);font-size:15px;font-weight:600">
            <span style="color:var(--c-succeeded)">成功 ${okN}</span>
            ${failN ? `<span style="color:var(--c-failed)">失败 ${failN}</span>` : '<span class="dim" style="font-size:12px">无失败</span>'}
          </div>${failList}
        </div>
        <div class="m-actions"><button class="btn primary" data-act="ok">关闭</button></div>`;
      $("[data-act=ok]", m).onclick = closeModal;
    },
    fail(msg) {
      m.dataset.closable = "1";
      m.innerHTML = `<h3>批量操作失败</h3><div class="m-body">${esc(msg)}</div>
        <div class="m-actions"><button class="btn primary" data-act="ok">关闭</button></div>`;
      $("[data-act=ok]", m).onclick = closeModal;
    },
  };
}
$("#modal-backdrop").addEventListener("mousedown", e => { if (e.target.id === "modal-backdrop") closeModal(); });


/* ---------------- Slideover ---------------- */
function openSlideover() {
  $("#slideover").classList.add("open");
  $("#slideover").setAttribute("aria-hidden", "false");
  $("#slideover-backdrop").classList.add("open");
}
function closeSlideover() {
  $("#slideover").classList.remove("open");
  $("#slideover").setAttribute("aria-hidden", "true");
  $("#slideover-backdrop").classList.remove("open");
  if (S.detail && S.detail.timer) clearInterval(S.detail.timer);
  S.detail = null;
}
$("#slideover-backdrop").addEventListener("click", closeSlideover);

/* ---------------- 命令 ---------------- */
async function sendCommand(kind, target, reason) {
  try {
    const r = await api("/api/command", { method: "POST", body: JSON.stringify({ kind, target, reason }) });
    if (r.ok) { toast(r.detail || "操作成功"); return true; }
    toast(r.detail || "操作失败", false);
    return false;
  } catch (e) { toast("操作失败：" + e.message, false); return false; }
}

/* ---------------- 图表序列（服务端历史 + 客户端实时拼接） ---------------- */
async function loadMetrics(force) {
  const now = Date.now() / 1000;
  if (!force && now - S.metrics.fetchedAt < 60) return;
  try {
    const d = await api(`/api/metrics?range=${S.metrics.range}`);
    S.metrics.server = d.points || [];
    S.metrics.fetchedAt = now;
  } catch (e) { /* 静默 */ }
}

function chartSeries(key, clientKey) {
  const sp = S.metrics.server.filter(p => p[key] != null).map(p => ({ t: p.t, v: p[key] }));
  const lastT = sp.length ? sp[sp.length - 1].t : 0;
  const cp = S.history.filter(p => p.t / 1000 > lastT && p[clientKey] != null)
    .map(p => ({ t: p.t / 1000, v: p[clientKey] }));
  return sp.concat(cp);
}

/* ================================================================
   视图：总览
   ================================================================ */
const Overview = {
  sparks: {}, chartDefs: null,
  render(root) {
    root.innerHTML = `
    <div class="vw">
      <div class="toolbar" style="margin-bottom:14px">
        <span class="fresh" data-fresh="overview"><span class="f-dot"></span><span class="f-text">连接中…</span></span>
        <span style="flex:1"></span>
        <select id="range-select" class="range-select" title="时间范围">
          <option value="1h">近 1 小时</option>
          <option value="6h">近 6 小时</option>
          <option value="12h">近 12 小时</option>
          <option value="24h">近 24 小时</option>
          <option value="48h">近 48 小时</option>
        </select>
      </div>
      <div class="chart-grid stc-grid" id="ov-charts"></div>
      <div class="card table-card">
        <div class="card-head">
          <div class="card-title">任务运行规模 Top</div>
          <a class="btn ghost sm" href="#/tasks">全部任务 →</a>
        </div>
        <div class="tbl-wrap"><table class="tbl"><thead>
          <tr><th>任务</th><th>优先级</th><th>运行中</th><th>等待中</th><th>需处理</th><th>生产者</th></tr>
        </thead><tbody id="ov-top-tasks"></tbody></table></div>
      </div>
    </div>`;
    this.buildCharts();
    const rs = $("#range-select");
    if (rs) {
      if (![...rs.options].some(o => o.value === S.metrics.range)) S.metrics.range = "24h";
      rs.value = S.metrics.range;
      rs.onchange = async () => {
        S.metrics.range = rs.value;
        S.metrics.fetchedAt = 0;
        await loadMetrics(true);
        this.refresh();
      };
    }
    loadMetrics().then(() => this.refresh());
    this.refresh();
  },
  buildCharts() {
    const defs = [
      { id: "waiting", title: "等待中", color: "#8ea3bd", st: "WAITING", key: "jWaiting", fmt: v => fmt.num(v) },
      { id: "finished", title: "已完成", color: "#2dd4bf", st: "FINISHED", key: "jFinished", fmt: v => fmt.num(v) },
      { id: "running", title: "运行中", color: "#34d399", st: "RUNNING_ACTIVE", key: "jRunning", fmt: v => fmt.num(v) },
      { id: "attention", title: "需处理", color: "#fb7185", st: "ATTENTION", key: "jAttention", fmt: v => fmt.num(v) },
      { id: "cpu", title: "执行 CPU", color: "#5e8bff", key: "cpu", fmt: v => fmt.pct(v, 1) },
      { id: "mem", title: "执行内存", color: "#f5bf4f", key: "mem", fmt: v => (v / 1024).toFixed(2) + " GB" },
      { id: "io", title: "执行 I/O", color: "#a78bfa", key: "io", fmt: v => fmt.bytes(v) + "/s" },
      { id: "producers", title: "生产者在线", color: "#38bdf8", key: "producers", fmt: v => Math.round(v) + " 个" },
    ];
    this.chartDefs = defs;
    this.sparks = {};
    $("#ov-charts").innerHTML = defs.map(d => `
      <div class="card chart-card ${d.st ? "stc-card" : ""}" ${d.st ? `data-st="${d.st}" title="点击查看${d.title}的作业"` : ""}>
        <div class="chart-head"><span class="chart-title">${d.title}</span><span class="chart-value" id="cv-${d.id}">—</span></div>
        <canvas id="spark-${d.id}"></canvas>
        <div class="chart-sub"><span id="cs-${d.id}-min">—</span><span id="cs-${d.id}-max">—</span></div>
      </div>`).join("");
    defs.forEach(d => {
      this.sparks[d.id] = new Spark($("#spark-" + d.id), d.color, { fmt: d.fmt });
    });
    $$("#ov-charts .stc-card").forEach(c => c.onclick = () => {
      location.hash = "#/jobs?status=" + c.dataset.st;
    });
  },
  refresh() {
    const o = S.overview;
    if (!o) return;
    const snap = o.snapshot;
    const c = o.job_counts || {};
    const grp = {
      waiting: (c.QUEUED || 0) + (c.BACKOFF || 0) + (c.PAUSED || 0) + (c.RECOVERING || 0),
      finished: (c.SUCCEEDED || 0) + (c.CANCELLED || 0),
      running: (c.STARTING || 0) + (c.RUNNING || 0) + (c.PAUSING || 0),
      attention: (c.FAILED || 0) + (c.RECOVERY_BLOCKED || 0),
    };
    const producers = snap ? snap.producers : [];
    const alive = producers.filter(p => p.alive).length;
    const taskN = o.task_specs ? Object.keys(o.task_specs).length : 0;
    const cpuN = o.machine ? o.machine.cpu_count : 1;
    const memT = o.machine ? o.machine.mem_total_mb : 0;
    const last = S.history[S.history.length - 1];
    const cur = {
      waiting: grp.waiting, finished: grp.finished, running: grp.running,
      attention: grp.attention, producers: alive,
      cpu: last ? last.cpuPct : null, mem: last ? last.memMb : null, io: last ? last.ioBps : null,
    };
    // 统一 8 卡更新（值 + 折线 + min/max）
    (this.chartDefs || []).forEach(d => {
      const key = d.id === "cpu" ? "cpuPct" : d.id === "mem" ? "memMb" : d.id === "io" ? "ioBps"
        : d.id === "producers" ? "producersAlive" : d.key;
      const pts = chartSeries(d.key, key);
      const cv = $("#cv-" + d.id);
      if (cv) {
        if (d.id === "producers") cv.innerHTML = `${alive}<small> / ${taskN} 任务</small>`;
        else if (d.id === "cpu") cv.innerHTML = last ? `${fmt.pct(last.cpuPct)}<small> / ${cpuN} 核</small>` : "—";
        else if (d.id === "mem") cv.innerHTML = last ? `${(last.memMb / 1024).toFixed(2)}<small>GB${memT ? " · " + fmt.bytes(memT * 1048576) : ""}</small>` : "—";
        else if (d.id === "io") cv.innerHTML = last ? `${fmt.bytes(last.ioBps)}<small> /秒</small>` : "—";
        else cv.textContent = fmt.num(cur[d.id] || 0);
      }
      const sp = this.sparks[d.id];
      sp && sp.draw(pts);
      const vs = pts.map(p => p.v).filter(v => v != null);
      const mn = $("#cs-" + d.id + "-min"), mx = $("#cs-" + d.id + "-max");
      if (mn && mx) {
        if (vs.length) {
          mn.textContent = "min " + d.fmt(Math.min(...vs));
          mx.textContent = "max " + d.fmt(Math.max(...vs));
        } else { mn.textContent = "—"; mx.textContent = "—"; }
      }
    });

    // Top 任务（四分法列）
    const tasks = S.tasksCache || [];
    const top = [...tasks].sort((a, b) =>
      ((b.monitor && b.monitor.live) || 0) - ((a.monitor && a.monitor.live) || 0)).slice(0, 8);
    $("#ov-top-tasks").innerHTML = top.length ? top.map(t => {
      const m = t.monitor || {};
      return `<tr class="clickable" tabindex="0" data-task="${esc(t.task_id)}">
        <td class="cell-mono">${esc(t.task_id)}</td>
        <td><span class="pri-chip"><span class="bar"></span>${t.priority}</span></td>
        <td class="num">${fmt.num(m.live || 0)}</td>
        <td class="num">${fmt.num(m.waiting ?? m.queued ?? 0)}</td>
        <td class="num ${m.attention ? "text-err" : "dim"}">${fmt.num(m.attention || 0)}</td>
        <td class="${m.producer && m.producer.alive ? "" : "text-err"}">${m.producer ? (m.producer.alive ? "在线" : "离线") : "—"}</td>
      </tr>`;
    }).join("") : `<tr><td colspan="6"><div class="empty"><p>暂无任务数据</p></div></td></tr>`;
    bindRows($$("#ov-top-tasks tr[data-task]"), tr => openTaskDetail(tr.dataset.task));
  },
};

/* ================================================================
   视图：任务

/* ================================================================
   视图：任务
   ================================================================ */
const Tasks = {
  q: "",
  render(root) {
    root.innerHTML = `
    <div class="vw">
      <div class="toolbar">
        <span class="dim" style="font-size:11.5px" id="task-count"></span>
        <span style="flex:1"></span>
        <span class="fresh" data-fresh="tasks"><span class="f-dot"></span><span class="f-text">加载中…</span></span>
      </div>
      <div class="card table-card tbl-wrap">
        <table class="tbl"><thead>
          <tr>
            <th class="sortable" data-k="task_id">任务</th>
            <th class="sortable" data-k="priority">优先级</th>
            <th class="sortable" data-k="live">运行中</th>
            <th class="sortable" data-k="waiting">等待中</th>
            <th class="sortable" data-k="attention">需处理</th>
            <th>启用</th>
            <th>生产者</th>
          </tr>
        </thead><tbody id="task-rows"></tbody></table>
      </div>
    </div>`;
    root.querySelectorAll("th.sortable").forEach(th => th.onclick = () => {
      const k = th.dataset.k;
      S.taskSort = { key: k, dir: S.taskSort.key === k ? -S.taskSort.dir : (k === "task_id" ? 1 : -1) };
      this.refresh(true);
    });
    this.refresh(true);
  },
  refresh(force) {
    if (window.getSelection && window.getSelection().toString()) return;
    const tasks = (S.tasksCache || []).filter(t => !this.q || t.task_id.includes(this.q));
    $$("th.sortable").forEach(th => {
      th.querySelectorAll(".arrow").forEach(a => a.remove());
      if (S.taskSort.key === th.dataset.k) {
        th.insertAdjacentHTML("beforeend",
          `<span class="arrow">${S.taskSort.dir === -1 ? "▼" : "▲"}</span>`);
      }
    });
    const payload = JSON.stringify(tasks) + this.q + S.taskSort.key + S.taskSort.dir;
    if (!force && payload === this._lastPayload) return;
    this._lastPayload = payload;
    const { key, dir } = S.taskSort;
    tasks.sort((a, b) => {
      let va, vb;
      if (key === "task_id") { va = a.task_id; vb = b.task_id; }
      else {
        va = (a.monitor || {})[key] ?? (a.scale || {})[key] ?? -1;
        vb = (b.monitor || {})[key] ?? (b.scale || {})[key] ?? -1;
      }
      if (typeof va === "string") return va.localeCompare(vb) * dir;
      const cmp = ((va || 0) - (vb || 0)) * dir;
      return cmp || a.task_id.localeCompare(b.task_id);
    });
    $("#task-count").textContent = `${tasks.length} 个任务`;
    const tb = $("#task-rows");
    if (!tb) return;
    tb.innerHTML = tasks.map(t => {
      const m = t.monitor || {}, sc = t.scale || {}, p = m.producer || {};
      const h = (m.health || [])[0];
      const slow = (m.health || []).some(x => x.slow);
      const crashTitle = p.crash_count ? `累计崩溃 ${p.crash_count} 次${p.last_crash_wall ? "，最近 " + fmt.rel(p.last_crash_wall) : ""}` : "无崩溃记录";
      return `<tr class="clickable" tabindex="0" data-task="${esc(t.task_id)}">
        <td class="cell-mono" style="max-width:240px"><div class="truncate">${esc(t.task_id)}</div></td>
        <td><span class="pri-chip"><span class="bar"></span>${t.priority}</span></td>
        <td class="num">${fmt.num(m.live || 0)}</td>
        <td class="num">${fmt.num(m.waiting ?? m.queued ?? 0)}</td>
        <td class="num ${m.attention ? "text-err" : "dim"}">${fmt.num(m.attention || 0)}</td>
        <td><span class="switch ${t.enabled ? "on" : ""}" data-task="${esc(t.task_id)}" title="点击${t.enabled ? "禁用" : "启用"}任务（停新准入，运行中不受影响）"></span></td>
        <td title="${esc(crashTitle)}">${p.pid ? (p.alive
          ? `<span class="muted" style="font-size:11px">在线</span>${p.crash_count ? ` <span title="${esc(crashTitle)}" style="color:var(--c-starting)">↻${p.crash_count}</span>` : ""}`
          : '<span style="color:var(--c-failed)">离线</span>') : "—"}</td>
      </tr>`;
    }).join("");
    bindRows($$("#task-rows tr[data-task]"), tr => openTaskDetail(tr.dataset.task));
    $$("#task-rows .switch").forEach(sw => sw.onclick = async e => {
      e.stopPropagation();
      const tid = sw.dataset.task;
      const enabling = !sw.classList.contains("on");
      try {
        const r = await api("/api/task", { method: "POST", body: JSON.stringify({ task_id: tid, action: enabling ? "enable" : "disable" }) });
        toast(r.detail || (enabling ? "已启用" : "已禁用"), r.ok !== false);
        await refreshTasks();
        Tasks.refresh(true);
      } catch (err) {
        toast("操作失败：" + err.message, false);
      }
    });
  },
};

/* ================================================================
   视图：作业
   ================================================================ */
const Jobs = {
  loading: false,
  data: { total: 0, page: 1, limit: 50, items: [] },
  /* 选择集生命周期：筛选（状态/任务/搜索）变化或离开作业视图 → 清空；
     翻页 / 排序变化 → 保留（同一结果集内跨页累积是批量操作的真实需求） */
  clearSelection() {
    S.jobsSel.clear();
    this.paintSelection();
  },
  render(root) {
    const o = S.overview;
    const counts = (o && o.job_counts) || {};
    const specs = (o && o.task_specs) || {};
    const f = S.jobsFilter;
    root.innerHTML = `
    <div class="vw">
      <div class="toolbar">
        <div class="pills" id="job-pills"></div>
        <span style="flex:1"></span>
        <select class="select" id="job-task">
          <option value="">全部任务</option>
          ${Object.keys(specs).sort().map(t => `<option value="${esc(t)}" ${f.task === t ? "selected" : ""}>${esc(t)}</option>`).join("")}
        </select>
        <span class="fresh" data-fresh="jobs"><span class="f-dot"></span><span class="f-text">加载中…</span></span>
      </div>
      <div class="chips" id="job-chips"></div>
      <div class="card table-card">
        <div class="tbl-wrap" style="max-height:calc(100vh - 290px);overflow-y:auto">
          <table class="tbl"><thead><tr>
            <th style="width:30px"><input type="checkbox" class="rowcheck" id="sel-all" title="全选本页作业"></th>
            <th>作业 ID</th><th>任务</th>
            <th class="sortable" data-k="priority">优先级</th><th>状态</th>
            <th class="sortable" data-k="recent">接收时间</th><th>开始 / 结束</th><th>原因</th>
          </tr></thead><tbody id="job-rows">
            ${'<tr><td colspan="8"><div class="skeleton-row"></div></td></tr>'.repeat(6)}
          </tbody></table>
        </div>
        <div class="batchbar" id="batchbar" hidden></div>
        <div class="pager" id="job-pager"></div>
      </div>
    </div>`;
    $("#job-task").onchange = e => {
      S.jobsFilter.task = e.target.value; S.jobsFilter.page = 1;
      this.clearSelection(); this.syncHash(); this.fetchNow();
    };
    S.jobCounts = counts;
    this.paintPills();
    root.querySelectorAll("th.sortable").forEach(th => th.onclick = () => {
      const k = th.dataset.k;
      if (k === "priority") {
        S.jobsFilter.sort = S.jobsFilter.sort === "priority" ? "priority_asc" : "priority";
      } else {
        S.jobsFilter.sort = S.jobsFilter.sort === "recent" ? "oldest" : "recent";
      }
      S.jobsFilter.page = 1;
      this.syncHash(); this.fetchNow();
    });
    $("#sel-all") && ($("#sel-all").onclick = () => {
      const on = $("#sel-all").checked;
      this.data.items.forEach(j => { if (on) S.jobsSel.add(j.job_id); else S.jobsSel.delete(j.job_id); });
      this.paintSelection();
    });
    this.fetchNow();
  },
  syncHash() {
    const f = S.jobsFilter;
    const qs = [];
    if (f.status) qs.push("status=" + encodeURIComponent(f.status));
    if (f.task) qs.push("task=" + encodeURIComponent(f.task));
    if (f.q) qs.push("q=" + encodeURIComponent(f.q));
    if (f.sort !== "recent") qs.push("sort=" + f.sort);
    if (f.page > 1) qs.push("page=" + f.page);
    if (f.limit !== 50) qs.push("limit=" + f.limit);
    const h = "#/jobs" + (qs.length ? "?" + qs.join("&") : "");
    if (location.hash !== h) history.replaceState(null, "", h);
  },
  async fetchNow() {
    const f = S.jobsFilter;
    this.loading = true;
    try {
      const qs = new URLSearchParams({ page: f.page, limit: f.limit, sort: f.sort });
      if (f.status) qs.set("status", f.status);
      if (f.task) qs.set("task", f.task);
      if (f.q) qs.set("q", f.q);
      this.data = await api("/api/jobs?" + qs);
      S.fresh.jobs = Date.now();
    } catch (e) {
      const tb = $("#job-rows");
      if (tb) tb.innerHTML = `<tr><td colspan="8"><div class="empty"><p>加载失败：${esc(e.message)}</p></div></td></tr>`;
    }
    this.loading = false;
    this.paint();
  },
  /* 四分法状态条：全部 + 4 大类（聚合计数，悬停显示构成）；选中大类（或
     其细分状态）时展开该大类的细分级 pills 供精确筛选 */
  paintPills() {
    const el = $("#job-pills");
    if (!el) return;
    const f = S.jobsFilter;
    const counts = S.jobCounts || {};
    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    let html = `<button class="pill ${!f.status ? "active" : ""}" data-st="">全部<span class="p-count">${fmt.num(total)}</span></button>`;
    for (const g of Object.keys(GROUP_STATES)) {
      const n = GROUP_STATES[g].reduce((a, s) => a + (counts[s] || 0), 0);
      const tip = GROUP_STATES[g].map(s => `${STATUS_CN[s]} ${fmt.num(counts[s] || 0)}`).join(" · ");
      html += `<button class="pill ${STATUS_GROUPS[g].cls} ${f.status === g ? "active" : ""}" data-st="${g}" title="${esc(tip)}">${STATUS_CN[g]}<span class="p-count">${fmt.num(n)}</span></button>`;
    }
    el.innerHTML = html;
    $$("#job-pills .pill").forEach(p => p.onclick = () => {
      S.jobsFilter.status = p.dataset.st; S.jobsFilter.page = 1;
      this.clearSelection();
      this.paintPills();
      this.syncHash(); this.fetchNow();
    });
  },
  paintSelection() {
    $$("#job-rows tr[data-job]").forEach(tr => {
      const cb = $(".rowcheck", tr);
      if (cb) cb.checked = S.jobsSel.has(tr.dataset.job);
    });
    const all = $("#sel-all");
    if (all) {
      all.checked = this.data.items.length > 0
        && this.data.items.every(j => S.jobsSel.has(j.job_id));
    }
    const bar = $("#batchbar");
    if (!bar) return;
    if (S.jobsSel.size === 0) { bar.hidden = true; bar.innerHTML = ""; return; }
    // 按选择内容计算各动作适用数
    const sel = this.data.items.filter(j => S.jobsSel.has(j.job_id));
    const selStatus = new Map(sel.map(j => [j.job_id, j.status]));
    const nCancel = [...selStatus.values()].filter(st => LIVE.has(st) || st === "QUEUED").length;
    const nRequeue = [...selStatus.values()].filter(st => REQUEUEABLE(st)).length;
    const nDelete = [...selStatus.values()].filter(st => !LIVE.has(st)).length;
    bar.hidden = false;
    bar.innerHTML = `
      <span>已选 <b class="num">${S.jobsSel.size}</b> 个作业</span>
      ${nCancel ? `<button class="btn danger sm" id="batch-cancel">停止/取消 ${nCancel}</button>` : ""}
      ${nRequeue ? `<button class="btn primary sm" id="batch-requeue">批量重排 ${nRequeue}</button>` : ""}
      ${nDelete ? `<button class="btn danger sm" id="batch-delete">删除 ${nDelete}</button>` : ""}
      <button class="btn ghost sm" id="batch-clear">清除选择</button>
      <span class="dim" style="margin-left:auto;font-size:11px">取消=协作停止并置已取消 · 删除=物理清文件+列表移除（登记逻辑保留）· 翻页保留选择，切换筛选清空</span>`;
    $("#batch-clear").onclick = () => this.clearSelection();
    const bc = $("#batch-cancel");
    if (bc) bc.onclick = () => confirmModal({
      title: "批量取消",
      body: `将 <b>${nCancel}</b> 个排队/运行中作业终止执行并置为「已取消」终态，不再自动调度（检查点保留，可重新排队复活）。`,
      confirmText: `取消 ${nCancel} 个作业`, danger: true, reason: true,
      onConfirm: rsn => this.runCommandBatch("cancel_job",
        sel.filter(j => LIVE.has(j.status) || j.status === "QUEUED").map(j => j.job_id), rsn, "批量取消"),
    });
    const br = $("#batch-requeue");
    if (br) br.onclick = () => confirmModal({
      title: "批量重新排队",
      body: `将 <b>${nRequeue}</b> 个作业重排回 QUEUED（每个作业先关闭遗留 attempt）。`,
      confirmText: `重排 ${nRequeue} 个作业`, reason: true,
      onConfirm: rsn => this.runCommandBatch("requeue_job",
        sel.filter(j => REQUEUEABLE(j.status)).map(j => j.job_id), rsn, "批量重排"),
    });
    const bd = $("#batch-delete");
    if (bd) bd.onclick = () => confirmModal({
      title: "批量删除作业",
      body: `删除 <b>${nDelete}</b> 个非运行中作业：本地工件物理清除，从列表与统计中移除（登记逻辑保留）；运行中的 ${S.jobsSel.size - nDelete} 个将被跳过。`,
      confirmText: `删除 ${nDelete} 个作业`, danger: true, reason: true,
      onConfirm: async rsn => {
        const ids = sel.filter(j => !LIVE.has(j.status)).map(j => j.job_id);
        this.clearSelection();
        const pm = progressModal(`批量删除（${ids.length} 个）`);
        pm.update(0, ids.length, 0, 0);
        try {
          const r = await api("/api/job_delete_batch", {
            method: "POST",
            body: JSON.stringify({ job_ids: ids }),
          });
          pm.done(r.ok || 0, r.fail || 0, (r.results || []).filter(x => !x.ok));
          toast(`批量删除完成：成功 ${r.ok || 0}，失败 ${r.fail || 0}`, (r.fail || 0) === 0);
        } catch (e) { pm.fail("批量删除端点调用失败：" + e.message); }
        this.fetchNow();
      },
    });
  },
  /* 控制通道批量命令（取消/重排共用）：确认即清选择 + 进度模态 + 批量端点 */
  async runCommandBatch(kind, ids, reason, label) {
    this.clearSelection();
    const pm = progressModal(`${label}（${ids.length} 个）`);
    pm.update(0, ids.length, 0, 0);
    try {
      const r = await api("/api/command_batch", {
        method: "POST",
        body: JSON.stringify({ kind, targets: ids, reason: reason || ("Web " + label) }),
      });
      const fails = (r.results || []).filter(x => !x.ok);
      pm.done(r.ok || 0, r.fail || 0, fails);
      toast(`${label}完成：成功 ${r.ok || 0}，失败 ${r.fail || 0}`, (r.fail || 0) === 0);
    } catch (e) {
      pm.fail("批量端点调用失败：" + e.message);
    }
    this.fetchNow();
  },
  _rowInner(j) {
    const tip = `${j.job_id}\n原因：${j.reason || "—"}\n幂等键：${j.idempotency_key || "—"}${j.next_retry_at ? "\n下次重试：" + fmt.dateTime(j.next_retry_at) : ""}`;
    return `
      <td><input type="checkbox" class="rowcheck" data-sel="${esc(j.job_id)}" ${S.jobsSel.has(j.job_id) ? "checked" : ""}></td>
      <td class="cell-mono truncate" style="max-width:330px" title="${esc(tip)}">${esc(j.job_id)}</td>
      <td class="cell-mono dim clickable" style="max-width:170px" data-tasklink="${esc(j.task_id)}" title="筛选任务 ${esc(j.task_id)}"><div class="truncate">${esc(j.task_id)}</div></td>
      <td><span class="pri-chip"><span class="bar"></span>${j.priority}</span></td>
      <td>${groupBadge(j.status)}</td>
      <td class="dim mono" style="font-size:11px;white-space:nowrap">${fmt.dateTime(j.queued_at)}</td>
      <td class="dim mono" style="font-size:11px;white-space:nowrap;line-height:1.5">${jobTimes(j)}</td>
      <td class="dim truncate" style="max-width:180px;font-size:11.5px" title="${esc(j.reason)}">${esc(j.reason || "—")}</td>`;
  },
  _bindRow(tr, d) {
    const open = () => openJobDetail(tr.dataset.job, { source: d.items.map(x => x.job_id) });
    tr.onclick = open;
    tr.onkeydown = e => { if (e.key === "Enter") { e.preventDefault(); open(); } };
    $("[data-tasklink]", tr).onclick = e => {
      e.stopPropagation();
      S.jobsFilter.task = e.currentTarget.dataset.tasklink; S.jobsFilter.page = 1;
      this.clearSelection();
      const ts = $("#job-task"); if (ts) ts.value = S.jobsFilter.task;
      this.syncHash(); this.fetchNow();
    };
    const cb = $(".rowcheck", tr);
    if (cb) cb.onclick = e => {
      e.stopPropagation();
      if (cb.checked) S.jobsSel.add(cb.dataset.sel); else S.jobsSel.delete(cb.dataset.sel);
      this.paintSelection();
    };
  },
  paint() {
    const tb = $("#job-rows");
    if (!tb) return;
    const f = S.jobsFilter;
    const d = this.data;
    // 筛选 chips
    const chips = [];
    if (f.status) chips.push({ k: "status", label: `状态：${STATUS_CN[f.status]}` });
    if (f.task) chips.push({ k: "task", label: `任务：${f.task}` });
    if (f.sort !== "recent") chips.push({ k: "sort", label: `排序：${{ oldest: "最早接收", priority: "优先级降序", priority_asc: "优先级升序" }[f.sort] || f.sort}`, neutral: true });
    $("#job-chips").innerHTML = chips.length
      ? chips.map(c => `<span class="chip ${c.neutral ? "neutral" : ""}" data-k="${c.k}"><b>${esc(c.label)}</b><span class="x" title="移除">×</span></span>`).join("")
        + `<button class="chip-clear" id="chips-clear">清除全部</button>`
      : `<span class="hint">无筛选 · 显示全部作业</span>`;
    $$("#job-chips .chip .x").forEach(x => x.onclick = () => {
      const k = x.parentElement.dataset.k;
      S.jobsFilter[k] = k === "sort" ? "recent" : "";
      S.jobsFilter.page = 1;
      this.clearSelection();
      const tq = $("#job-task"); if (tq && k === "task") tq.value = "";
      const qi = $("#job-q"); if (qi && k === "q") qi.value = "";
      this.paintPills();
      this.syncHash(); this.fetchNow();
    });
    const cc = $("#chips-clear");
    if (cc) cc.onclick = () => {
      S.jobsFilter = { status: "", task: "", q: "", sort: "recent", page: 1, limit: S.jobsFilter.limit };
      this.clearSelection();
      const tq = $("#job-task"); if (tq) tq.value = "";
      const qi = $("#job-q"); if (qi) qi.value = "";
      this.paintPills();
      this.syncHash(); this.fetchNow();
    };

    // 表体：行级 diff（消除整表重绘的闪烁/悬停丢失/滚动跳动）
    if (!d.items || !d.items.length) {
      const hasFilter = f.status || f.task || f.q;
      tb.innerHTML = `<tr><td colspan="8"><div class="empty">
        <svg class="ic" viewBox="0 0 24 24"><path d="M7 8V5a2 2 0 0 1 2-2h6a2 2 0 0 1 2 2v3M5 8h14a1.5 1.5 0 0 1 1.5 1.5V19a2 2 0 0 1-2 2h-13a2 2 0 0 1-2-2V9.5A1.5 1.5 0 0 1 5 8Z"/></svg>
        <p>没有匹配的作业</p>
        ${hasFilter ? '<button class="btn sm" id="empty-clear">清除筛选</button>' : ""}</div></td></tr>`;
      const ec = $("#empty-clear");
      if (ec) ec.onclick = () => {
        S.jobsFilter = { status: "", task: "", q: "", sort: "recent", page: 1, limit: S.jobsFilter.limit };
        this.clearSelection();
        $$("#job-pills .pill").forEach(p => p.classList.toggle("active", !p.dataset.st));
        const tq = $("#job-task"); if (tq) tq.value = "";
        const qi = $("#job-q"); if (qi) qi.value = "";
        this.syncHash(); this.fetchNow();
      };
    } else {
      const bucket = Math.floor(Date.now() / 60000);  // 分钟桶：相对时间刷新
      const sig = j => JSON.stringify([j.status, j.retry_count, j.reason, bucket]);
      const existing = new Map();
      $$("#job-rows tr[data-job]").forEach(tr => existing.set(tr.dataset.job, tr));
      const want = new Set(d.items.map(j => j.job_id));
      // 清掉不在结果集里的行（含初始骨架行）
      Array.from(tb.children).forEach(tr => {
        const id = tr.dataset && tr.dataset.job;
        if (!id || !want.has(id)) tr.remove();
      });
      let ref = tb.firstElementChild;
      d.items.forEach(j => {
        let tr = existing.get(j.job_id);
        if (!tr) {
          tr = el(`<tr class="clickable" tabindex="0" data-job="${esc(j.job_id)}">${this._rowInner(j)}</tr>`);
          tr.dataset.sig = sig(j);
          this._bindRow(tr, d);
        } else if (tr.dataset.sig !== sig(j)) {
          tr.innerHTML = this._rowInner(j);
          tr.dataset.sig = sig(j);
          this._bindRow(tr, d);
        }
        if (ref === tr) { ref = tr.nextElementSibling; }
        else if (ref) { tb.insertBefore(tr, ref); }
        else { tb.appendChild(tr); }
      });
    }
    this.paintSelection();

    // 排序箭头
    $$("th.sortable").forEach(th => {
      th.querySelectorAll(".arrow").forEach(a => a.remove());
      const k = th.dataset.k;
      const active = (k === "priority" && (f.sort === "priority" || f.sort === "priority_asc"))
        || (k === "recent" && (f.sort === "recent" || f.sort === "oldest"));
      if (active) {
        const desc = f.sort === "priority" || f.sort === "recent";
        th.insertAdjacentHTML("beforeend", `<span class="arrow">${desc ? "▼" : "▲"}</span>`);
      }
    });

    // 分页（跳页 + 每页条数）
    const pages = Math.max(1, Math.ceil(d.total / f.limit));
    $("#job-pager").innerHTML = `
      <span class="pg-info">共 ${fmt.num(d.total)} 个作业</span>
      <button class="btn sm" data-pg="first" ${d.page <= 1 ? "disabled" : ""}>«</button>
      <button class="btn sm" data-pg="prev" ${d.page <= 1 ? "disabled" : ""}>‹ 上一页</button>
      <span class="jump"><input id="pg-jump" type="number" min="1" max="${pages}" value="${d.page}"> / ${fmt.num(pages)} 页</span>
      <button class="btn sm" data-pg="next" ${d.page >= pages ? "disabled" : ""}>下一页 ›</button>
      <select class="select" id="pg-limit" style="padding:3.5px 26px 3.5px 9px;font-size:11.5px">
        ${[50, 100, 200].map(n => `<option value="${n}" ${f.limit === n ? "selected" : ""}>${n} 条/页</option>`).join("")}
      </select>`;
    $$("#job-pager [data-pg]").forEach(b => b.onclick = () => {
      const pg = b.dataset.pg;
      S.jobsFilter.page = pg === "first" ? 1 : pg === "last" ? pages
        : Math.min(pages, Math.max(1, S.jobsFilter.page + (pg === "next" ? 1 : -1)));
      this.syncHash(); this.fetchNow();
    });
    $("#pg-jump").addEventListener("keydown", e => {
      if (e.key === "Enter") {
        S.jobsFilter.page = Math.min(pages, Math.max(1, parseInt(e.target.value) || 1));
        this.syncHash(); this.fetchNow();
      }
    });
    $("#pg-limit").onchange = e => {
      S.jobsFilter.limit = parseInt(e.target.value);
      S.jobsFilter.page = 1;
      this.syncHash(); this.fetchNow();
    };
  },
};

/* ================================================================
   视图：实时执行
   ================================================================ */
const Attempts = {
  data: { items: [] },
  render(root) {
    const specs = (S.overview && S.overview.task_specs) || {};
    root.innerHTML = `
    <div class="vw">
      <div class="toolbar">
<span class="dim" style="font-size:12px">正在运行的执行过程</span>
        <select class="select" id="att-task">
          <option value="">全部任务</option>
          ${Object.keys(specs).sort().map(t => `<option value="${esc(t)}" ${S.attFilter.task === t ? "selected" : ""}>${esc(t)}</option>`).join("")}
        </select>
        <span style="flex:1"></span>
        <span class="dim" style="font-size:11px" id="att-summary"></span>
        <span class="fresh" data-fresh="attempts"><span class="f-dot"></span><span class="f-text">加载中…</span></span>
      </div>
      <div class="card table-card">
        <div class="tbl-wrap" style="max-height:calc(100vh - 220px);overflow-y:auto">
          <table class="tbl"><thead><tr>
            <th>Attempt</th><th>作业</th><th>任务</th><th>优先级</th><th>状态</th>
            <th>PID</th><th>CPU</th><th>内存</th><th>启动时刻</th><th>已运行</th><th></th>
          </tr></thead><tbody id="att-rows">
            ${'<tr><td colspan="11"><div class="skeleton-row"></div></td></tr>'.repeat(8)}
          </tbody></table>
        </div>
        <div class="pager" id="att-pager"></div>
      </div>
    </div>`;
    $("#att-task").onchange = e => { S.attFilter.task = e.target.value; S.attFilter.page = 1; this.paint(); };
    this.fetchNow();
  },
  async fetchNow() {
    try {
      this.data = await api("/api/attempts/live");
      S.fresh.attempts = Date.now();
      this.paint();
    } catch (e) {
      const tb = $("#att-rows");
      if (tb) tb.innerHTML = `<tr><td colspan="11"><div class="empty"><p>加载失败：${esc(e.message)}</p></div></td></tr>`;
    }
  },
  filtered() {
    return (this.data.items || []).filter(a =>
      (!S.attFilter.status || a.status === S.attFilter.status)
      && (!S.attFilter.task || a.task_id === S.attFilter.task));
  },
  paint() {
    const tb = $("#att-rows");
    if (!tb) return;
    const items = this.filtered();
    const byStatus = {};
    (this.data.items || []).forEach(a => byStatus[a.status] = (byStatus[a.status] || 0) + 1);
    $("#att-summary").innerHTML = `存活 <b class="num" style="color:var(--text)">${fmt.num(this.data.items.length)}</b>
      ${Object.entries(byStatus).map(([s, n]) => ` · <span style="color:${STATUS_COLOR[s]}">${STATUS_CN[s]} ${n}</span>`).join("")}`;
    // 客户端分页
    const pages = Math.max(1, Math.ceil(items.length / S.attFilter.limit));
    if (S.attFilter.page > pages) S.attFilter.page = pages;
    const start = (S.attFilter.page - 1) * S.attFilter.limit;
    const pageItems = items.slice(start, start + S.attFilter.limit);
    const memT = (S.overview && S.overview.machine && S.overview.machine.mem_total_mb) || 30919;
    tb.innerHTML = pageItems.map(a => `
      <tr tabindex="0">
        <td class="cell-mono dim" style="cursor:pointer;color:var(--text-2)" data-att="${esc(a.attempt_id)}" data-job="${esc(a.job_id)}" title="查看所属作业">${esc(a.attempt_id)}</td>
        <td class="cell-mono truncate clickable" style="max-width:280px;color:var(--accent)" data-job="${esc(a.job_id)}" title="${esc(a.job_id)}">${esc(a.job_id)}</td>
        <td class="cell-mono dim" style="max-width:160px;cursor:pointer" data-task="${esc(a.task_id)}" title="查看任务详情"><div class="truncate">${esc(a.task_id)}</div></td>
        <td><span class="pri-chip"><span class="bar"></span>${a.priority}</span></td>
        <td>${badge(a.status)}</td>
        <td class="num dim">${a.pid || "—"}</td>
        <td>${a.cpu_pct != null ? `<span class="res-bar ${a.cpu_pct > 60 ? "hot" : ""}"><span class="rb"><i style="width:${Math.min(100, a.cpu_pct)}%"></i></span>${a.cpu_pct.toFixed(1)}%</span>` : "—"}</td>
        <td>${a.mem_mb != null ? `<span class="res-bar ${a.mem_mb / memT > 0.03 ? "hot" : ""}"><span class="rb"><i style="width:${Math.min(100, a.mem_mb / memT * 100)}%"></i></span>${fmt.num(Math.round(a.mem_mb))}MB</span>` : "—"}</td>
        <td class="dim" style="font-size:11.5px">${fmt.time(a.started_at)}</td>
        <td class="num" style="font-size:11.5px">${a.started_at ? fmt.dur((Date.now() - new Date(a.started_at)) / 1000) : "—"}</td>
        <td><button class="btn ghost sm" data-cancel="${esc(a.attempt_id)}">取消</button></td>
      </tr>`).join("");
    bindRows($$("#att-rows [data-job]"), td => openJobDetail(td.dataset.job, {
      source: [...new Set(pageItems.map(x => x.job_id))],
      focusAttempt: td.dataset.att || null,
    }));
    $$("#att-rows [data-task]").forEach(td => td.onclick = e => {
      e.stopPropagation(); openTaskDetail(td.dataset.task);
    });
    $$("#att-rows [data-cancel]").forEach(b => b.onclick = e => {
      e.stopPropagation();
      const aid = b.dataset.cancel;
      confirmModal({
        title: "取消执行尝试",
        body: `终止 <code>${esc(aid)}</code> 的整个执行进程树（先协作退出，宽限后强杀），作业置为「已取消」终态，不再自动调度；检查点保留，可手动重新排队复活。`,
        confirmText: "取消执行", danger: true, reason: true,
        onConfirm: rsn => sendCommand("cancel_attempt", aid, rsn).then(() => setTimeout(() => this.fetchNow(), 800)),
      });
    });
    $("#att-pager").innerHTML = `
      <span class="pg-info">本页 ${pageItems.length} / 筛选后 ${fmt.num(items.length)}</span>
      <button class="btn sm" data-pg="prev" ${S.attFilter.page <= 1 ? "disabled" : ""}>‹</button>
      <span class="jump"><span class="mono" style="font-size:11px">${S.attFilter.page} / ${pages}</span></span>
      <button class="btn sm" data-pg="next" ${S.attFilter.page >= pages ? "disabled" : ""}>›</button>`;
    $$("#att-pager [data-pg]").forEach(b => b.onclick = () => {
      S.attFilter.page += b.dataset.pg === "next" ? 1 : -1;
      this.paint();
    });
  },
};

/* ================================================================
   详情：作业（局部更新：骨架一次构建 + attempt diff + 时间线增量 + 日志 skip-same）
   ================================================================ */
async function openJobDetail(jobId, opts = {}) {
  const inner = $("#slideover-inner");
  inner.innerHTML = `<div class="skeleton-row"></div><div class="skeleton-row"></div><div class="skeleton-row"></div>`;
  openSlideover();
  if (S.detail && S.detail.timer) clearInterval(S.detail.timer);
  S.detail = {
    jobId, idx: opts.source ? opts.source.indexOf(jobId) : -1,
    source: opts.source || [jobId],
    attState: (S.detail && S.detail.jobId === jobId && S.detail.attState) || {},
    follow: (S.detail && S.detail.jobId === jobId && S.detail.follow) || {},
    openPayloads: (S.detail && S.detail.jobId === jobId && S.detail.openPayloads) || new Set(),
    tlFilter: (S.detail && S.detail.jobId === jobId && S.detail.tlFilter) || {},
    _evCache: {}, _tlMax: {}, _tlLimit: {},
    panelBuilt: false, timer: null,
  };
  await refreshJobDetail(opts.focusAttempt || null);
  // 存活作业：自动刷新（局部更新，不重建 DOM，无闪烁）
  S.detail.timer = setInterval(() => {
    const j = S.detail && S.detail.job;
    if (S.detail && j && LIVE.has(j.status) && $("#slideover").classList.contains("open")) {
      refreshJobDetail(null, true);
    }
  }, 8000);
}

async function refreshJobDetail(focusAttempt, silent) {
  const det = S.detail;
  if (!det) return;
  let data;
  try {
    data = await api("/api/jobs/" + encodeURIComponent(det.jobId));
  } catch (e) {
    if (!silent) $("#slideover-inner").innerHTML = `<div class="empty"><p>${esc(e.message)}</p></div>`;
    return;
  }
  if (!S.detail || S.detail.jobId !== det.jobId) return;  // 已切换
  det.job = data.job;
  if (!det.panelBuilt || !$("#jd-badge")) {
    buildJobPanel(data, focusAttempt);
    det.panelBuilt = true;
  } else {
    updateJobPanel(data);
  }
}

/* ---------- 面板骨架（每个作业只构建一次） ---------- */
function buildJobPanel(data, focusAttempt) {
  const det = S.detail;
  const inner = $("#slideover-inner");
  const j = data.job, atts = data.attempts || [];
  inner.innerHTML = `
    <div class="so-head">
      <div style="min-width:0">
        <div style="margin-bottom:8px;display:flex;gap:8px;align-items:center">
          <span id="jd-badge"></span>
          <span id="jd-copy"></span>
          <span id="jd-live" class="dim" style="font-size:10.5px" hidden>自动刷新中 · 8s</span>
        </div>
        <div class="so-title">${esc(j.job_id)}</div>
        <div class="dim" style="font-size:11.5px;margin-top:4px">${esc(j.task_id)} · 优先级 ${j.priority} · 接收于 ${fmt.dateTime(j.queued_at)}</div>
      </div>
      <button class="so-close" title="关闭 (Esc)">
        <svg class="ic" viewBox="0 0 24 24"><path d="M6 6l12 12M18 6 6 18" stroke-linecap="round"/></svg>
      </button>
    </div>
    <div class="so-actions">
      <button class="btn danger sm" id="jd-cancel">取消作业</button>
      <button class="btn sm" id="jd-requeue">重新排队</button>
      <button class="btn danger sm" id="jd-delete">删除作业</button>
      <span style="flex:1"></span>
      <span class="dim mono" style="font-size:10.5px">#${j.receive_seq}</span>
    </div>
    <div class="so-section">
      <div class="so-section-title">执行状态</div>
      <dl class="dl">
        <dt>当前状态</dt><dd class="plain" id="jd-statusline"></dd>
        <dt>开始时刻</dt><dd id="jd-started"></dd>
        <dt>结束时刻</dt><dd id="jd-finished"></dd>
        <dt>状态序号</dt><dd id="jd-statusseq"></dd>
      </dl>
    </div>
    <div class="so-section">
      <div class="so-section-title">协议与输入</div>
      <dl class="dl">
        <dt>幂等键</dt><dd>${esc(j.idempotency_key || "—")}</dd>
        <dt>内容摘要</dt><dd>${esc(j.content_hash || "")}</dd>
        <dt>工作目录</dt><dd>${esc(j.workdir)}</dd>
        <dt>入口参数</dt><dd>${esc(JSON.stringify(j.entry_args_json ?? []))}</dd>
        <dt>输入引用</dt><dd>${esc((j.input_refs_json || []).join("  ") || "—")}</dd>
        <dt>业务环境</dt><dd>${esc(Object.keys(j.env_json || {}).length ? JSON.stringify(j.env_json) : "—")}</dd>
      </dl>
    </div>
    <div class="so-section">
      <div class="so-section-title">执行尝试（<span id="jd-attn">0</span>）</div>
      <div id="so-attempts"></div>
    </div>
    <div class="so-nav">
      <button class="btn ghost sm" id="so-prev" ${det.idx > 0 ? "" : "disabled"}>‹ 上一项</button>
      <span class="dim">${det.idx >= 0 ? `${det.idx + 1} / ${det.source.length}` : ""}</span>
      <button class="btn ghost sm" id="so-next" ${det.idx >= 0 && det.idx < det.source.length - 1 ? "" : "disabled"}>下一项 ›</button>
    </div>`;
  $("#jd-copy", inner).appendChild(copyBtn(j.job_id, "复制作业 ID"));
  $(".so-close", inner).onclick = closeSlideover;
  $("#so-prev", inner).onclick = () => { if (det.idx > 0) openJobDetail(det.source[det.idx - 1], { source: det.source }); };
  $("#so-next", inner).onclick = () => { if (det.idx < det.source.length - 1) openJobDetail(det.source[det.idx + 1], { source: det.source }); };
  $("#jd-cancel", inner).onclick = () => confirmModal({
    title: "取消作业",
    body: `取消 <code>${esc(j.job_id)}</code>：终止存活执行进程树（先协作退出，宽限后强杀），作业置为「已取消」终态，不再自动调度；检查点保留，可重新排队复活。不影响其他生产者。`,
    confirmText: "确认取消", danger: true, reason: true,
    onConfirm: rsn => sendCommand("cancel_job", j.job_id, rsn).then(ok => ok && refreshJobDetail()),
  });
  $("#jd-delete", inner).onclick = () => confirmModal({
    title: "删除作业",
    body: `删除 <code>${esc(j.job_id)}</code>：本地工件（worker.log 等）物理清除，作业从列表与统计中移除。<br><span class="dim">数据库登记以逻辑删除保留（接收去重锚点仍在，生产者重发不会重复执行）；任务级删除才彻底清表。</span>`,
    confirmText: "确认删除", danger: true,
    onConfirm: async () => {
      try {
        const r = await api("/api/job_delete", { method: "POST", body: JSON.stringify({ job_id: j.job_id }) });
        toast(r.detail || "已删除", r.ok);
        if (r.ok) {
          closeSlideover();
          if (S.view === "jobs") Jobs.fetchNow();
        }
      } catch (e) { toast("删除失败：" + e.message, false); }
    },
  });
  $("#jd-requeue", inner).onclick = () => confirmModal({
    title: "重新排队",
    body: `将 <code>${esc(j.job_id)}</code> 重排回 QUEUED（RECOVERY_BLOCKED 等状态的解除出口）。运行中的作业不可重排。`,
    confirmText: "确认重排", reason: true,
    onConfirm: rsn => sendCommand("requeue_job", j.job_id, rsn).then(ok => ok && refreshJobDetail()),
  });
  updatePanelFields(j);
  // attempt 块（仅此处创建；updateJobPanel 只做 diff 更新，避免重复建块）
  const box = $("#so-attempts", inner);
  if (!atts.length) {
    box.innerHTML = `<div class="empty" style="padding:24px"><p>尚无执行尝试</p></div>`;
    return;
  }
  atts.forEach((a, i) => {
    const remembered = det.attState[a.attempt_id];
    const open = remembered && remembered.open !== undefined
      ? remembered.open
      : (focusAttempt ? a.attempt_id === focusAttempt : i === 0);
    box.appendChild(buildAttBlock(a, open));
  });
  if (focusAttempt) {
    const blk = $(`.att-block[data-att="${CSS.escape(focusAttempt)}"]`, inner);
    if (blk) blk.scrollIntoView({ block: "center", behavior: "smooth" });
  }
  const first = focusAttempt || atts[0].attempt_id;
  loadAttemptTimeline(first, 40);
  loadAttemptLog(first);
}

/* ---------- 面板字段更新（build 与 update 共用） ---------- */
function updatePanelFields(j) {
  const inner = $("#slideover-inner");
  $("#jd-badge", inner).innerHTML = badge(j.status);
  $("#jd-statusline", inner).innerHTML = `${badge(j.status)} <span class="dim">${esc(j.reason || "")}</span>`;
  $("#jd-live", inner).hidden = !LIVE.has(j.status);
  $("#jd-started", inner).textContent = (LIVE.has(j.status) || ATTEMPT_ENDED(j.status)) ? fmt.dateTime(j.started_at) : "—";
  $("#jd-finished", inner).textContent = ATTEMPT_ENDED(j.status) ? fmt.dateTime(j.finished_at) : "—";
  $("#jd-statusseq", inner).textContent = fmt.num(j.status_seq);
  $("#jd-cancel", inner).hidden = TERMINAL.has(j.status);
  $("#jd-requeue", inner).hidden = !REQUEUEABLE(j.status);
  $("#jd-delete", inner).hidden = LIVE.has(j.status);
}

/* ---------- 局部更新（8s 自动刷新路径，不重建 DOM → 无闪烁） ---------- */
function updateJobPanel(data) {
  const det = S.detail;
  const j = data.job, atts = data.attempts || [];
  const inner = $("#slideover-inner");
  if (!det || !$("#jd-badge", inner)) return;
  updatePanelFields(j);
  // attempt 列表 diff
  const box = $("#so-attempts", inner);
  if (!atts.length) {
    box.innerHTML = `<div class="empty" style="padding:24px"><p>尚无执行尝试</p></div>`;
    return;
  }
  if (!$(".att-block", box)) box.innerHTML = "";
  const existing = new Map($$(".att-block", box).map(b => [b.dataset.att, b]));
  const want = new Set(atts.map(a => a.attempt_id));
  existing.forEach((blk, aid) => { if (!want.has(aid)) blk.remove(); });
  const res = (S.overview && S.overview.snapshot && S.overview.snapshot.attempt_resources) || {};
  atts.forEach(a => {
    let blk = existing.get(a.attempt_id);
    if (!blk) {
      blk = buildAttBlock(a, false);   // 新尝试：折叠插入（seq 降序 → 顶部）
      const first = box.firstElementChild;
      if (first) box.insertBefore(blk, first); else box.appendChild(blk);
    } else {
      updateAttBlockHead(blk, a, res[a.attempt_id]);
    }
  });
  $("#jd-attn", inner).textContent = atts.length;
  // 展开中的块：增量刷新时间线与日志
  atts.forEach(a => {
    const st = det.attState[a.attempt_id];
    if (st && st.open) {
      loadAttemptTimeline(a.attempt_id, det._tlLimit[a.attempt_id] || 40, true);
      loadAttemptLog(a.attempt_id);
    }
  });
}

/* ---------- attempt 块 ---------- */
function buildAttBlock(a, open) {
  const det = S.detail;
  det.attState[a.attempt_id] = Object.assign({ open: true }, det.attState[a.attempt_id], { open });
  const blk = el(`<div class="att-block ${open ? "open" : ""}" data-att="${esc(a.attempt_id)}">
    <div class="att-block-head">
      <span data-f="badge"></span>
      <span class="att-id">${esc(a.attempt_id)}</span>
      <span data-f="res"></span>
      <span class="att-meta">
        <span data-f="exit"></span>
        <span data-f="pid"></span>
        <span data-f="time"></span>
      </span>
      <span class="att-copy-slot"></span>
      <svg class="ic" style="width:13px;height:13px;color:var(--text-3)" viewBox="0 0 24 24"><path d="m6 9 6 6 6-6" stroke-linecap="round"/></svg>
    </div>
    <div class="att-block-body">
      <div class="tl" data-tl="${esc(a.attempt_id)}">
        <div class="tl-filter">${[["all", "全部"], ["progress", "进展"], ["error", "错误"],
          ["done", "完成"], ["ack", "控制"]].map(([k, cn]) =>
          `<button data-tlf="${k}">${cn}</button>`).join("")}</div>
        <div class="tl-list"></div>
        <div class="tl-foot"></div>
      </div>
      <div class="att-log" data-log="${esc(a.attempt_id)}">
        <div class="att-log-head">
          <span class="att-log-title">执行日志 · worker.log（末尾 200 行）</span>
          <span style="display:flex;gap:12px;align-items:center">
            <label class="log-follow" title="自动刷新时滚动到最新输出"><input type="checkbox" data-follow="${esc(a.attempt_id)}" ${det.follow[a.attempt_id] ? "checked" : ""}>跟随</label>
            <button class="btn ghost sm" data-refresh-log>刷新</button>
          </span>
        </div>
        <pre class="att-log-body">加载中…</pre>
      </div>
    </div>
  </div>`);
  const res = (S.overview && S.overview.snapshot && S.overview.snapshot.attempt_resources) || {};
  updateAttBlockHead(blk, a, res[a.attempt_id]);
  $(".att-copy-slot", blk).appendChild(copyBtn(a.attempt_id, "复制 attempt ID"));
  const aid = a.attempt_id;
  $(".att-block-head", blk).onclick = async () => {
    const willOpen = !blk.classList.contains("open");
    blk.classList.toggle("open", willOpen);
    det.attState[aid] = Object.assign({ open: true }, det.attState[aid], { open: willOpen });
    if (willOpen) await Promise.all([loadAttemptTimeline(aid, 40), loadAttemptLog(aid)]);
  };
  $("[data-refresh-log]", blk).onclick = async e => {
    e.stopPropagation();
    await loadAttemptLog(aid);
  };
  const fw = $("[data-follow]", blk);
  fw.onclick = e => e.stopPropagation();
  fw.onchange = () => { det.follow[aid] = fw.checked; if (fw.checked) loadAttemptLog(aid); };
  $$(".tl-filter [data-tlf]", blk).forEach(b => b.onclick = e => {
    e.stopPropagation();
    det.tlFilter[aid] = b.dataset.tlf;
    renderTimeline(aid, false);
  });
  return blk;
}

function updateAttBlockHead(blk, a, r) {
  const head = $(".att-block-head", blk);
  $("[data-f=badge]", head).innerHTML = badge(a.status);
  $("[data-f=exit]", head).textContent = a.exit_code != null ? `exit ${a.exit_code}` : "";
  $("[data-f=pid]", head).textContent = a.pid ? `PID ${a.pid}` : "";
  $("[data-f=time]", head).textContent = fmt.time(a.started_at);
  $("[data-f=res]", head).innerHTML = r
    ? `<span class="res-bar ${r.cpu_pct > 60 ? "hot" : ""}" title="CPU ${r.cpu_pct}% · 内存 ${r.mem_mb}MB"><span class="rb"><i style="width:${Math.min(100, r.cpu_pct)}%"></i></span>${r.cpu_pct}%</span>` : "";
}

/* ---------- 时间线（diff 模式只 prepend 新事件，不重绘已有项） ---------- */
async function loadAttemptTimeline(attemptId, limit, diff) {
  const det = S.detail;
  if (!det) return;
  const tl = $(`[data-tl="${CSS.escape(attemptId)}"]`);
  if (!tl) return;
  const cached = det._evCache[attemptId];
  if (!cached || (det._tlLimit[attemptId] || 0) < limit) {
    let data;
    try {
      data = await api(`/api/attempts/${encodeURIComponent(attemptId)}/events?limit=${limit}`);
    } catch (e) {
      $(".tl-list", tl).innerHTML = `<div class="empty" style="padding:16px"><p>${esc(e.message)}</p></div>`;
      return;
    }
    if (!S.detail || S.detail.jobId !== det.jobId) return;
    det._evCache[attemptId] = data;
    det._tlLimit[attemptId] = limit;
  }
  renderTimeline(attemptId, !!diff);
}

function _tlItemHtml(e, attemptId) {
  const det = S.detail;
  const p = e.payload || {};
  let extra = "";
  if (e.kind === "progress") {
    const parts = [];
    if (p.completed_units != null) parts.push(`已完成 ${p.completed_units} 单元`);
    if (p.pending != null) parts.push(p.pending ? "有待处理" : "无待处理");
    extra = parts.join(" · ");
  } else if (e.kind === "error") extra = p.reason || "";
  else if (e.kind === "ack") extra = p.command || p.kind || "";
  else if (e.kind === "started") {
    const bits = [];
    if (p.pid) bits.push(`PID ${p.pid}`);
    extra = bits.join(" · ");
  }
  const key = attemptId + ":" + e.seq;
  const expanded = det.openPayloads.has(key);
  return `<div class="tl-item k-${esc(e.kind)} expandable" data-key="${esc(key)}" data-seq="${e.seq}">
    <span class="tl-dot"></span>
    <span class="tl-kind">${esc(EVENT_CN[e.kind] || e.kind)} <span class="dim">#${e.seq}</span></span>
    ${extra ? `<span class="tl-payload">${esc(extra)}</span>` : "<span></span>"}
    <span class="tl-at">${fmt.time(e.at)}</span>
    ${expanded ? `<div class="tl-payload-json">${esc(JSON.stringify(p, null, 1))}</div>` : ""}
  </div>`;
}

function _findEvent(key) {
  const det = S.detail;
  if (!det) return null;
  const idx = key.lastIndexOf(":");
  const aid = key.slice(0, idx), seq = key.slice(idx + 1);
  const data = (det._evCache || {})[aid];
  if (!data) return null;
  return (data.events || []).find(e => String(e.seq) === seq);
}

function _bindTlItems(scope) {
  $$(".tl-item.expandable", scope).forEach(item => {
    if (item.dataset.bound) return;
    item.dataset.bound = "1";
    item.onclick = () => {
      const det = S.detail;
      if (!det) return;
      const key = item.dataset.key;
      let p = item.querySelector(".tl-payload-json");
      if (det.openPayloads.has(key)) {
        det.openPayloads.delete(key);
        if (p) p.remove();
      } else {
        det.openPayloads.add(key);
        if (!p) {
          const ev = _findEvent(key);
          item.appendChild(el(`<div class="tl-payload-json">${esc(JSON.stringify((ev && ev.payload) || {}, null, 1))}</div>`));
        }
      }
    };
  });
}

function renderTimeline(attemptId, diff) {
  const det = S.detail;
  if (!det) return;
  const tl = $(`[data-tl="${CSS.escape(attemptId)}"]`);
  if (!tl) return;
  const data = (det._evCache || {})[attemptId];
  if (!data) return;
  const list = $(".tl-list", tl);
  const foot = $(".tl-foot", tl);
  const filter = det.tlFilter[attemptId] || "all";
  $$(".tl-filter [data-tlf]", tl).forEach(b => b.classList.toggle("active", b.dataset.tlf === filter));
  const pass = e => filter === "all" || e.kind === filter;
  const evs = data.events || [];
  const cps = data.checkpoints || [];
  const maxSeq = det._tlMax[attemptId] || 0;
  if (diff && list.children.length) {
    // 增量：只插入比 maxSeq 新的事件（倒序插入 → 最新在顶）
    const fresh = evs.filter(e => e.seq > maxSeq && pass(e));
    if (fresh.length) {
      fresh.slice().reverse().forEach(e => {
        list.insertAdjacentHTML("afterbegin", _tlItemHtml(e, attemptId));
      });
      _bindTlItems(list);
    }
    det._tlMax[attemptId] = evs.length ? Math.max(maxSeq, ...evs.map(e => e.seq)) : maxSeq;
  } else {
    const shown = evs.filter(pass);
    list.innerHTML = shown.length ? shown.map(e => _tlItemHtml(e, attemptId)).join("")
      : `<div class="empty" style="padding:14px"><p>${filter === "all"
          ? "暂无入库事件<br><span style='font-size:10.5px'>心跳仅内存跟踪；进展 / 外部不足 / 检查点 / 完成事件将在此显示</span>"
          : "该类型暂无事件"}</p></div>`;
    _bindTlItems(list);
    det._tlMax[attemptId] = evs.length ? Math.max(...evs.map(e => e.seq)) : 0;
  }
  // 底部：加载更早 + 检查点摘要
  const total = evs.length;
  const limit = det._tlLimit[attemptId] || 40;
  foot.innerHTML = `${total >= limit ? `<div class="tl-more"><button data-more>加载更早事件（共 ${total}+）</button></div>` : ""}`
    + `${cps.length ? `<div class="tl-more dim" style="font-size:10.5px">检查点 ${cps.length} 个 · 最新提交 ${fmt.time(cps[0].at)}${cps[0].ref ? " · " + esc(cps[0].ref) : ""}</div>` : ""}`;
  const more = $("[data-more]", foot);
  if (more) more.onclick = () => loadAttemptTimeline(attemptId, limit * 3);
}

/* ---------- 日志（内容不变则跳过 → 保留滚动位置） ---------- */
async function loadAttemptLog(attemptId) {
  const det = S.detail;
  if (!det) return;
  const box = $(`[data-log="${CSS.escape(attemptId)}"]`);
  if (!box) return;
  const pre = $(".att-log-body", box);
  try {
    const d = await api(`/api/attempts/${encodeURIComponent(attemptId)}/log?tail=200`);
    if (d.error) { pre.textContent = d.error; return; }
    if (!d.exists) { pre.textContent = d.note || "无日志文件"; return; }
    const text = d.lines.length ? d.lines.join("\n") : "（日志为空）";
    if (pre.dataset.sig === text) return;   // 内容未变：不动 DOM
    pre.textContent = text;
    pre.dataset.sig = text;
    if (det.follow[attemptId]) pre.scrollTop = pre.scrollHeight;
  } catch (e) {
    pre.textContent = "日志加载失败：" + e.message;
  }
}


/* ================================================================
   详情：任务（含最近作业内嵌）
   ================================================================ */
function openTaskDetail(taskId) {
  const t = (S.tasksCache || []).find(x => x.task_id === taskId);
  const inner = $("#slideover-inner");
  if (!t) { inner.innerHTML = `<div class="empty"><p>任务未找到：${esc(taskId)}</p></div>`; openSlideover(); return; }
  const m = t.monitor || {}, sc = t.scale || {}, p = m.producer || {};
  const health = m.health || [];
  const cursor = m.cursor && !m.cursor._truncated ? JSON.stringify(m.cursor) : (m.cursor && m.cursor._truncated ? "（已截断，共 " + m.cursor.size + " 字节）" : "—");
  inner.innerHTML = `
    <div class="so-head">
      <div>
        <div style="margin-bottom:8px;display:flex;gap:8px">
          ${t.enabled ? '<span class="badge s-RUNNING"><span class="bdot"></span>启用</span>' : '<span class="badge s-CANCELLED"><span class="bdot"></span>停用</span>'}
          <span class="pri-chip"><span class="bar"></span>优先级 ${t.priority}</span>
        </div>
        <div class="so-title">${esc(t.task_id)}</div>
        <div class="dim" style="font-size:11.5px;margin-top:4px">授权作业优先级上限 ${t.job_priority_cap ?? t.priority}</div>
      </div>
      <button class="so-close" title="关闭 (Esc)"><svg class="ic" viewBox="0 0 24 24"><path d="M6 6l12 12M18 6 6 18" stroke-linecap="round"/></svg></button>
    </div>
    <div class="so-actions">
      <button class="btn sm ${t.enabled ? "danger" : "primary"}" data-act="toggle">${t.enabled ? "禁用任务" : "启用任务"}</button>
      <button class="btn danger sm" id="td-stop" ${(m.live || 0) > 0 ? "" : "hidden"}>停止全部执行（${m.live || 0}）</button>
      <button class="btn danger sm" id="td-delete">删除任务</button>
      <a class="btn ghost sm" href="#/jobs" id="so-view-jobs">查看该任务作业</a>
    </div>
    <div class="so-section">
      <div class="so-section-title">运行视图</div>
      <dl class="dl">
        <dt>存活 / 动态目标</dt><dd class="plain">${fmt.num(m.live || 0)} / ${fmt.num(sc.run_target ?? "—")}</dd>
        <dt>运行模式</dt><dd class="plain">${sc.mode === "explore" ? "冷启动探索" : sc.mode === "steady" ? "稳定运行" : "—"}</dd>
        <dt>等待中 / 需处理</dt><dd>${fmt.num(m.waiting ?? m.queued ?? 0)} / ${fmt.num(m.attention || 0)}</dd>
        <dt>排队作业</dt><dd>${fmt.num(m.queued || 0)}</dd>
        <dt>未确认发布</dt><dd>${fmt.num(m.unacked || 0)}${m.conflicted ? ` · 内容冲突 ${m.conflicted}` : ""}</dd>
        <dt>发现游标</dt><dd style="max-height:80px;overflow:auto">${esc(cursor)}</dd>
        ${m.backoff_reason ? `<dt>退避原因</dt><dd class="plain" style="color:var(--c-backoff)">${esc(m.backoff_reason)}</dd>` : ""}
        ${m.next_retry_at ? `<dt>下次重试</dt><dd>${fmt.dateTime(m.next_retry_at)}</dd>` : ""}
      </dl>
    </div>
    ${health ? `
    <div class="so-section">
      <div class="so-section-title">健康基线</div>
      <div class="stat-item" style="margin-bottom:8px">
        <div style="display:flex;justify-content:space-between;align-items:baseline">
          <span class="s-value" style="font-size:13px;${health.slow ? "color:var(--c-starting)" : ""}">${(health.rate_per_sec || 0).toFixed(3)} <small>单元/s</small></span>
        </div>
        <div class="dim" style="font-size:10.5px;margin-top:2px">样本 ${health.samples} · 累计完成 ${fmt.num(health.completed_units)} 单元${health.slow ? " · 判定变慢" : ""}</div>
      </div>
    </div>` : ""}
    <div class="so-section">
      <div class="so-section-title">生产者</div>
      <dl class="dl">
        <dt>状态</dt><dd class="plain">${p.pid ? (p.alive ? "常驻运行中" : "已退出（等待守护重启）") : "未运行"}</dd>
        <dt>PID</dt><dd>${p.pid || "—"}</dd>
        <dt>累计崩溃</dt><dd>${fmt.num(p.crash_count || 0)}${p.last_crash_wall ? ` · 最近 ${fmt.rel(p.last_crash_wall)}` : ""}</dd>
      </dl>
    </div>
    <div class="so-section">
      <div class="so-section-title">最近作业</div>
      <div class="mini-jobs" id="task-mini-jobs"><div class="skeleton-row"></div></div>
    </div>
    <div class="so-section">
      <div class="so-section-title">入口契约</div>
      <dl class="dl">
        <dt>生产者入口</dt><dd>${esc(t.producer_entry.interpreter)} ${esc(t.producer_entry.file)} ${esc((t.producer_entry.args || []).join(" "))}</dd>
        <dt>执行入口</dt><dd>${esc(t.executor_entry.interpreter)} ${esc(t.executor_entry.file)} ${esc((t.executor_entry.args || []).join(" "))}</dd>
        <dt>恢复方式</dt><dd class="plain">${esc(t.recovery.replay)} · 保留：${esc(t.recovery.retain || "—")}</dd>
        <dt>遗漏识别</dt><dd class="plain">${esc(t.recovery.replay_note || "—")}</dd>
      </dl>
    </div>`;
  openSlideover();
  $(".so-close", inner).onclick = closeSlideover;
  $("#so-view-jobs", inner).onclick = () => {
    S.jobsFilter = { status: "", task: taskId, q: "", sort: "recent", page: 1, limit: 50 };
    closeSlideover();
  };
  // 超级管理：停止该任务全部存活执行（协作暂停 + 进程树回收）
  const stopBtn = $("#td-stop", inner);
  if (stopBtn) stopBtn.onclick = () => confirmModal({
    title: "停止全部执行",
    body: `协作暂停 <b>${m.live || 0}</b> 个存活执行（检查点保留）。作业回到<b>排队</b>状态：任务保持禁用时不会被启动；<b>重新启用后自动从检查点续跑</b>，无需人工重排。`,
    confirmText: `停止 ${m.live || 0} 个执行`, danger: true, reason: true,
    onConfirm: async rsn => {
      try {
        const r = await api("/api/task", { method: "POST", body: JSON.stringify({ task_id: taskId, action: "stop" }) });
        toast(r.detail || "已停止", r.ok);
        refreshTasks().then(() => openTaskDetail(taskId));
      } catch (e) { toast("停止失败：" + e.message, false); }
    },
  });
  // 完全删除任务（双重确认：输入任务 ID）
  const delBtn = $("#td-delete", inner);
  if (delBtn) delBtn.onclick = () => {
    const m2 = $("#modal");
    m2.dataset.closable = "1";
    m2.innerHTML = `
      <h3>删除任务</h3>
      <div class="m-body">
        将<b>完全删除</b>任务 <code>${esc(taskId)}</code>，等同从未创建：
        <ul style="margin:8px 0 8px 18px;padding:0;color:var(--text-2);font-size:12px">
          <li>停止全部存活执行并终止生产者进程</li>
          <li>删除任务目录（代码 / 配置 / outbox / producer_state / receipts）</li>
          <li>清除数据库全部相关行（作业 / 尝试 / 事件 / 检查点 / 运行目标）</li>
          <li>删除运行工件与归档（worker.log / 请求 JSON / 隔离区）</li>
        </ul>
        <b style="color:var(--c-failed)">不可恢复。</b>输入任务 ID 确认：
        <div class="m-reason"><input class="input" id="td-confirm-input" type="text" placeholder="${esc(taskId)}" autocomplete="off"></div>
      </div>
      <div class="m-actions">
        <button class="btn ghost" data-act="cancel">取消</button>
        <button class="btn danger" data-act="ok" disabled>永久删除</button>
      </div>`;
    $("#modal-backdrop").hidden = false;
    const input = $("#td-confirm-input"), okBtn = $("[data-act=ok]", m2);
    input.addEventListener("input", () => { okBtn.disabled = input.value.trim() !== taskId; });
    $("[data-act=cancel]", m2).onclick = closeModal;
    okBtn.onclick = async () => {
      closeModal();
      const pm = progressModal(`删除任务 ${taskId}`);
      try {
        if ((m.live || 0) > 0) {
          pm.update(0, 2, 0, 0);
          await api("/api/task", { method: "POST", body: JSON.stringify({ task_id: taskId, action: "stop" }) });
          pm.update(1, 2, 0, 0);
        }
        const r = await api("/api/task", { method: "POST", body: JSON.stringify({ task_id: taskId, action: "delete" }) });
        if (r.ok) {
          pm.done(1, 0, []);
          toast(r.detail || "任务已删除");
          closeSlideover();
          await refreshTasks();
          location.hash = "#/overview";
        } else {
          pm.fail(r.detail || "删除失败");
        }
      } catch (e) {
        pm.fail("删除失败：" + e.message);
      }
    };
    setTimeout(() => input.focus(), 40);
  };
  $("[data-act=toggle]", inner).onclick = () => {
    const action = t.enabled ? "disable" : "enable";
    confirmModal({
      title: action === "disable" ? "禁用任务" : "启用任务",
      body: action === "disable"
        ? `禁用 <code>${esc(taskId)}</code> 后不再产生新的执行准入；已运行执行与生产者按协议处理。`
        : `启用 <code>${esc(taskId)}</code>：将校验 task.json 合规性后置 enabled=true。`,
      confirmText: action === "disable" ? "确认禁用" : "确认启用",
      danger: action === "disable",
      onConfirm: async () => {
        try {
          const r = await api("/api/task", { method: "POST", body: JSON.stringify({ task_id: taskId, action }) });
          toast(r.detail || "操作成功", r.ok);
          if (r.ok) refreshTasks().then(() => openTaskDetail(taskId));
        } catch (e) { toast("操作失败：" + e.message, false); }
      },
    });
  };
  // 内嵌最近作业
  api(`/api/jobs?task=${encodeURIComponent(taskId)}&limit=10`).then(d => {
    const box = $("#task-mini-jobs");
    if (!box) return;
    if (!d.items.length) { box.innerHTML = `<div class="dim" style="font-size:11.5px;padding:6px 2px">暂无作业记录</div>`; return; }
    box.innerHTML = d.items.map(j => `
      <div class="mini-job" data-job="${esc(j.job_id)}">
        ${badge(j.status)}
        <span class="mj-id">${esc(j.job_id)}</span>
        <span class="mj-time">${fmt.rel(j.finished_at || j.started_at || j.queued_at)}</span>
      </div>`).join("");
    $$(".mini-job", box).forEach(mj => mj.onclick = () => openJobDetail(mj.dataset.job, { source: d.items.map(x => x.job_id) }));
  }).catch(() => {});
}

/* ================================================================
   行绑定（键盘可达 + j/k 游标）
   ================================================================ */
function bindRows(rows, onClick) {
  rows.forEach(tr => {
    tr.onclick = () => onClick(tr);
    tr.onkeydown = e => { if (e.key === "Enter") { e.preventDefault(); onClick(tr); } };
  });
}
const Cursor = {
  rows: [], idx: -1,
  set(rows) { this.rows = rows; this.idx = -1; this.clear(); },
  clear() { $$(".rowcursor").forEach(r => r.classList.remove("rowcursor")); },
  move(d) {
    if (!this.rows.length) return;
    this.idx = Math.min(this.rows.length - 1, Math.max(0, this.idx + d));
    const tr = this.rows[this.idx];
    this.clear();
    if (tr && tr.isConnected) {
      tr.classList.add("rowcursor");
      tr.scrollIntoView({ block: "nearest" });
    } else this.rows = this.rows.filter(r => r.isConnected);
  },
  open() {
    const tr = this.rows[this.idx];
    if (tr && tr.isConnected) tr.click();
  },
};

/* ================================================================
   路由（含筛选参数 / 深链接）
   ================================================================ */
const VIEWS = {
  overview: { title: "总览", sub: "运行 · 等待 · 完成 · 需处理", mod: Overview },
  tasks: { title: "任务", sub: "每个任务的运行与等待情况", mod: Tasks },
  jobs: { title: "作业", sub: "全部作业 · 筛选 · 批量操作", mod: Jobs },
  attempts: { title: "实时执行", sub: "正在运行的执行过程与资源占用", mod: Attempts },
};

function clearTimers() {
  S.viewTimers.forEach(clearInterval);
  S.viewTimers = [];
}

function route() {
  const hash = location.hash || "#/overview";
  const m = hash.replace(/^#\//, "");
  const [seg, queryStr] = m.split("?");
  const parts = seg.split("/").filter(Boolean);
  let name = parts[0] || "overview";
  const deep = parts[1] ? decodeURIComponent(parts[1]) : null;
  if (!VIEWS[name]) name = "overview";
  const q = {};
  if (queryStr) new URLSearchParams(queryStr).forEach((v, k) => q[k] = v);
  clearTimers();
  S.view = name;
  // route() 只在真实导航（视图切换/前进后退/深链接）时触发；筛选与翻页走
  // replaceState 不经过这里 → 无条件清空批量选择，翻页选择天然保留
  S.jobsSel.clear();
  $$(".nav-item").forEach(a => a.classList.toggle("active", a.dataset.view === name));
  const v = VIEWS[name];
  $("#page-title").textContent = v.title;
  $("#page-sub").textContent = v.sub;
  if (name === "jobs" && !deep) {
    // URL 筛选参数 → 过滤器
    const f = S.jobsFilter;
    f.status = q.status && (STATUS_LIST.includes(q.status) || GROUP_STATES[q.status]) ? q.status : "";
    f.task = q.task || "";
    f.q = q.q || "";
    f.sort = ["recent", "oldest", "priority", "priority_asc"].includes(q.sort) ? q.sort : "recent";
    f.page = Math.max(1, parseInt(q.page) || 1);
    f.limit = [50, 100, 200].includes(parseInt(q.limit)) ? parseInt(q.limit) : 50;
  }
  v.mod.render($("#view"));
  if (name === "overview") {
    S.viewTimers.push(setInterval(() => Overview.refresh(), 2000));
    S.viewTimers.push(setInterval(() => Overview.refresh(), 10000));
    S.viewTimers.push(setInterval(() => { loadMetrics(); }, 60000));
  }
  if (name === "tasks") S.viewTimers.push(setInterval(() => { refreshTasks().then(() => Tasks.refresh()); }, 5000));
  if (name === "jobs") S.viewTimers.push(setInterval(() => {
    if (Jobs.loading) return;
    if (S.jobsSel.size) return;                                  // 批量选择中不重绘
    if ($("#slideover").classList.contains("open")) return;      // 详情打开中不重绘
    if (window.getSelection && window.getSelection().toString()) return;  // 选字中
    const qi = $("#job-q");
    if (qi && qi.value) return;                                  // 搜索输入中
    const pager = $("#job-pager");
    if (pager && pager.contains(document.activeElement)) return; // 跳页输入中
    Jobs.fetchNow();
  }, 6000));
  if (name === "attempts") S.viewTimers.push(setInterval(() => Attempts.fetchNow(), 3000));
  if (name === "jobs" && deep) openJobDetail(deep);
  if (name === "tasks" && deep) openTaskDetail(deep);
  Cursor.set([]);
}

window.addEventListener("hashchange", route);

/* ================================================================
   overview 数据处理（SSE / 轮询共用）
   ================================================================ */
function handleOverview(o) {
  S.overview = o;
  S.tz = o.timezone || "Asia/Shanghai";
  const snap = o.snapshot;
  if (snap) {
    const cur = { t: Date.now(), cpu: snap.exec_cpu_usec, mem: snap.exec_mem_mb, io: snap.exec_io_bytes, queued: snap.queued, live: snap.live_attempts };
    const last = S.lastResource;
    if (last && cur.t > last.t) {
      const dt = (cur.t - last.t) / 1000;
      const jc = o.job_counts || {};
      S.history.push({
        t: cur.t, queued: cur.queued, live: cur.live, memMb: cur.mem,
        cpuPct: Math.max(0, (cur.cpu - last.cpu) / 1e6 / dt / o.machine.cpu_count * 100),
        ioBps: Math.max(0, (cur.io - last.io) / dt),
        jRunning: (jc.STARTING || 0) + (jc.RUNNING || 0) + (jc.PAUSING || 0),
        jWaiting: (jc.QUEUED || 0) + (jc.BACKOFF || 0) + (jc.PAUSED || 0) + (jc.RECOVERING || 0),
        jFinished: (jc.SUCCEEDED || 0) + (jc.CANCELLED || 0),
        jAttention: (jc.FAILED || 0) + (jc.RECOVERY_BLOCKED || 0),
        producersAlive: snap.producers ? snap.producers.filter(p => p.alive).length : null,
      });
      if (S.history.length > HIST_MAX) S.history.shift();
    }
    S.lastResource = cur;
  }
  // serve 状态
  const pill = $("#serve-pill");
  if (o.serve_alive) { pill.className = "serve-pill ok"; $(".serve-label", pill).textContent = "serve 运行中"; }
  else { pill.className = "serve-pill down"; $(".serve-label", pill).textContent = "serve 未运行"; }
  $("#brand-version").textContent = "v" + o.version;
  const live = snap ? snap.live_attempts : 0;
  const nb = $("#nav-live-badge");
  nb.textContent = fmt.num(live);
  nb.className = "nav-badge" + (live > 0 ? " live" : "");
  $("#nav-tasks-badge").textContent = fmt.num(Object.keys(o.task_specs || {}).length);
  S.fresh.overview = Date.now();
  // 标签页徽标
  const c = o.job_counts || {};
  const producersDown = snap ? snap.producers.filter(p => !p.alive).length : 0;
  const abnormal = (c.FAILED || 0) + (c.RECOVERY_BLOCKED || 0) + producersDown;
  document.title = (abnormal > 0 ? `(${abnormal} 异常) ` : "") + "Sched · 调度器控制台";
  if (S.view === "overview") Overview.refresh();
  if (S.view === "jobs") {
    const counts = o.job_counts || {};
    S.jobCounts = counts;
    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    $$("#job-pills .pill").forEach(p => {
      const cEl = $(".p-count", p);
      if (!cEl) return;
      const st = p.dataset.st;
      if (!st) { cEl.textContent = fmt.num(total); return; }
      if (GROUP_STATES[st]) {  // 大类：聚合计数
        cEl.textContent = fmt.num(GROUP_STATES[st].reduce((a, s) => a + (counts[s] || 0), 0));
      } else {                 // 细分：单状态计数
        cEl.textContent = fmt.num(counts[st] || 0);
      }
    });
  }
}

/* ---------------- SSE（失败自动降级轮询） ---------------- */
function connectSSE() {
  if (S.polling) return;
  try {
    const tk = getToken();
    const url = "/api/stream" + (tk ? "?token=" + encodeURIComponent(tk) : "");
    const es = new EventSource(url);
    es.addEventListener("overview", e => {
      S.sseFailed = 0;
      try { handleOverview(JSON.parse(e.data)); } catch (err) { /* 忽略坏帧 */ }
    });
    es.onopen = () => { S.sseFailed = 0; };
    es.onerror = () => {
      S.sseFailed++;
      if (S.sseFailed >= 4) {
        es.close();
        S.sse = null;
        startPolling();  // 降级：SSE 不可用（代理剥离 / 服务端旧版）
      }
    };
    S.sse = es;
  } catch (e) { startPolling(); }
}

function startPolling() {
  if (S.polling) return;
  S.polling = true;
  pollOverview();
  setInterval(pollOverview, 2000);
}

async function pollOverview() {
  if (document.hidden) return;
  try {
    handleOverview(await api("/api/overview"));
  } catch (e) {
    if (e.message === "unauthorized") return;
    const pill = $("#serve-pill");
    pill.className = "serve-pill down";
    $(".serve-label", pill).textContent = "连接断开";
  }
}

/* ---------------- tasks 轮询 ---------------- */
let tasksFetching = false;
async function refreshTasks() {
  if (tasksFetching) return;
  tasksFetching = true;
  try {
    const r = await api("/api/tasks");
    S.tasksCache = r.tasks;
    S.fresh.tasks = Date.now();
  } catch (e) { /* 静默，下轮重试 */ }
  tasksFetching = false;
}

/* ---------------- 时钟 / 新鲜度 ---------------- */
setInterval(() => {
  $("#clock").textContent = new Date().toLocaleTimeString("zh-CN", { timeZone: S.tz, hour12: false })
    + " " + S.tz.split("/").pop();
}, 1000);

const FRESH_STALE_MS = { overview: 8000, tasks: 20000, jobs: 15000, attempts: 9000 };
setInterval(() => {
  $$(".fresh").forEach(f => {
    const key = f.dataset.fresh;
    const ts = S.fresh[key];
    const text = $(".f-text", f);
    if (!ts) { text.textContent = "加载中…"; f.classList.remove("stale"); return; }
    const age = Date.now() - ts;
    text.textContent = "数据截至 " + new Date(ts).toLocaleTimeString("zh-CN", { hour12: false });
    f.classList.toggle("stale", age > (FRESH_STALE_MS[key] || 15000));
  });
}, 1000);

/* ---------------- Esc：关闭弹窗 / 详情面板（唯一保留的快捷键） ---------------- */
document.addEventListener("keydown", e => {
  if (e.key === "Escape") {
    if (!$("#modal-backdrop").hidden) closeModal();
    else if (!$("#login-overlay").hidden) { /* 登录层不关 */ }
    else closeSlideover();
  }
});

function collectCursor() {
  let rows = [];
  if (S.view === "jobs") rows = $$("#job-rows tr[data-job]");
  else if (S.view === "tasks") rows = $$("#task-rows tr[data-task]");
  else if (S.view === "attempts") rows = $$("#att-rows [data-job]");
  else if (S.view === "overview") rows = $$("#ov-top-tasks tr[data-task]");
  Cursor.set(rows);
}

/* ---------------- 主题 ---------------- */
function applyTheme(t) {
  document.documentElement.setAttribute("data-theme", t === "light" ? "light" : "dark");
}

applyTheme(localStorage.getItem("sched-theme") || "dark");
$("#theme-toggle").onclick = () => {
  const cur = document.documentElement.getAttribute("data-theme") === "light" ? "light" : "dark";
  const next = cur === "light" ? "dark" : "light";
  localStorage.setItem("sched-theme", next);
  applyTheme(next);
  toast(`已切换到${next === "light" ? "浅色" : "深色"}主题`);
};

/* ---------------- 启动 ---------------- */
(async function init() {
  await Promise.all([pollOverview(), refreshTasks()]);
  route();
  connectSSE();
  setInterval(refreshTasks, 8000);
})();
