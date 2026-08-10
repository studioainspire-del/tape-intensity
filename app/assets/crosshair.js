/*
 * Crosshair overlay for the tape-intensity chart.
 *
 * Draws a vertical line spanning ALL panels (continuous through the
 * gaps between them) plus a horizontal line floating at the cursor's
 * y position inside the hovered panel.
 *
 * Why a DOM overlay instead of Plotly spikes:
 *   - Plotly's x spike can only span the subplot it is drawn in. Panels
 *     are rows of one figure, and `hoversubplots: "axis"` does not draw
 *     spikes across rows (verified against plotly.js 3.7, including the
 *     documented minimal example) — so a continuous line is not
 *     reachable natively.
 *   - The figure is rebuilt up to 5x/sec. Every Plotly.react clears
 *     hover state (see CLAUDE.md gotcha #8), which would make a
 *     spike-based crosshair flicker. A DOM overlay driven by mousemove
 *     is immune, and costs zero server round-trips.
 *
 * Constraints this file must keep:
 *   - pointer-events: none on both lines. They sit over the drag layer;
 *     if they ever swallow events, panning and chart clicks break.
 *   - z-index below the LIVE badge (10) so the badge stays clickable.
 *   - Plot bounds come from Plotly's .nsewdrag rects (the per-panel drag
 *     layers) read via getBoundingClientRect, not from _fullLayout, to
 *     avoid depending on Plotly internals. They are re-read on every
 *     move, so figure rebuilds can't leave stale geometry behind.
 */
(function () {
    "use strict";

    var WRAP_ID = "chart-wrapper";
    var V_ID = "crosshair-v";
    var H_ID = "crosshair-h";
    var COLOR = "rgba(204, 204, 204, 0.55)";

    // Gap between the cursor and the right edge of the hover box.
    var HOVER_GAP = 12;

    // Cursor position, recorded in the capture phase so it is current
    // when Plotly's own mousemove handler redraws the hover box.
    var lastX = null;
    var lastY = null;

    function styleLine(el, vertical) {
        el.style.position = "absolute";
        el.style.pointerEvents = "none";
        el.style.zIndex = "5";
        el.style.display = "none";
        if (vertical) {
            el.style.width = "0";
            el.style.borderLeft = "1px dashed " + COLOR;
        } else {
            el.style.height = "0";
            el.style.borderTop = "1px dashed " + COLOR;
        }
    }

    function ensureLines(wrap) {
        var v = document.getElementById(V_ID);
        var h = document.getElementById(H_ID);
        if (!v) {
            v = document.createElement("div");
            v.id = V_ID;
            styleLine(v, true);
            wrap.appendChild(v);
        }
        if (!h) {
            h = document.createElement("div");
            h.id = H_ID;
            styleLine(h, false);
            wrap.appendChild(h);
        }
        return [v, h];
    }

    function hideLines() {
        [V_ID, H_ID].forEach(function (id) {
            var el = document.getElementById(id);
            if (el) {
                el.style.display = "none";
            }
        });
    }

    /*
     * Move the unified hover box to the LEFT of the cursor.
     *
     * Plotly has no layout attribute for this — layout.hoverlabel only
     * exposes align/bgcolor/bordercolor/font/namelength/showarrow — so
     * the box is nudged in the DOM. Plotly renders the "x unified" box
     * as <g class="legend"> inside .hoverlayer, placing it ~5px right of
     * the cursor, except near the right edge where it already flips
     * left. Because the live edge IS the right edge, a blind shift would
     * double up exactly where the cursor spends most of its time, so we
     * measure Plotly's actual placement each time and set an absolute
     * offset from it.
     *
     * Clearing our own transform before measuring is what keeps this
     * from compounding: every pass starts from Plotly's own position.
     */
    function placeHoverLeft(wrap) {
        var box = wrap.querySelector(".hoverlayer g.legend");
        if (!box || lastX === null) {
            return;
        }
        // A CSS transform REPLACES the SVG transform attribute rather
        // than composing with it, so Plotly's own translate has to be
        // read and carried, not just offset from.
        var attr = box.getAttribute("transform") || "";
        var m = /translate\(\s*(-?[\d.]+)[\s,]+(-?[\d.]+)/.exec(attr);
        if (!m) {
            return;
        }
        var baseX = parseFloat(m[1]);
        var baseY = parseFloat(m[2]);

        // Measure with our offset cleared, so every pass starts from
        // Plotly's own placement and the shift can't compound.
        box.style.transform = "";
        var r = box.getBoundingClientRect();
        if (!r.width) {
            return;
        }
        var dx = (lastX - HOVER_GAP) - r.right;
        // Near the left edge there is no room; keep Plotly's placement
        // rather than pushing the box off the chart.
        if (dx < 0 && r.left + dx > wrap.getBoundingClientRect().left) {
            box.style.transform =
                "translate(" + (baseX + dx) + "px," + baseY + "px)";
        }
    }

    /*
     * Reposition on the next animation frame.
     *
     * rAF runs after Plotly's own mousemove handler has drawn the hover
     * box but before paint, so the box never appears on the wrong side
     * first — no flicker — and the work only happens when the pointer
     * actually moves.
     *
     * This deliberately does NOT use a MutationObserver over the chart:
     * the figure is rebuilt up to 5x/sec, so an observer would fire on
     * every rebuild and force a synchronous layout of the whole
     * (900-point, 8-trace) SVG each time. Measured at ~3.4x the layout
     * and style-recalc count for no benefit — a rebuild CLEARS hover
     * state (gotcha #8) rather than redrawing the box, so there is
     * nothing to correct until the pointer moves again.
     */
    var framePending = false;

    function scheduleHoverPlacement(wrap) {
        if (framePending) {
            return;
        }
        framePending = true;
        requestAnimationFrame(function () {
            framePending = false;
            placeHoverLeft(wrap);
        });
    }

    function attach(wrap) {
        if (wrap.dataset.crosshairOn === "1") {
            return;
        }
        wrap.dataset.crosshairOn = "1";

        // Capture phase: runs before Plotly's own handler, so lastX is
        // current by the time Plotly redraws the hover box.
        wrap.addEventListener("mousemove", function (ev) {
            lastX = ev.clientX;
            lastY = ev.clientY;
        }, true);

        wrap.addEventListener("mousemove", function (ev) {
            var drags = wrap.querySelectorAll(".nsewdrag");
            if (!drags.length) {
                return;
            }

            // Union of the panel plot areas, plus which panel (if any)
            // the cursor is vertically inside.
            var left = Infinity, right = -Infinity;
            var top = Infinity, bottom = -Infinity;
            var inPanel = false;
            for (var i = 0; i < drags.length; i++) {
                var r = drags[i].getBoundingClientRect();
                if (!r.width || !r.height) {
                    continue;
                }
                left = Math.min(left, r.left);
                right = Math.max(right, r.right);
                top = Math.min(top, r.top);
                bottom = Math.max(bottom, r.bottom);
                if (ev.clientY >= r.top && ev.clientY <= r.bottom) {
                    inPanel = true;
                }
            }
            if (left === Infinity) {
                return;
            }

            var lines = ensureLines(wrap);
            var v = lines[0], h = lines[1];
            var wr = wrap.getBoundingClientRect();
            var insideX = ev.clientX >= left && ev.clientX <= right;
            var insideY = ev.clientY >= top && ev.clientY <= bottom;

            if (insideX && insideY) {
                v.style.display = "block";
                v.style.left = (ev.clientX - wr.left) + "px";
                v.style.top = (top - wr.top) + "px";
                v.style.height = (bottom - top) + "px";
            } else {
                v.style.display = "none";
            }

            // Horizontal line only inside a panel, never in the gap
            // between two panels.
            if (insideX && inPanel) {
                h.style.display = "block";
                h.style.top = (ev.clientY - wr.top) + "px";
                h.style.left = (left - wr.left) + "px";
                h.style.width = (right - left) + "px";
            } else {
                h.style.display = "none";
            }

            scheduleHoverPlacement(wrap);
        });

        wrap.addEventListener("mouseleave", hideLines);
    }

    // The wrapper is rendered by Dash after page load, so poll until it
    // exists. attach() is idempotent (dataset flag), so this is cheap.
    setInterval(function () {
        var wrap = document.getElementById(WRAP_ID);
        if (wrap) {
            attach(wrap);
        }
    }, 500);
})();
