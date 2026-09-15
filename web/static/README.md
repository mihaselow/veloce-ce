# Vendored interactive client assets

These files are served same-origin by the Veloce web UI so Interactive Shell / VNC
do not depend on CDN reachability (VPN / enterprise filters).

| Path | Upstream | License |
|------|----------|---------|
| `novnc/` | [noVNC v1.5.0](https://github.com/novnc/noVNC/releases/tag/v1.5.0) (`core/` + `vendor/`) | MPL-2.0 |
| `xterm/` | [xterm.js 5.3.0](https://github.com/xtermjs/xterm.js) + [addon-fit 0.8.0](https://github.com/xtermjs/xterm.js/tree/master/addons/addon-fit) | MIT |

Do not replace with CDN URLs in `index.html` session open paths.
