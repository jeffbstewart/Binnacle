package api

import (
	"database/sql"
	"html/template"
	"net/http"
	"time"

	"github.com/jeffbstewart/Binnacle/internal/store"
)

// ViewerPath is the canonical path for the human-facing log viewer.
const ViewerPath = "/logs"

// ViewerHandler renders an HTML log viewer. It reuses the same
// parseQueryFilter + store.Query pipeline that the JSON API uses,
// then renders the result via an embedded html/template.
type ViewerHandler struct {
	DB *sql.DB
}

// viewerData is the template context.
type viewerData struct {
	Records  []store.QueriedRecord
	Count    int
	Services []string // for the filter dropdown

	// Active filters — typed so the template doesn't traffic in
	// format-specific strings.
	FilterService   string
	FilterSeverity  string
	Since           time.Time
	Until           time.Time
	FilterSearch    string
	FilterLimit     string
	RefreshInterval time.Duration // zero = off

	// Presets for the time-range quick links.
	Presets []preset

	Error string // non-empty on parse/query failure
}

type preset struct {
	Label string
	Since time.Time
}

func (h *ViewerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var refreshInterval time.Duration
	if s := r.URL.Query().Get("refresh"); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d >= 5*time.Second {
			refreshInterval = d
		}
	}

	var since, until time.Time
	if s := r.URL.Query().Get("since"); s != "" {
		since, _ = time.Parse(time.RFC3339, s)
	}
	if s := r.URL.Query().Get("until"); s != "" {
		until, _ = time.Parse(time.RFC3339, s)
	}

	data := viewerData{
		FilterService:   r.URL.Query().Get("service"),
		FilterSeverity:  r.URL.Query().Get("severity"),
		Since:           since,
		Until:           until,
		FilterSearch:    r.URL.Query().Get("q"),
		FilterLimit:     r.URL.Query().Get("limit"),
		RefreshInterval: refreshInterval,
	}

	// Time-range presets.
	now := time.Now().UTC()
	data.Presets = []preset{
		{"15m", now.Add(-15 * time.Minute)},
		{"1h", now.Add(-1 * time.Hour)},
		{"6h", now.Add(-6 * time.Hour)},
		{"24h", now.Add(-24 * time.Hour)},
		{"7d", now.Add(-7 * 24 * time.Hour)},
	}

	// Populate service dropdown.
	if svcs, err := store.ListDistinctServices(h.DB); err == nil {
		data.Services = svcs
	}

	// Parse filter + run query.
	filter, err := parseQueryFilter(r)
	if err != nil {
		data.Error = err.Error()
	} else {
		result, err := store.Query(h.DB, filter)
		if err != nil {
			data.Error = err.Error()
		} else {
			data.Records = result.Records
			data.Count = result.CountReturned
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := viewerTmpl.Execute(w, data); err != nil {
		http.Error(w, "template: "+err.Error(), http.StatusInternalServerError)
	}
}

// sevClass maps severity to a CSS class for the left-border color band.
func sevClass(s store.Severity) string {
	switch {
	case s >= store.SeverityFatal:
		return "sev-fatal"
	case s >= store.SeverityError:
		return "sev-error"
	case s >= store.SeverityWarn:
		return "sev-warn"
	case s >= store.SeverityInfo:
		return "sev-info"
	case s >= store.SeverityDebug:
		return "sev-debug"
	default:
		return "sev-trace"
	}
}

// shortTime formats a time as HH:MM:SS for the compact table view.
func shortTime(t time.Time) string {
	return t.Local().Format("15:04:05")
}

// fullTime formats a time for the expanded detail view.
func fullTime(t time.Time) string {
	return t.Local().Format("2006-01-02 15:04:05.000")
}

// truncate shortens a string to n runes with an ellipsis.
func truncate(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "..."
}

// queryString builds a URL query string from the current filters,
// optionally overriding or adding the refresh parameter. Used by the
// template to generate links that preserve the active filter state.
func queryString(d viewerData, extraKey, extraVal string) string {
	params := make(map[string]string)
	if d.FilterService != "" {
		params["service"] = d.FilterService
	}
	if d.FilterSeverity != "" {
		params["severity"] = d.FilterSeverity
	}
	if !d.Since.IsZero() {
		params["since"] = d.Since.Format(time.RFC3339)
	}
	if !d.Until.IsZero() {
		params["until"] = d.Until.Format(time.RFC3339)
	}
	if d.FilterSearch != "" {
		params["q"] = d.FilterSearch
	}
	if d.FilterLimit != "" {
		params["limit"] = d.FilterLimit
	}
	if d.RefreshInterval > 0 {
		params["refresh"] = d.RefreshInterval.String()
	}
	if extraKey != "" {
		if extraVal != "" {
			params[extraKey] = extraVal
		} else {
			delete(params, extraKey)
		}
	}
	var parts []string
	for k, v := range params {
		parts = append(parts, k+"="+v)
	}
	if len(parts) == 0 {
		return ""
	}
	return "?" + joinStringsAPI(parts, "&")
}

func joinStringsAPI(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for _, p := range parts[1:] {
		result += sep + p
	}
	return result
}

func rfc3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

var viewerFuncs = template.FuncMap{
	"sevClass":  sevClass,
	"shortTime": shortTime,
	"fullTime":  fullTime,
	"truncate":  truncate,
	"list":    func(args ...string) []string { return args },
	"qs":      queryString,
	"rfc3339": rfc3339,
}

var viewerTmpl = template.Must(template.New("viewer").Funcs(viewerFuncs).Parse(viewerHTML))

const viewerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Binnacle</title>
{{if .RefreshInterval}}<meta http-equiv="refresh" content="{{.RefreshInterval.Seconds | printf "%.0f"}}">{{end}}
<style>
  :root {
    --bg: #0d1117; --fg: #c9d1d9; --bg2: #161b22; --border: #30363d;
    --blue: #58a6ff; --green: #3fb950; --yellow: #d29922; --red: #f85149;
    --purple: #bc8cff; --subtle: #8b949e;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
         font-size: 14px; background: var(--bg); color: var(--fg); }
  a { color: var(--blue); text-decoration: none; }
  a:hover { text-decoration: underline; }

  /* -- filter bar -- */
  .filters { background: var(--bg2); border-bottom: 1px solid var(--border);
             padding: 12px 16px; display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
  .filters select, .filters input {
    background: var(--bg); color: var(--fg); border: 1px solid var(--border);
    border-radius: 6px; padding: 5px 8px; font-size: 13px; }
  .filters select { min-width: 140px; }
  .filters input[type=text] { min-width: 200px; flex: 1; }
  .filters button {
    background: #238636; color: #fff; border: none; border-radius: 6px;
    padding: 6px 14px; cursor: pointer; font-size: 13px; }
  .filters button:hover { background: #2ea043; }

  /* -- presets -- */
  .presets { padding: 6px 16px; background: var(--bg2); border-bottom: 1px solid var(--border);
             display: flex; gap: 6px; align-items: center; font-size: 12px; color: var(--subtle); }
  .presets a { padding: 2px 8px; border-radius: 4px; background: var(--bg); border: 1px solid var(--border); }
  .presets a:hover { border-color: var(--blue); }

  /* -- active chips -- */
  .chips { padding: 6px 16px; display: flex; flex-wrap: wrap; gap: 6px; }
  .chip { display: inline-flex; align-items: center; gap: 4px;
          background: var(--bg2); border: 1px solid var(--border); border-radius: 12px;
          padding: 2px 10px; font-size: 12px; }
  .chip .x { color: var(--subtle); cursor: pointer; font-weight: bold; }
  .chip .x:hover { color: var(--red); }

  /* -- status -- */
  .status { padding: 6px 16px; font-size: 12px; color: var(--subtle); }
  .error { padding: 12px 16px; background: #3d1f1f; color: var(--red); border-bottom: 1px solid #5a2d2d; }

  /* -- log table -- */
  table { width: 100%; border-collapse: collapse; }
  th { background: var(--bg2); position: sticky; top: 0; z-index: 1;
       text-align: left; padding: 8px 12px; font-size: 12px; font-weight: 600;
       color: var(--subtle); border-bottom: 1px solid var(--border); }
  tr.row { cursor: pointer; border-left: 3px solid transparent; }
  tr.row:hover { background: var(--bg2); }
  td { padding: 4px 12px; vertical-align: top; white-space: nowrap; }
  td.msg { white-space: normal; word-break: break-word; max-width: 0; width: 100%; }
  .ts { color: var(--subtle); font-family: "SF Mono", "Cascadia Mono", Consolas, monospace; font-size: 12px; }

  /* severity badges */
  .badge { display: inline-block; padding: 1px 6px; border-radius: 3px;
           font-size: 11px; font-weight: 600; font-family: monospace; }
  .sev-fatal .badge, .sev-error .badge { background: #5a2d2d; color: var(--red); }
  .sev-warn .badge { background: #3d2e00; color: var(--yellow); }
  .sev-info .badge { background: #0d2b45; color: var(--blue); }
  .sev-debug .badge { background: #1a1a2e; color: var(--purple); }
  .sev-trace .badge { background: #1a1a2e; color: var(--subtle); }

  /* severity left-border colors */
  .sev-fatal, .sev-error { border-left-color: var(--red) !important; }
  .sev-warn { border-left-color: var(--yellow) !important; }
  .sev-info { border-left-color: var(--blue) !important; }
  .sev-debug { border-left-color: var(--purple) !important; }

  /* detail row (hidden by default) */
  tr.detail { display: none; }
  tr.detail.open { display: table-row; }
  tr.detail td { padding: 8px 16px 16px 16px; background: var(--bg2);
                 border-bottom: 1px solid var(--border); white-space: normal; }
  .detail-grid { display: grid; grid-template-columns: auto 1fr; gap: 2px 16px; font-size: 13px; }
  .detail-grid dt { color: var(--subtle); font-weight: 600; text-align: right; }
  .detail-grid dd { word-break: break-all; }
  pre.stacktrace { margin-top: 8px; padding: 8px; background: var(--bg); border-radius: 6px;
                    font-size: 12px; overflow-x: auto; white-space: pre-wrap; word-break: break-all; }

  /* service link */
  .svc { color: var(--green); }
</style>
</head>
<body>

<form method="get" action="/logs">
<div class="filters">
  <select name="service">
    <option value="">All services</option>
    {{range .Services}}
    <option value="{{.}}"{{if eq . $.FilterService}} selected{{end}}>{{.}}</option>
    {{end}}
  </select>
  <select name="severity">
    <option value="">All levels</option>
    {{range $_, $sev := (list "TRACE" "DEBUG" "INFO" "WARN" "ERROR" "FATAL")}}
    <option value="{{$sev}}"{{if eq $sev $.FilterSeverity}} selected{{end}}>{{$sev}}+</option>
    {{end}}
  </select>
  <input type="text" name="q" placeholder="Search messages..." value="{{.FilterSearch}}">
  <input type="hidden" name="since" value="{{rfc3339 .Since}}">
  <input type="hidden" name="until" value="{{rfc3339 .Until}}">
  <button type="submit">Search</button>
  <a href="/logs" style="font-size:12px; margin-left:4px;">Clear</a>
</div>
</form>

<div class="presets">
  <span>Time range:</span>
  {{range .Presets}}
  <a href="/logs{{qs $ "since" (rfc3339 .Since)}}">{{.Label}}</a>
  {{end}}
  <a href="/logs{{qs $ "since" ""}}">All</a>
  <span style="margin-left:16px">Refresh:</span>
  <a href="/logs{{qs $ "refresh" "5s"}}">5s</a>
  <a href="/logs{{qs $ "refresh" "30s"}}">30s</a>
  <a href="/logs{{qs $ "refresh" "1m"}}">1m</a>
  {{if .RefreshInterval}}<a href="/logs{{qs $ "refresh" ""}}" style="color:var(--red)">off</a>
  <span style="color:var(--green)">&#9679; {{.RefreshInterval}}</span>{{end}}
</div>

<div class="chips">
  {{if .FilterService}}<span class="chip">service: {{.FilterService}} <a class="x" href="/logs{{qs $ "service" ""}}">x</a></span>{{end}}
  {{if .FilterSeverity}}<span class="chip">severity: {{.FilterSeverity}}+ <a class="x" href="/logs{{qs $ "severity" ""}}">x</a></span>{{end}}
  {{if not .Since.IsZero}}<span class="chip">since: {{shortTime .Since}} <a class="x" href="/logs{{qs $ "since" ""}}">x</a></span>{{end}}
  {{if .RefreshInterval}}<span class="chip">refresh: {{.RefreshInterval}} <a class="x" href="/logs{{qs $ "refresh" ""}}">x</a></span>{{end}}
</div>

{{if .Error}}<div class="error">{{.Error}}</div>{{end}}

<div class="status">{{.Count}} records</div>

<table>
<thead>
<tr><th class="ts">Time</th><th>Sev</th><th>Service</th><th>Message</th></tr>
</thead>
<tbody>
{{range $i, $r := .Records}}
<tr class="row {{sevClass $r.Severity}}" onclick="toggle({{$i}})">
  <td class="ts" title="{{fullTime $r.Time}}">{{shortTime $r.Time}}</td>
  <td><span class="badge">{{$r.Severity}}</span></td>
  <td><a class="svc" href="/logs?service={{$r.Service}}">{{$r.Service}}</a></td>
  <td class="msg">{{truncate $r.Message 200}}</td>
</tr>
<tr class="detail" id="d{{$i}}">
  <td colspan="4">
    <dl class="detail-grid">
      <dt>Time</dt><dd>{{fullTime $r.Time}}</dd>
      <dt>Ingest</dt><dd>{{fullTime $r.Ingest}}</dd>
      <dt>Severity</dt><dd>{{$r.Severity}}</dd>
      <dt>Service</dt><dd>{{$r.Service}}</dd>
      {{if $r.Instance}}<dt>Instance</dt><dd>{{$r.Instance}}</dd>{{end}}
      {{if $r.Version}}<dt>Version</dt><dd>{{$r.Version}}</dd>{{end}}
      {{if $r.Logger}}<dt>Logger</dt><dd>{{$r.Logger}}</dd>{{end}}
      {{if not $r.TraceID.IsZero}}<dt>Trace ID</dt><dd>{{$r.TraceID}}</dd>{{end}}
      {{if not $r.SpanID.IsZero}}<dt>Span ID</dt><dd>{{$r.SpanID}}</dd>{{end}}
      <dt>Message</dt><dd>{{$r.Message}}</dd>
      {{range $k, $v := $r.Attrs}}<dt>{{$k}}</dt><dd>{{$v}}</dd>{{end}}
    </dl>
    {{if $r.Exception}}
    <details open>
      <summary style="color:var(--red);font-weight:600;margin-top:8px">
        {{$r.Exception.Type}}{{if $r.Exception.Message}}: {{$r.Exception.Message}}{{end}}
      </summary>
      <pre class="stacktrace">{{$r.Exception.StackTrace}}</pre>
    </details>
    {{end}}
  </td>
</tr>
{{end}}
</tbody>
</table>

<script>
function toggle(i){var d=document.getElementById("d"+i);d.classList.toggle("open")}
</script>
</body>
</html>`
