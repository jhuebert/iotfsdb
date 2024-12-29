@import gg.jte.Content
@import org.springframework.core.env.Environment

@param Environment environment
@param String title
@param Content content

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="color-scheme" content="light dark">
    <title>iotfsdb - ${title}</title>
    <link rel="icon" href="/icons/cpu.svg" sizes="any" type="image/svg+xml">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css">
    <script src="https://unpkg.com/htmx.org@2.0.3"
            integrity="sha384-0895/pl2MU10Hqc6jd4RvrthNlDiE9U1tWmX7WRESftEDRosgxNsQG/Ze9YMRzHq"
            crossorigin="anonymous"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@^4"></script>
    <script src="https://cdn.jsdelivr.net/npm/moment@^2"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-moment@^1"></script>
</head>
<body class="container-fluid" style="min-width: 420px">
<nav>
    <ul>
        <li><h1 onclick="location.href='/ui'">iotfsdb</h1></li>
    </ul>
    <ul>
        <li><a href="/ui/series">Series</a></li>
        <li><a href="/ui/data">Data</a></li>
        <li>
            <button title="Export Database" onclick="location.href='/ui/export'">
                <img src="/icons/download.svg"/>
            </button>
        </li>
        @if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
            <li>
                <button title="Create Series" hx-get="/ui/series/create" hx-target="#main-body" hx-swap="innerHTML">
                    <img src="/icons/plus.svg"/>
                </button>
            </li>
        @endif
    </ul>
</nav>
<div id="main-body">
    ${content}
</div>
</body>
</html>
