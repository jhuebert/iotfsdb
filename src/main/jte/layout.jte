@import gg.jte.Content

@param String title
@param Content content

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="color-scheme" content="light dark">
    <title>iotfsdb - ${title}</title>
    <%--    <link rel="icon" href="./favicon.ico" type="image/x-icon"> -->--%>
<%--    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css">--%>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@yohns/picocss@2.2.1/css/pico.min.css" />
    <script src="https://unpkg.com/htmx.org@2.0.3" integrity="sha384-0895/pl2MU10Hqc6jd4RvrthNlDiE9U1tWmX7WRESftEDRosgxNsQG/Ze9YMRzHq" crossorigin="anonymous"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@^4"></script>
    <script src="https://cdn.jsdelivr.net/npm/moment@^2"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-moment@^1"></script>
</head>
<body class="container-fluid">
<nav>
    <ul>
        <li>
            <h1 onclick="location.href='/ui'">iotfsdb</h1>
        </li>
    </ul>
    <ul>
        <li><a href="/ui/series">Series</a></li>
        <li><a href="/ui/data">Data</a></li>
        <li>
            <button title="Export Series" onclick="location.href='/ui/export'">
                <img src="/icons/download.svg"/> Export
            </button>
        </li>
        <%--            TODO Import Database --%>
        <li>
            <button title="Create Series" hx-get="/ui/series/create" hx-target="#main-body" hx-swap="innerHTML">
                <img src="/icons/plus.svg"/> Series
            </button>
        </li>
    </ul>
</nav>
<div id="main-body">
    ${content}
</div>
</body>
</html>
