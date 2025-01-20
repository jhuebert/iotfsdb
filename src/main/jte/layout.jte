@import gg.jte.Content
@import org.springframework.core.env.Environment

@param Environment environment
@param String title
@param Content content

<!DOCTYPE html>
<html lang="en" data-bs-theme="dark">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>iotfsdb - ${title}</title>
    <link rel="icon" href="/icons/cpu.svg" sizes="any" type="image/svg+xml">
    <script src="https://unpkg.com/htmx.org@2.0.3"
            integrity="sha384-0895/pl2MU10Hqc6jd4RvrthNlDiE9U1tWmX7WRESftEDRosgxNsQG/Ze9YMRzHq"
            crossorigin="anonymous"></script>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet"
          integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"
            integrity="sha384-YvpcrYf0tY3lHB60NNkmXc5s9fDVZLESaAA55NDzOxhy9GkcIdslK1eN7N6jIeHz"
            crossorigin="anonymous"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@^4"></script>
    <script src="https://cdn.jsdelivr.net/npm/moment@^2"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-moment@^1"></script>
    <script>
        function handleHtmxResponseError(event) {
          console.error(event);
          document.getElementById('error-content').innerHTML = event.detail.error;
          document.getElementById('error-dialog').showModal();
        }
    </script>
</head>
<body class="container-fluid">
<nav class="navbar navbar-expand-lg bg-body-tertiary">
    <div class="container-fluid">
        <a class="navbar-brand" href="/ui">iotfsdb</a>
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarSupportedContent"
                aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
            <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarSupportedContent">
            <ul class="navbar-nav me-auto mb-2 mb-lg-0">
                <li class="nav-item"><a class="nav-link" href="/ui/series">Series</a></li>
                <li class="nav-item"><a class="nav-link" href="/ui/data">Data</a></li>
                <li class="nav-item"><a class="nav-link" href="/ui/transfer">Transfer</a></li>
            </ul>
            @if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
                @template.components.button(content = @`<img src="/icons/plus.svg"/> Create Series`, hxGet=@`/ui/series/create`, hxTarget="#main-body")
            @endif
        </div>
    </div>
</nav>
<div class="mt-3" id="main-body">
    ${content}
</div>
<div id="error-div">
    <dialog id="error-dialog">
        <article>
            <header>
                <button class="btn btn-danger" aria-label="Close" rel="prev"
                        onclick="document.getElementById('error-dialog').close()"></button>
                <p>
                    <strong id="error-title">Error</strong>
                </p>
            </header>
            <p id="error-content"></p>
        </article>
    </dialog>
</div>
</body>
</html>
