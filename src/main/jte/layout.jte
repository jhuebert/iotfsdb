@import gg.jte.Content
@import org.huebert.iotfsdb.api.ui.service.BasePageService

@param BasePageService.BasePage basePage
@param String title
@param Content content

<!DOCTYPE html>
<html lang="en" data-bs-theme="dark">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>iotfsdb - ${title}</title>
    <link rel="icon" href="/icons/cpu.svg" sizes="any" type="image/svg+xml">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/htmx/2.0.7/htmx.min.js" integrity="sha512-IisGoumHahmfNIhP4wUV3OhgQZaaDBuD6IG4XlyjT77IUkwreZL3T3afO4xXuDanSalZ57Un+UlAbarQjNZCTQ==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.8/css/bootstrap.min.css" integrity="sha512-2bBQCjcnw658Lho4nlXJcc6WkV/UxpE/sAokbXPxQNGqmNdQrWqtw26Ns9kFF/yG792pKR1Sx8/Y1Lf1XN4GKA==" crossorigin="anonymous" referrerpolicy="no-referrer" />
    <script src="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.3.8/js/bootstrap.bundle.min.js" integrity="sha512-HvOjJrdwNpDbkGJIG2ZNqDlVqMo77qbs4Me4cah0HoDrfhrbA+8SBlZn1KrvAQw7cILLPFJvdwIgphzQmMm+Pw==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap-icons/1.13.1/font/bootstrap-icons.min.css" integrity="sha512-t7Few9xlddEmgd3oKZQahkNI4dS6l80+eGEzFQiqtyVYdvcSG2D3Iub77R20BdotfRPA9caaRkg1tyaJiPmO0g==" crossorigin="anonymous" referrerpolicy="no-referrer" />
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.5.0/chart.umd.min.js" integrity="sha512-Y51n9mtKTVBh3Jbx5pZSJNDDMyY+yGe77DGtBPzRlgsf/YLCh13kSZ3JmfHGzYFCmOndraf0sQgfM654b7dJ3w==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/moment.js/2.30.1/moment-with-locales.min.js" integrity="sha512-4F1cxYdMiAW98oomSLaygEwmCnIP38pb4Kx70yQYqRwLVCs3DbRumfBq82T08g/4LJ/smbFGFpmeFlQgoDccgg==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-adapter-moment/1.0.1/chartjs-adapter-moment.min.js" integrity="sha512-hVy4KxCKgnXi2ok7rlnlPma4JHXI1VPQeempoaclV1GwRHrDeaiuS1pI6DVldaj5oh6Opy2XJ2CTljQLPkaMrQ==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
    <script>
        function handleHtmxResponseError(event) {
          console.error(event);
          let alert = '<div class="alert alert-danger alert-dismissible fade show" role="alert"><strong>Error</strong> ';
          alert += event.detail.error;
          alert += '<button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button></div>';
          document.getElementById('htmx-error').innerHTML = alert;
        }
    </script>
    <style>
        html, body {
          height: 100%;
          margin: 0;
        }
    </style>
</head>
<body class="container-fluid h-100 p-0" hx-headers='js:{"Iotfsdb-Tz": Intl.DateTimeFormat().resolvedOptions().timeZone}'>
<nav class="navbar navbar-expand-sm bg-body-tertiary">
    <div class="container-fluid">
        <a class="navbar-brand" href="/ui">
            <span class="me-2" title="iotfsdb ${basePage.getVersion()}"><i class="bi bi-cpu"></i> iotfsdb</span>
        </a>
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarSupportedContent"
                aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
            <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarSupportedContent">
            <ul class="navbar-nav me-auto mb-2 mb-lg-0">
                <li class="nav-item"><a class="nav-link ${"Series".equals(title) ? "active bg-secondary rounded" : ""}" href="/ui/series">Series</a></li>
                <li class="nav-item"><a class="nav-link ${"Data".equals(title) ? "active bg-secondary rounded" : ""}" href="/ui/data">Data</a></li>
                <li class="nav-item"><a class="nav-link ${"Transfer".equals(title) ? "active bg-secondary rounded" : ""}" href="/ui/transfer">Transfer</a></li>
                @if(basePage.isSpringdocEnabled())
                    <li class="nav-item"><a class="nav-link" href="/swagger-ui/index.html">API</a></li>
                @endif
            </ul>
            @if(!basePage.isReadOnly())
                @template.components.button(content = @`<i class="bi bi-plus"></i> Create Series`, hxGet=@`/ui/series/create`, hxTarget="#main-body")
            @endif
        </div>
    </div>
</nav>
<div class="h-100 p-3" id="main-body">
    <div id="htmx-error"></div>
    ${content}
</div>
</body>
</html>
