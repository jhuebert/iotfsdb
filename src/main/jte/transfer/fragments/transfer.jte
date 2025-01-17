@import org.springframework.core.env.Environment

@param Environment environment

<h2>Transfer</h2>
@if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
    <form id='form' hx-encoding='multipart/form-data' hx-post='/ui/data/import'>
        <fieldset role="group">
            <input type='file' name='file'>
            <button>
                <img src="/icons/download.svg"/> Import Data
            </button>
        </fieldset>
        <progress id='progress' value='0' max='100'></progress>
    </form>
@endif
<button class="container-fluid" title="Export Database" onclick="location.href='/ui/data/export'">
    <img src="/icons/download.svg"/> Export Database
</button>
<script>
    htmx.on('#form', 'htmx:xhr:progress', function(evt) {
      htmx.find('#progress').setAttribute('value', evt.detail.loaded/evt.detail.total * 100)
    });
</script>
