@import org.springframework.core.env.Environment

@param Environment environment

<div class="container">
    <div class="row mb-3">
        <h2>Transfer</h2>
    </div>
    @if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
        <div class="row mb-3">
            <form id='form' hx-encoding='multipart/form-data' hx-post='/ui/data/import'>
                <div class="input-group">
                    <input class="form-control" type='file' name='file'>
                    @template.components.button(content = @`<img src="/icons/download.svg"/> Import Data`)
                </div>
            </form>
        </div>
        <div class="row mb-3">
            <progress id='progress' value='0' max='100'></progress>
        </div>
    @endif
    <div class="row mb-3">
        @template.components.anchor(content = @`<img src="/icons/download.svg"/> Export Database`, href = @`/ui/transfer/export`)
    </div>
</div>
<script>
    htmx.on('#form', 'htmx:xhr:progress', function(evt) {
      htmx.find('#progress').setAttribute('value', evt.detail.loaded/evt.detail.total * 100)
    });
</script>
