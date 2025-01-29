@import org.springframework.core.env.Environment

@param Environment environment

<div class="row mb-3">
    <h4>Transfer</h4>
</div>
@if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
    <div class="row mb-3">
        <form id='form' hx-encoding='multipart/form-data' hx-post='/ui/transfer/import'>
            <div class="input-group">
                <input class="form-control" type='file' name='file'>
                @template.components.button(content = @`<i class="bi bi-download"></i> Import Data`)
            </div>
        </form>
    </div>
@endif
<div class="row p-3">
    @template.components.anchor(content = @`<i class="bi bi-download"></i> Export Database`, href = @`/ui/transfer/export`)
</div>
