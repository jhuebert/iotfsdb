@import org.huebert.iotfsdb.ui.service.BasePageService

@param BasePageService.BasePage basePage

@if(!basePage.isReadOnly())
    <div class="row mt-3 mb-3">
        <form id="form" hx-encoding="multipart/form-data" hx-post="/ui/transfer/import">
            <div class="input-group">
                <input class="form-control" type="file" name="file">
                @template.components.button(content = @`<i class="bi bi-download"></i> Import Data`)
            </div>
        </form>
    </div>
@endif
<div class="row p-3">
    <a class="btn btn-primary text-nowrap" href="/ui/transfer/export">
        <i class="bi bi-download"></i> Export Database
    </a>
</div>
