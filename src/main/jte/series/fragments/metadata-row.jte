@import org.huebert.iotfsdb.api.schema.SeriesFile
@import org.huebert.iotfsdb.api.ui.service.BasePageService

@param BasePageService.BasePage basePage
@param SeriesFile file
@param String key
@param String value

!{boolean isNew = (key == null) && (value == null);}

<form class="metadata-row" hx-on::after-request="if(event.detail.successful) this.reset()">
    <div class="row mb-3">
        @template.components.text(id = "key", value = key, disabled = !isNew, placeholder = "Key")
        @template.components.text(id = "value", value = value, disabled = basePage.isReadOnly(), placeholder = "Value")
        @if(!basePage.isReadOnly())
            <div class="col col-auto">
                @if(isNew)
                    @template.components.button(content = @`<i class="bi bi-floppy"></i>`, hxPost = @`/ui/series/${file.getId()}/metadata`, hxTarget = "closest .metadata-row", hxSwap = "beforebegin")
                    @template.components.button(content = @`<i class="bi bi-trash"></i>`, secondary = true, visible = false)
                @else
                    @template.components.button(content = @`<i class="bi bi-floppy"></i>`, hxPost = @`/ui/series/${file.getId()}/metadata/${key}`, hxTarget = "closest .metadata-row", hxSwap = "outerHTML")
                    @template.components.button(content = @`<i class="bi bi-trash"></i>`, secondary = true, hxConfirm = @`Are you sure you want to delete '${key}'?`, hxDelete = @`/ui/series/${file.getId()}/metadata/${key}`, hxTarget = "closest .metadata-row")
                @endif
            </div>
        @endif
    </div>
</form>
