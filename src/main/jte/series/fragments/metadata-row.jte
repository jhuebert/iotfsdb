@import org.huebert.iotfsdb.schema.SeriesFile
@import org.springframework.core.env.Environment

@param Environment environment
@param SeriesFile file
@param String key
@param String value

!{boolean isNew = (key == null) && (value == null);}

<form class="metadata-row" hx-on::after-request="if(event.detail.successful) this.reset()">
    <div class="row mb-3">
        @template.components.text(id = "key", value = key, disabled = !isNew, placeholder = "Key")
        @template.components.text(id = "value", value = value, disabled = environment.getProperty("iotfsdb.read-only", Boolean.class, true), placeholder = "Value")
        @if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
            <div class="col col-auto">
                @if(isNew)
                    @template.components.button(content = @`<i class="bi bi-floppy"></i>`, hxPost = @`/ui/series/${file.getId()}/metadata`, hxTarget = "closest .metadata-row", hxSwap = "beforebegin")
                    @template.components.button(content = @`<i class="bi bi-trash"></i>`, secondary = true, style="visibility: hidden")
                @else
                    @template.components.button(content = @`<i class="bi bi-floppy"></i>`, hxPost = @`/ui/series/${file.getId()}/metadata/${key}`, hxTarget = "closest .metadata-row", hxSwap = "outerHTML")
                    @template.components.button(content = @`<i class="bi bi-trash"></i>`, secondary = true, hxConfirm = @`Are you sure you want to delete '${key}'?`, hxDelete = @`/ui/series/${file.getId()}/metadata/${key}`, hxTarget = "closest .metadata-row")
                @endif
            </div>
        @endif
    </div>
</form>
