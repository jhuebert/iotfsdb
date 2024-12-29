@import org.huebert.iotfsdb.schema.SeriesFile
@import org.springframework.core.env.Environment

@param Environment environment
@param SeriesFile file
@param String key
@param String value

!{boolean isNew = (key == null) && (value == null);}

<form class="grid metadata-row" style="grid-template-columns: 4fr 2fr"
      hx-on::after-request="if(event.detail.successful) this.reset()">
    <div role="group">
        <input name="key" placeholder="Key" autocomplete="metadata-key" value="${key}" readonly="${!isNew}"/>
        <input name="value" placeholder="Value" autocomplete="metadata-value" value="${value}"
               readonly="${environment.getProperty("iotfsdb.read-only", Boolean.class, true)}"/>
    </div>
    @if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
        <div class="group">
            @if(isNew)
                <button hx-post="/ui/series/${file.getId()}/metadata" hx-target="closest .metadata-row"
                        hx-swap="beforebegin" hx-on::response-error="handleHtmxResponseError(event)">
                    <img src="/icons/floppy.svg"/>
                </button>
            @else
                <button hx-post="/ui/series/${file.getId()}/metadata/${key}" hx-target="closest .metadata-row"
                        hx-swap="outerHTML" hx-on::response-error="handleHtmxResponseError(event)">
                    <img src="/icons/floppy.svg"/>
                </button>
                <button class="secondary" title="Delete Series" hx-delete="/ui/series/${file.getId()}/metadata/${key}"
                        hx-confirm="Are you sure you want to delete '${key}'?" hx-target="closest .metadata-row"
                        hx-on::response-error="handleHtmxResponseError(event)">
                    <img src="/icons/trash.svg"/>
                </button>
            @endif
        </div>
    @endif
</form>
