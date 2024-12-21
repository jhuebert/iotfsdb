@import org.huebert.iotfsdb.schema.SeriesFile

@param SeriesFile file
@param String key
@param String value

!{boolean isNew = (key == null) && (value == null);}

<form class="grid metadata-row" style="grid-template-columns: 4fr 1fr" hx-on::after-request="if(event.detail.successful) this.reset()">
    <div role="group">
        @if(isNew)
            <input
                    name="key"
                    placeholder="Key"
                    autocomplete="metadata-key"
                    value="${key}"
            />
        @else
            <input
                    name="key"
                    placeholder="Key"
                    autocomplete="metadata-key"
                    value="${key}"
                    readonly
            />
        @endif
        <input
                name="value"
                placeholder="Value"
                autocomplete="metadata-value"
                value="${value}"
        />
    </div>
    <div class="group">
        @if(isNew)
            <button
                    hx-post="/ui/series/${file.getId()}/metadata"
                    hx-target="closest .metadata-row"
                    hx-swap="beforebegin">
                <img src="/icons/floppy.svg"/>
            </button>
        @else
            <button
                    hx-post="/ui/series/${file.getId()}/metadata/${key}"
                    hx-target="closest .metadata-row"
                    hx-swap="outerHTML">
                <img src="/icons/floppy.svg"/>
            </button>
            <button
                    class="secondary"
                    title="Delete Series"
                    hx-delete="/ui/series/${file.getId()}/metadata/${key}"
                    hx-confirm="Are you sure you want to delete ${key}?"
                    hx-target="closest .metadata-row">
                <img src="/icons/trash.svg"/>
            </button>
        @endif
    </div>
</form>
