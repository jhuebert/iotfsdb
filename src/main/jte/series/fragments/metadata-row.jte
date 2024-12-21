@import org.huebert.iotfsdb.schema.SeriesFile

@param SeriesFile file
@param String key
@param String value

<form class="grid metadata-row" style="grid-template-columns: 4fr 1fr">
    <label>
        ${key}
        <input
                name="value"
                placeholder="Value"
                autocomplete="metadata-value"
                value="${value}"
        />
    </label>
    <div class="group">
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
    </div>
</form>
