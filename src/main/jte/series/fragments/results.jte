@import java.util.List
@import java.util.Map.Entry
@import org.huebert.iotfsdb.schema.SeriesFile

@param List<SeriesFile> series

@for(SeriesFile file : series)
    <div class="grid series-row" style="grid-template-columns: 4fr 1fr">
        <div>
            <details>
                <summary><h4>${file.getId()}</h4></summary>
                <h5>Details</h5>
                <div class="grid">
                    <div>
                        <div><h6>Type</h6></div>
                        <div><kbd>${file.getDefinition().getType()}</kbd></div>
                    </div>
                    <div>
                        <div><h6>Partition</h6></div>
                        <div><kbd>${file.getDefinition().getPartition()}</kbd></div>
                    </div>
                    <div>
                        <div><h6>Interval (ms)</h6></div>
                        <div>${file.getDefinition().getInterval()}</div>
                    </div>
                    @if(file.getDefinition().getMin() != null)
                        <div>
                            <div><h6>Minimum</h6></div>
                            <div>${file.getDefinition().getMin()}</div>
                        </div>
                    @endif
                    @if(file.getDefinition().getMax() != null)
                        <div>
                            <div><h6>Maximum</h6></div>
                            <div>${file.getDefinition().getMax()}</div>
                        </div>
                    @endif
                </div>
                <br>
                <h5>Metadata</h5>
                @for(Entry<String, String> entry : file.getMetadata().entrySet())
                    @template.series.fragments.metadata-row(file = file, key = entry.getKey(), value = entry.getValue())
                @endfor
                <form class="grid metadata-new" style="grid-template-columns: 3fr 1fr" hx-on::after-request=" if(event.detail.successful) this.reset()">
                    <div role="group">
                        <input
                                name="key"
                                placeholder="Key"
                                autocomplete="metadata-key"
                        />
                        <input
                                name="value"
                                placeholder="Value"
                                autocomplete="metadata-value"
                        />
                    </div>
                    <div>
                        <button
                                hx-post="/ui/series/${file.getId()}/metadata"
                                hx-target="closest .metadata-new"
                                hx-swap="beforebegin">
                            <img src="/icons/floppy.svg"/>
                        </button>
                    </div>
                </form>
            </details>
        </div>
        <div>
            <button title="Chart Series"><img src="/icons/graph-up.svg"/></button>
            <button
                    class="secondary"
                    title="Download Series"
                    hx-post="/v2/data/export/series/${file.getId()}"
            >
                <img src="/icons/download.svg"/>
            </button>
            <button
                    class="secondary"
                    title="Delete Series"
                    hx-delete="/ui/series/${file.getId()}"
                    hx-confirm="Are you sure you want to delete ${file.getId()}?"
                    hx-target="closest .series-row"
            >
                <img src="/icons/trash.svg"/>
            </button>
        </div>
    </div>
@else
    <div style="text-align: center">
        <h4>No results</h4>
    </div>
@endfor
