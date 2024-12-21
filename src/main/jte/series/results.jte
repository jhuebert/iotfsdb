@import java.util.List
@import java.util.Map.Entry
@import org.huebert.iotfsdb.schema.SeriesFile

@param List<SeriesFile> series

@for(SeriesFile file : series)
    <tr>
        <td>
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
                    <form class="grid" style="grid-template-columns: 3fr 1fr">
                        <label>
                            ${entry.getKey()}
                            <input
                                    name="value"
                                    placeholder="Value"
                                    autocomplete="metadata-value"
                                    value="${entry.getValue()}"
                            />
                        </label>
                        <div class="group">
                            <button
                                    hx-post="/ui/series"
                                    hx-target="#results"
                                    hx-swap="innerHTML">
                                <img src="/icons/floppy.svg"/>
                            </button>
                            <button
                                    class="secondary"
                                    hx-post="/ui/series"
                                    hx-target="#results"
                                    hx-confirm="Are you sure you want to delete ${entry.getKey()}?"
                                    hx-swap="innerHTML">
                                <img src="/icons/trash.svg"/>
                            </button>
                        </div>
                    </form>
                @endfor
            </details>
        </td>
        <td style="text-align:end;vertical-align:top;">
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
                    hx-target="closest tr"
            >
                <img src="/icons/trash.svg"/>
            </button>
        </td>
    </tr>
@else
    <tr>
        <td colspan="2" style="text-align:center;"><h4>No results</h4></td>
    </tr>
@endfor
