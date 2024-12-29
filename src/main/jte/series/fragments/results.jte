@import java.util.List
@import java.util.Map.Entry
@import org.huebert.iotfsdb.schema.SeriesFile
@import org.springframework.core.env.Environment

@param Environment environment
@param List<SeriesFile> series

@for(SeriesFile file : series)
    <article class="series-row">
        <div class="grid" style="grid-template-columns: 4fr 2fr">
            <div>
                <h4>${file.getId()}</h4>
            </div>
            <div class="grid">
                <button title="Chart Series">
                    <img src="/icons/graph-up.svg"/>
                </button>
                <button class="secondary" title="Export Series"
                        onclick="location.href='/ui/series/${file.getId()}/export'">
                    <img src="/icons/download.svg"/>
                </button>
                @if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
                    <button class="secondary" title="Delete Series" hx-delete="/ui/series/${file.getId()}"
                            hx-confirm="Are you sure you want to delete '${file.getId()}'?"
                            hx-target="closest .series-row">
                        <img src="/icons/trash.svg"/>
                    </button>
                @endif
            </div>
        </div>
        <hr/>
        <div>
            <details>
                <summary>Details</summary>
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
                    @template.series.fragments.metadata-row(environment = environment, file = file, key = entry.getKey(), value = entry.getValue())
                @endfor
                @if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
                    @template.series.fragments.metadata-row(environment = environment, file = file, key = null, value = null)
                @endif
            </details>
        </div>
    </article>
@else
    <div style="text-align: center">
        <h4>No results</h4>
    </div>
@endfor
