@import java.util.List
@import java.util.Map.Entry
@import org.huebert.iotfsdb.schema.SeriesFile
@import org.springframework.core.env.Environment

@param Environment environment
@param List<SeriesFile> series

@for(SeriesFile file : series)
    <div class="card mb-3 series-row">
        <div class="card-body">
            <div class="row">
                <div class="col">
                    <h6 class="mb-3">${file.getId()}</h6>
                    <div class="row card-subtitle text-body-secondary">
                        <div class="col">
                            <small>Type:</small>
                            <small><samp>${file.getDefinition().getType()}</samp></small>
                        </div>
                        <div class="col">
                            <small>Partition:</small>
                            <small><samp>${file.getDefinition().getPartition()}</samp></small>
                        </div>
                        <div class="col">
                            <small>Interval:</small>
                            <small><samp>${file.getDefinition().getInterval()}ms</samp></small>
                        </div>
                        @if(file.getDefinition().getMin() != null)
                            <div class="col">
                                <small>Minimum:</small>
                                <small><samp>${file.getDefinition().getMin()}</samp></small>
                            </div>
                        @endif
                        @if(file.getDefinition().getMax() != null)
                            <div class="col">
                                <small>Maximum:</small>
                                <small><samp>${file.getDefinition().getMax()}</samp></small>
                            </div>
                        @endif
                    </div>
                </div>
                <div class="col col-md-auto">
                    @template.components.anchor(content = @`<img src="/icons/download.svg"/>`, href = @`/ui/series/${file.getId()}/export`)
                    @if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
                        @template.components.button(content = @`<img src="/icons/trash.svg"/>`, secondary = true, hxDelete = @`/ui/series/${file.getId()}`, hxConfirm = @`Are you sure you want to delete '${file.getId()}'?`, hxSwap = "outerHTML", hxTarget = "closest .series-row")
                    @endif
                </div>
            </div>
        </div>
        <div class="card-footer">
            <details>
                <summary>Metadata</summary>
                <div class="row mt-3">
                    @for(Entry<String, String> entry : file.getMetadata().entrySet())
                        @template.series.fragments.metadata-row(environment = environment, file = file, key = entry.getKey(), value = entry.getValue())
                    @endfor
                    @if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
                        @template.series.fragments.metadata-row(environment = environment, file = file, key = null, value = null)
                    @endif
                </div>
            </details>
        </div>
    </div>
@else
    <div class="row text-center">
        <h4>No results</h4>
    </div>
@endfor
