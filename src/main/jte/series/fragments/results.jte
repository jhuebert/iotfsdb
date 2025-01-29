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
                    <h6 class="user-select-all mb-3">${file.getId()}</h6>
                </div>
                <div class="col col-auto">
                    @template.components.anchor(content = @`<i class="bi bi-download"></i>`, href = @`/ui/series/${file.getId()}/export`)
                    @if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
                        @template.components.button(content = @`<i class="bi bi-trash"></i>`, secondary = true, hxDelete = @`/ui/series/${file.getId()}`, hxConfirm = @`Are you sure you want to delete '${file.getId()}'?`, hxSwap = "outerHTML", hxTarget = "closest .series-row")
                    @endif
                </div>
            </div>
        </div>
        <div class="card-body">
            <div class="row">
                <div class="col">
                    <div class="row card-subtitle text-body-secondary">
                        <div class="col">
                            <small>Type:</small>
                            <small class="font-monospace">${file.getDefinition().getType()}</small>
                        </div>
                        <div class="col">
                            <small>Partition:</small>
                            <small class="font-monospace">${file.getDefinition().getPartition()}</small>
                        </div>
                        <div class="col">
                            <small>Interval:</small>
                            <small class="font-monospace">${file.getDefinition().getInterval()}ms</small>
                        </div>
                        @if(file.getDefinition().getMin() != null)
                            <div class="col">
                                <small>Minimum:</small>
                                <small class="font-monospace">${file.getDefinition().getMin()}</small>
                            </div>
                        @endif
                        @if(file.getDefinition().getMax() != null)
                            <div class="col">
                                <small>Maximum:</small>
                                <small class="font-monospace">${file.getDefinition().getMax()}</small>
                            </div>
                        @endif
                    </div>
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
