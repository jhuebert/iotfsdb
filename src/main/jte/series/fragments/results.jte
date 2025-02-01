@import java.util.List
@import java.util.Map.Entry
@import org.huebert.iotfsdb.schema.SeriesFile
@import org.huebert.iotfsdb.ui.service.BasePageService

@param BasePageService.BasePage basePage
@param List<SeriesFile> series

@for(SeriesFile file : series)
    <div class="card mb-3 series-row">
        <div class="card-body">
            <div class="row">
                <div class="col">
                    <h6 class="user-select-all mb-3">${file.getId()}</h6>
                </div>
                <div class="col col-auto">
                    <a class="btn btn-primary text-nowrap" href="/ui/series/${file.getId()}/export">
                        <i class="bi bi-download"></i>
                    </a>
                    @if(!basePage.isReadOnly())
                        @template.components.button(content = @`<i class="bi bi-trash"></i>`, secondary = true, hxDelete = @`/ui/series/${file.getId()}`, hxConfirm = @`Are you sure you want to delete '${file.getId()}'?`, hxSwap = "outerHTML", hxTarget = "closest .series-row")
                    @endif
                </div>
            </div>
        </div>
        <div class="card-body">
            <div class="row">
                <div class="col">
                    <div class="row card-subtitle text-center">
                        <div class="col">
                            <div class="row">
                                <small class="text-body-emphasis">Type</small>
                                <small class="text-body-secondary text-body-secondary font-monospace">${file.getDefinition().getType()}</small>
                            </div>
                        </div>
                        <div class="col">
                            <div class="row">
                                <small class="text-body-emphasis">Partition</small>
                                <small class="text-body-secondary font-monospace">${file.getDefinition().getPartition()}</small>
                            </div>
                        </div>
                        <div class="col">
                            <div class="row">
                                <small class="text-body-emphasis">Interval</small>
                                <small class="text-body-secondary font-monospace">${file.getDefinition().getInterval()}ms</small>
                            </div>
                        </div>
                        @if(file.getDefinition().getMin() != null)
                            <div class="col">
                                <div class="row">
                                    <small class="text-body-emphasis">Minimum</small>
                                    <small class="text-body-secondary font-monospace">${file.getDefinition().getMin()}</small>
                                </div>
                            </div>
                        @endif
                        @if(file.getDefinition().getMax() != null)
                            <div class="col">
                                <div class="row">
                                    <small class="text-body-emphasis">Maximum</small>
                                    <small class="text-body-secondary font-monospace">${file.getDefinition().getMax()}</small>
                                </div>
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
                        @template.series.fragments.metadata-row(basePage = basePage, file = file, key = entry.getKey(), value = entry.getValue())
                    @endfor
                    @if(!basePage.isReadOnly())
                        @template.series.fragments.metadata-row(basePage = basePage, file = file, key = null, value = null)
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
