@import java.util.List
@import java.util.Map.Entry
@import org.huebert.iotfsdb.schema.SeriesFile
@import org.springframework.core.env.Environment

@param Environment environment
@param List<SeriesFile> series

<table class="table table-striped">
    <thead>
    <tr>
        <th scope="col"><h4>ID</h4></th>
        <th scope="col"><h4>Type</h4></th>
        <th scope="col"><h4>Partition</h4></th>
        <th scope="col"><h4>Interval</h4></th>
        <th scope="col"><h4>Range</h4></th>
        <th scope="col"></th>
    </tr>
    </thead>
    <tbody>
    @for(SeriesFile file : series)
        <tr class="align-middle series-row">
            <td>${file.getId()}</td>
            <td><kbd>${file.getDefinition().getType()}</kbd></td>
            <td><kbd>${file.getDefinition().getPartition()}</kbd></td>
            <td>${file.getDefinition().getInterval()}ms</td>
            <td>
                @if((file.getDefinition().getMin() != null) || (file.getDefinition().getMax() != null))
                    ${file.getDefinition().getMin()} - ${file.getDefinition().getMax()}
                @endif
            </td>
            <td>
                @template.components.anchor(content = @`<img src="/icons/download.svg"/>`, href = @`/ui/series/${file.getId()}/export`)
                @if(!environment.getProperty("iotfsdb.read-only", Boolean.class, true))
                    @template.components.button(content = @`<img src="/icons/trash.svg"/>`, secondary = true, hxDelete = @`/ui/series/${file.getId()}`, hxConfirm = @`Are you sure you want to delete '${file.getId()}'?`, hxSwap = "outerHTML", hxTarget = "closest .series-row")
                @endif
            </td>
        </tr>
        <tr>
            <th colspan="6" scope="row">
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
            </th>
        </tr>
    @else
        <tr>
            <th class="text-center" colspan="6" scope="row">
                <h4>No results</h4>
            </th>
        </tr>
    @endfor
    </tbody>
</table>
