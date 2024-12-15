@import java.util.List
@import java.util.Map.Entry
@import org.huebert.iotfsdb.schema.SeriesFile
@import org.apache.commons.lang3.StringUtils

@param List<SeriesFile> series

@for(SeriesFile file : series)
    <tr>
        <td>${file.getDefinition().getId()}</td>
        <td>
            <details>
                <summary>Details</summary>
                <ul>
                    <li>Type: ${file.getDefinition().getType()}</li>
                    <li>Partition: ${file.getDefinition().getPartition()}</li>
                    <li>Interval: ${file.getDefinition().getInterval()}</li>
                    <li>Min: ${file.getDefinition().getMin()}</li>
                    <li>Max: ${file.getDefinition().getMax()}</li>
                </ul>
            </details>
            @if (!file.getMetadata().isEmpty())
                <details>
                    <summary>Metadata</summary>
                    <ul>
                        @for(Entry<String, String> entry : file.getMetadata().entrySet())
                            <li>${entry.getKey()}: ${entry.getValue()}</li>
                        @endfor
                    </ul>
                </details>
            @endif
        </td>
        <td>
            <button class="secondary" title="Chart Series"><img src="/icons/graph-up.svg"/></button>
            <button class="secondary" title="Download Series"><img src="/icons/download.svg"/></button>
            <button class="secondary" title="Edit Metadata"><img src="/icons/pencil.svg"/></button>
            <button class="secondary" title="Delete Series"><img src="/icons/trash.svg"/></button>
        </td>
    </tr>
@else
    <tr>
        <td colspan="2" style="text-align:center;"><h4>No results</h4></td>
    </tr>
@endfor
