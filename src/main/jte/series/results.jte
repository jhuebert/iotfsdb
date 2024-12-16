@import java.util.List
@import java.util.Map.Entry
@import org.huebert.iotfsdb.schema.SeriesFile

@param List<SeriesFile> series

@for(SeriesFile file : series)
    <tr>
        <td>
            <details>
                <summary>${file.getDefinition().getId()}</summary>
                <h5>Details</h5>
                <table>
                    <thead>
                    <tr>
                        <th scope="col">Property</th>
                        <th scope="col">Value</th>
                    </tr>
                    </thead>
                    <tbody>
                    <tr>
                        <td>Type</td>
                        <td><kbd>${file.getDefinition().getType()}</kbd></td>
                    </tr>
                    <tr>
                        <td>Partition</td>
                        <td><kbd>${file.getDefinition().getPartition()}</kbd></td>
                    </tr>
                    <tr>
                        <td>Interval (ms)</td>
                        <td>${file.getDefinition().getInterval()}</td>
                    </tr>
                    @if(file.getDefinition().getMin() != null)
                        <tr>
                            <td>Min</td>
                            <td>${file.getDefinition().getMin()}</td>
                        </tr>
                    @endif
                    @if(file.getDefinition().getMax() != null)
                        <tr>
                            <td>Min</td>
                            <td>${file.getDefinition().getMax()}</td>
                        </tr>
                    @endif
                    </tbody>
                </table>
                <h5>Metadata</h5>
                <table>
                    <thead>
                    <tr>
                        <th scope="col">Key</th>
                        <th scope="col">Value</th>
                    </tr>
                    </thead>
                    <tbody>
                    @for(Entry<String, String> entry : file.getMetadata().entrySet())
                        <tr>
                            <td>${entry.getKey()}</td>
                            <td>${entry.getValue()}</td>
                        </tr>
                    @endfor
                    </tbody>
                </table>
            </details>
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
