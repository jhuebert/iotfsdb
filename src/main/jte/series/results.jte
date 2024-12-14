@import java.util.List
@import org.huebert.iotfsdb.schema.SeriesFile
@import org.apache.commons.lang3.StringUtils

@param List<SeriesFile> series

@for(SeriesFile file : series)
    <tr>
        <td>${file.getDefinition().getId()}</td>
        <td>${file.getDefinition().getInterval()}</td>
        <td>${file.getDefinition().getType()}</td>
        <td>
            <button class="secondary"><img src="/icons/pencil.svg"/></button>
            <button class="secondary"><img src="/icons/download.svg"/></button>
            <button class="secondary"><img src="/icons/trash.svg"/></button>
        </td>
    </tr>
@else
    <tr>
    <tr colspan="4" style="text-align:center;"><h4>No results</h4></tr>
    </tr>
@endfor
