@import java.util.List
@import org.huebert.iotfsdb.schema.SeriesDefinition

@param List<SeriesDefinition> series

@for(SeriesDefinition definition : series)
    <tr>
        <th scope="row">${definition.getId()}</th>
        <td>${definition.getInterval()}</td>
        <td>${definition.getType()}</td>
    </tr>
@else
    <tr>
        <th colspan="3" scope="row" style="text-align:center;"><h4>No results</h4></th>
    </tr>
@endfor
