@import java.util.List
@import org.huebert.iotfsdb.schema.FindSeriesRequest
@import org.huebert.iotfsdb.schema.SeriesFile
@import org.huebert.iotfsdb.ui.service.SearchParser
@import org.springframework.core.env.Environment

@param Environment environment
@param FindSeriesRequest request
@param List<SeriesFile> series

<form role="search">
    <input name="search" type="search" placeholder="Search" value="${request != null ? SearchParser.toSearch(request) : null}"/>
    <button hx-post="/ui/series/search" hx-target="#results" hx-swap="innerHTML">Search</button>
</form>
<div>
    <hr/>
    <div id="results">
        @template.series.fragments.results(environment = environment, series = series)
    </div>
</div>
