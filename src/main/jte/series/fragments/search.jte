@import java.util.List
@import org.huebert.iotfsdb.schema.FindSeriesRequest
@import org.huebert.iotfsdb.schema.SeriesFile
@import org.huebert.iotfsdb.ui.service.SearchParser
@import org.springframework.core.env.Environment

@param Environment environment
@param FindSeriesRequest request
@param List<SeriesFile> series

<form class="mb-3">
    @template.components.search(hxPost = @`/ui/series/search`, hxTarget = "#results", value = SearchParser.toSearch(request))
</form>
<div id="results">
    @template.series.fragments.results(environment = environment, series = series)
</div>
