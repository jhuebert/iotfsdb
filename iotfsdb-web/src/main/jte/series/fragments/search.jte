@import java.util.List
@import org.huebert.iotfsdb.api.schema.FindSeriesRequest
@import org.huebert.iotfsdb.api.schema.SeriesFile
@import org.huebert.iotfsdb.api.ui.service.BasePageService
@import org.huebert.iotfsdb.api.ui.service.SearchParser

@param BasePageService.BasePage basePage
@param FindSeriesRequest request
@param List<SeriesFile> series

<form class="mb-3">
    @template.components.search(hxPost = @`/ui/series/search`, hxTarget = "#results", value = SearchParser.toSearch(request), small = true)
</form>
<div id="results">
    @template.series.fragments.results(basePage = basePage, series = series)
</div>
