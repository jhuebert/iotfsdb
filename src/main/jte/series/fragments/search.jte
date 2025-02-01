@import java.util.List
@import org.huebert.iotfsdb.schema.FindSeriesRequest
@import org.huebert.iotfsdb.schema.SeriesFile
@import org.huebert.iotfsdb.ui.service.BasePageService
@import org.huebert.iotfsdb.ui.service.SearchParser

@param BasePageService.BasePage basePage
@param FindSeriesRequest request
@param List<SeriesFile> series

<form class="mb-3">
    @template.components.search(hxPost = @`/ui/series/search`, hxTarget = "#results", value = SearchParser.toSearch(request))
</form>
<div id="results">
    @template.series.fragments.results(basePage = basePage, series = series)
</div>
