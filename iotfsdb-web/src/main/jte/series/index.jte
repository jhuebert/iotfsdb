@import java.util.List
@import org.huebert.iotfsdb.api.schema.FindSeriesRequest
@import org.huebert.iotfsdb.api.schema.SeriesFile
@import org.huebert.iotfsdb.api.ui.service.BasePageService

@param BasePageService.BasePage basePage
@param FindSeriesRequest request
@param List<SeriesFile> series

@template.layout(
basePage = basePage,
title = "Series",
content = @`
    @template.series.fragments.search(basePage = basePage, request = request, series = series)
`
)
