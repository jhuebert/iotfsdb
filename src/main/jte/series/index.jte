@import java.util.List
@import org.huebert.iotfsdb.schema.FindSeriesRequest
@import org.huebert.iotfsdb.schema.SeriesFile
@import org.huebert.iotfsdb.ui.service.BasePageService

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
