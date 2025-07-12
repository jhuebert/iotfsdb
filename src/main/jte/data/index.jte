@import org.huebert.iotfsdb.api.schema.FindDataRequest
@import org.huebert.iotfsdb.api.ui.service.BasePageService
@import org.huebert.iotfsdb.api.ui.service.PlotData

@param BasePageService.BasePage basePage
@param FindDataRequest request
@param PlotData plotData

@template.layout(
basePage = basePage,
title = "Data",
content = @`
    @template.data.fragments.search(request = request, plotData = plotData)
`
)
