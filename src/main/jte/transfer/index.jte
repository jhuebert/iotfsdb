@import org.huebert.iotfsdb.schema.FindDataRequest
@import org.huebert.iotfsdb.ui.service.BasePageService
@import org.huebert.iotfsdb.ui.service.PlotData

@param BasePageService.BasePage basePage
@param FindDataRequest request
@param PlotData plotData

@template.layout(
basePage = basePage,
title = "Transfer",
content = @`
    @template.transfer.fragments.transfer(basePage = basePage)
`
)
