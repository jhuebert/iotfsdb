@import org.huebert.iotfsdb.schema.FindDataRequest
@import org.huebert.iotfsdb.ui.PlotData

@param FindDataRequest request
@param PlotData plotData

@template.layout(
title = "Data",
content = @`
    @template.data.fragments.search(request = request, plotData = plotData)
`
)
