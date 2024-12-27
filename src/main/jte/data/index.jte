@import org.huebert.iotfsdb.ui.PlotData

@param PlotData plotData

@template.layout(
title = "Data",
content = @`
    @template.data.fragments.search(plotData = plotData)
`
)
