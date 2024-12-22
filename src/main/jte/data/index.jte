@import org.huebert.iotfsdb.ui.PlotData
@import java.util.List

@param PlotData plotData

@template.layout(
title = "Data",
content = @`
    @template.data.fragments.search(plotData = plotData)
`
)
