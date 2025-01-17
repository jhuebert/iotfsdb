@import org.huebert.iotfsdb.schema.FindDataRequest
@import org.huebert.iotfsdb.ui.service.PlotData
@import org.springframework.core.env.Environment

@param Environment environment
@param FindDataRequest request
@param PlotData plotData

@template.layout(
environment = environment,
title = "Transfer",
content = @`
    @template.transfer.fragments.transfer(environment = environment)
`
)
