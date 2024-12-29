@import java.util.List
@import org.huebert.iotfsdb.schema.FindSeriesRequest
@import org.huebert.iotfsdb.schema.SeriesFile
@import org.springframework.core.env.Environment

@param Environment environment
@param FindSeriesRequest request
@param List<SeriesFile> series

@template.layout(
environment = environment,
title = "Series",
content = @`
    @template.series.fragments.search(environment = environment, request = request, series = series)
`
)
