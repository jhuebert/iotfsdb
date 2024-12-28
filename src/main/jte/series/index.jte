@import java.util.List
@import org.huebert.iotfsdb.schema.FindSeriesRequest
@import org.huebert.iotfsdb.schema.SeriesFile

@param FindSeriesRequest request
@param List<SeriesFile> series

@template.layout(
title = "Series",
content = @`
    @template.series.fragments.search(request = request, series = series)
`
)
