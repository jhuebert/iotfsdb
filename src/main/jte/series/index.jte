@import java.util.List
@import org.huebert.iotfsdb.schema.SeriesFile

@param List<SeriesFile> series

@template.layout(
title = "Series Search",
content = @`
    @template.series.fragments.search(series = series)
`
)
