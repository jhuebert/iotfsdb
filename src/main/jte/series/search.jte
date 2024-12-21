@template.layout(
title = "Series Search",
content = @`
    <form role="search">
        <input name="search" type="search" placeholder="Search"/>
        <button hx-post="/ui/series" hx-target="#results" hx-swap="innerHTML">Search</button>
    </form>
    <div>
        <hr/>
        <div id="results">
            @template.series.fragments.results(series = java.util.List.<org.huebert.iotfsdb.schema.SeriesFile>of())
        </div>
    </div>
`
)
