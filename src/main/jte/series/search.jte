@template.layout(
title = "Series Search",
content = @`
    <form role="search">
        <input name="search" type="search" placeholder="Search"/>
        <button hx-post="/ui" hx-target="#results" hx-swap="innerHTML">Search</button>
    </form>

    <table class=".striped">
        <thead>
        <tr>
            <th scope="col">Series</th>
            <th scope="col">Actions</th>
        </tr>
        </thead>
        <tbody id="results">
        @template.series.results(series = java.util.List.<org.huebert.iotfsdb.schema.SeriesFile>of())
        </tbody>
    </table>
`
)
