@template.layout(
title = "Series Search",
content = @`
    <form role="search">
        <input name="search" type="search" placeholder="Search"/>
        <button hx-post="/ui/series" hx-target="#results" hx-swap="innerHTML">Search</button>
    </form>
    <table class="striped">
        <thead>
        <tr>
            <th scope="col"><h2>Series</h2></th>
            <th scope="col" style="width:16rem;text-align:end"><h2>Actions</h2></th>
        </tr>
        </thead>
        <tbody id="results">
        @template.series.results(series = java.util.List.<org.huebert.iotfsdb.schema.SeriesFile>of())
        </tbody>
    </table>
`
)
