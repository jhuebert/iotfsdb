@import java.util.List
@import org.huebert.iotfsdb.schema.SeriesFile

@param List<SeriesFile> series

<form role="search">
    <input name="search" type="search" placeholder="Search"/>
    <button hx-post="/ui/series/search" hx-target="#results" hx-swap="innerHTML">Search</button>
</form>
<div>
    <hr/>
    <div id="results">
        @template.series.fragments.results(series = series)
    </div>
</div>
