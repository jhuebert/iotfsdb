@import org.huebert.iotfsdb.schema.Reducer
@import org.huebert.iotfsdb.ui.PlotData
@import java.time.ZonedDateTime
@import java.time.format.DateTimeFormatter

@param PlotData plotData

!{DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");}

<form>
    <input name="search" type="search" placeholder="Search"/>
    <fieldset role="group">
        <input type="datetime-local" name="from" aria-label="From Time" value="${ZonedDateTime.now().minusDays(1).toLocalDateTime().format(formatter)}">
        <input type="datetime-local" name="to" aria-label="To Time" value="${ZonedDateTime.now().toLocalDateTime().format(formatter)}">
        <button hx-post="/ui/data/search" hx-target="#chart-data" hx-swap="innerHTML">Search</button>
    </fieldset>
    <details>
        <summary>Advanced</summary>
        <fieldset class="grid">
            <label>
                Size
                <input type="number" name="size" aria-label="Size" min="1" value="250">
            </label>
            <label>
                Interval (ms)
                <input type="number" name="interval" aria-label="Size" min="1" value="60000">
            </label>
            <label>
                Null Value
                <input type="number" name="nullValue" aria-label="Null Value">
            </label>
        </fieldset>
        <fieldset class="grid">
            <label>
                Time Reducer
                <select name="timeReducer" aria-label="Select the time reducer..." required>
                    @for(Reducer reducer : Reducer.values())
                        <option selected="${reducer == Reducer.AVERAGE}">${reducer}</option>
                    @endfor
                </select>
            </label>
            <label>
                Series Reducer
                <select name="seriesReducer" aria-label="Select the series reducer...">
<%--                    Have text for none--%>
                    <option></option>
                    @for(Reducer reducer : Reducer.values())
                        <option>${reducer}</option>
                    @endfor
                </select>
            </label>
        </fieldset>
        <fieldset class="grid">
            <label>
                Include Null
                <input type="checkbox" role="switch" name="includeNull" aria-label="Include null">
            </label>
            <label>
                Use Big Decimal
                <input type="checkbox" role="switch" name="useBigDecimal" aria-label="Use Big Decimal">
            </label>
            <label>
                Use Previous
                <input type="checkbox" role="switch" name="usePrevious" aria-label="Use Previous">
            </label>
        </fieldset>
    </details>
</form>
<div>
    <hr/>
    <canvas id="plot"></canvas>
    <script>
        const chart = new Chart(document.getElementById('plot'), {
          type: 'line',
          data: {
            labels: [],
            datasets: [],
          },
          options: {
            animation: false,
            cubicInterpolationMode: 'monotone',
            elements: {
              line: {
                spanGaps: true
              }
            },
            scales: {
                x: {
                    type: 'time'
                }
            }
          }
        });
    </script>
    <div id="chart-data">
        @template.data.fragments.script(plotData = plotData)
    </div>
</div>
