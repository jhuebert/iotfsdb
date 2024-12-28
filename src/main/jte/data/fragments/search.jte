@import org.huebert.iotfsdb.schema.DateTimePreset
@import org.huebert.iotfsdb.schema.FindDataRequest
@import org.huebert.iotfsdb.schema.Reducer
@import org.huebert.iotfsdb.ui.PlotData
@import org.huebert.iotfsdb.ui.SearchParser
@import java.time.ZonedDateTime
@import java.time.format.DateTimeFormatter

@param FindDataRequest request
@param PlotData plotData

!{DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");}

<script>
function dateTimePresetChanged(self) {
  const fromDateTime = document.getElementById('fromDateTime');
  const toDateTime = document.getElementById('toDateTime');
  if (self.value === 'NONE') {
    fromDateTime.style.display = 'block';
    toDateTime.style.display = 'block';
  } else {
    fromDateTime.style.display = 'none';
    toDateTime.style.display = 'none';
  }
}
</script>
<form>
    <fieldset role="group">
        <input name="search" type="search" placeholder="Search" value="${request != null ? SearchParser.toSearch(request.getSeries()) : null}"/>
        <button hx-post="/ui/data/search" hx-target="#chart-data" hx-swap="innerHTML">Search</button>
    </fieldset>
    <details>
        <summary>Time</summary>
        <label>
            Preset
            <select name="dateTimePreset" aria-label="Select the date time preset..." onchange="dateTimePresetChanged(this)">
                @for(DateTimePreset preset : DateTimePreset.values())
                    <option selected="${request != null ? preset == request.getDateTimePreset() : preset == DateTimePreset.LAST_24_HOURS}">${preset}</option>
                @endfor
            </select>
        </label>
        <fieldset>
            <input id="fromDateTime" style="display: ${request == null ? "none" : (request.getDateTimePreset() == null || request.getDateTimePreset() == DateTimePreset.NONE ? "block" : "none")}" type="datetime-local" name="from" aria-label="From Time" value="${request != null ? request.getFrom().toLocalDateTime().format(formatter) : ZonedDateTime.now().minusDays(1).toLocalDateTime().format(formatter)}">
            <input id="toDateTime" style="display: ${request == null ? "none" : (request.getDateTimePreset() == null || request.getDateTimePreset() == DateTimePreset.NONE ? "block" : "none")}"  type="datetime-local" name="to" aria-label="To Time" value="${request != null ? request.getTo().toLocalDateTime().format(formatter) : ZonedDateTime.now().toLocalDateTime().format(formatter)}">
        </fieldset>
    </details>
    <details>
        <summary>Advanced</summary>
        <fieldset class="grid">
            <label>
                Size
                <input type="number" name="size" aria-label="Size" min="1" value="${request != null ? request.getSize() : 250}">
            </label>
            <label>
                Interval (ms)
                <input type="number" name="interval" aria-label="Size" min="1" value="${request != null ? request.getInterval() : 60000}">
            </label>
            <label>
                Null Value
                <input type="number" name="nullValue" aria-label="Null Value" value="${request != null ? request.getNullValue() : null}">
            </label>
        </fieldset>
        <fieldset class="grid">
            <label>
                Time Reducer
                <select name="timeReducer" aria-label="Select the time reducer..." required>
                    @for(Reducer reducer : Reducer.values())
                        <option selected="${request != null ? reducer == request.getTimeReducer() : reducer == Reducer.AVERAGE}">${reducer}</option>
                    @endfor
                </select>
            </label>
            <label>
                Series Reducer
                <select name="seriesReducer" aria-label="Select the series reducer...">
                    <option></option>
                    @for(Reducer reducer : Reducer.values())
                        <option selected="${request != null && reducer == request.getSeriesReducer()}">${reducer}</option>
                    @endfor
                </select>
            </label>
        </fieldset>
        <fieldset class="grid">
            <label>
                Include Null
                <input type="checkbox" role="switch" name="includeNull" aria-label="Include null" checked="${request != null && request.isIncludeNull()}">
            </label>
            <label>
                Use Big Decimal
                <input type="checkbox" role="switch" name="useBigDecimal" aria-label="Use Big Decimal" checked="${request != null && request.isUseBigDecimal()}">
            </label>
            <label>
                Use Previous
                <input type="checkbox" role="switch" name="usePrevious" aria-label="Use Previous" checked="${request != null && request.isUsePrevious()}">
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
