@import org.huebert.iotfsdb.schema.DateTimePreset
@import org.huebert.iotfsdb.schema.FindDataRequest
@import org.huebert.iotfsdb.schema.Reducer
@import org.huebert.iotfsdb.ui.service.PlotData
@import org.huebert.iotfsdb.ui.service.SearchParser
@import java.time.ZonedDateTime

@param FindDataRequest request
@param PlotData plotData

<script>
    function dateTimePresetChanged(self) {
      const fromDateTime = document.getElementById('from');
      const toDateTime = document.getElementById('to');
      if (self.value === 'NONE') {
        fromDateTime.disabled = false;
        toDateTime.disabled = false;
      } else {
        fromDateTime.disabled = true;
        toDateTime.disabled = true;
      }
    }
</script>
<form>
    <div class="row mb-3">
        @template.components.search(hxPost = @`/ui/data/search`, hxTarget = "#chart-data", value = SearchParser.toSearch(request))
    </div>
    <div class="row mb-3">
        @template.components.select(id = "dateTimePreset", label = "Preset", onchange="dateTimePresetChanged(this)", values = DateTimePreset.values(), selected = request != null ? request.getDateTimePreset() : DateTimePreset.LAST_24_HOURS)
        @template.components.datetime(id = "from", label = "From", disabled = request == null || (request.getDateTimePreset() != null && request.getDateTimePreset() != DateTimePreset.NONE), value = request != null && request.getFrom() != null ? request.getFrom() : ZonedDateTime.now().minusDays(1))
        @template.components.datetime(id = "to", label = "To", disabled = request == null || (request.getDateTimePreset() != null && request.getDateTimePreset() != DateTimePreset.NONE), value = request != null && request.getTo() != null ? request.getTo() : ZonedDateTime.now())
    </div>
    <div class="row mb-3">
        <details>
            <summary>Additional Parameters</summary>
            <div class="row mt-3 mb-3">
                @template.components.number(id = "size", label = "Size", min = 1, value = request != null ? request.getSize() : 250)
                @template.components.number(id = "interval", label = "Interval (ms)", min = 1L, step = 60000L, value = request != null ? request.getInterval() : 60000L)
                @template.components.number(id = "nullValue", label = "Null Value", value = request != null && request.getNullValue() != null ? request.getNullValue().doubleValue() : null)
            </div>
            <div class="row mb-3">
                @template.components.select(id = "timeReducer", label = "Time Reducer", values = Reducer.values(), selected = request != null ? request.getTimeReducer() : Reducer.AVERAGE)
                @template.components.select(id = "seriesReducer", label = "Series Reducer", emptyOption = true, values = Reducer.values(), selected = request != null ? request.getSeriesReducer() : null)
            </div>
            <div class="row mb-3">
                @template.components.switch(id = "includeNull", label = "Include Null", checked = request != null && request.isIncludeNull())
                @template.components.switch(id = "useBigDecimal", label = "Use Big Decimal", checked = request != null && request.isUseBigDecimal())
                @template.components.switch(id = "usePrevious", label = "Use Previous", checked = request != null && request.isUsePrevious())
            </div>
        </details>
    </div>
</form>
<div>
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
