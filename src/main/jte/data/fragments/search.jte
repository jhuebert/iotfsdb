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
    <div class="row mb-1">
        @template.components.search(hxPost = @`/ui/data/search`, hxTarget = "#chart-data", value = SearchParser.toSearch(request), small = true)
    </div>
    <div class="row mb-1">
        <div class="col col-md-auto">
            @template.components.select(id = "dateTimePreset", small = true, onchange="dateTimePresetChanged(this)", values = DateTimePreset.values(), selected = request != null ? request.getDateTimePreset() : DateTimePreset.LAST_24_HOURS)
        </div>
        <div class="col">
            @template.components.datetime(id = "from", small = true, disabled = request == null || (request.getDateTimePreset() != null && request.getDateTimePreset() != DateTimePreset.NONE), value = request != null && request.getFrom() != null ? request.getFrom() : ZonedDateTime.now().minusDays(1))
        </div>
        <div class="col">
            @template.components.datetime(id = "to", small = true, disabled = request == null || (request.getDateTimePreset() != null && request.getDateTimePreset() != DateTimePreset.NONE), value = request != null && request.getTo() != null ? request.getTo() : ZonedDateTime.now())
        </div>
    </div>
    <div class="row mb-1">
        <details>
            <summary><small>Advanced</small></summary>
            <div class="row mt-1 mb-1">
                @template.components.number(id = "size", label = "Size", min = 1, value = request != null ? request.getSize() : 250, small = true)
                @template.components.number(id = "interval", label = "Interval (ms)", min = 1L, step = 60000L, value = request != null ? request.getInterval() : 60000L, small = true)
                @template.components.number(id = "nullValue", label = "Null Value", value = request != null && request.getNullValue() != null ? request.getNullValue().doubleValue() : null, small = true)
            </div>
            <div class="row mb-1">
                @template.components.select(id = "timeReducer", small = true, label = "Time Reducer", values = Reducer.values(), selected = request != null ? request.getTimeReducer() : Reducer.AVERAGE)
                @template.components.select(id = "seriesReducer", small = true, label = "Series Reducer", emptyOption = true, values = Reducer.values(), selected = request != null ? request.getSeriesReducer() : null)
            </div>
            <div class="row mb-1">
                @template.components.switch(id = "includeNull", label = "Include Null", checked = request != null && request.isIncludeNull(), small = true)
                @template.components.switch(id = "useBigDecimal", label = "Use Big Decimal", checked = request != null && request.isUseBigDecimal(), small = true)
                @template.components.switch(id = "usePrevious", label = "Use Previous", checked = request != null && request.isUsePrevious(), small = true)
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
