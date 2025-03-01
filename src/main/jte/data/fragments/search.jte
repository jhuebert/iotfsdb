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
    <div class="row mb-2">
        @template.components.search(hxPost = @`/ui/data/search`, hxTarget = "#chart-data", value = SearchParser.toSearch(request), small = true)
    </div>
    <div class="row mb-2">
        @template.components.select(id = "dateTimePreset", small = true, onchange = "dateTimePresetChanged(this)", values = DateTimePreset.values(), selected = request.getDateTimePreset())
        @template.components.datetime(id = "from", small = true, disabled = request.getDateTimePreset() != null && request.getDateTimePreset() != DateTimePreset.NONE, value = request.getRange().lowerEndpoint())
        @template.components.datetime(id = "to", small = true, disabled = request.getDateTimePreset() != null && request.getDateTimePreset() != DateTimePreset.NONE, value = request.getRange().upperEndpoint())
    </div>
    <div class="row mb-2">
        <details>
            <summary><small>Advanced</small></summary>
            <div class="row mt-1 mb-2">
                @template.components.number(id = "size", label = "Size", min = 1, value = request.getSize(), small = true)
                @template.components.number(id = "interval", label = "Interval (ms)", min = 1L, step = 60000L, value = request.getInterval(), small = true)
                @template.components.number(id = "nullValue", label = "Null Value", value = request.getNullValue() != null ? request.getNullValue().doubleValue() : null, small = true)
            </div>
            <div class="row mb-2">
                @template.components.select(id = "timeReducer", small = true, label = "Time Reducer", values = Reducer.values(), selected = request.getTimeReducer())
                @template.components.select(id = "seriesReducer", small = true, label = "Series Reducer", emptyOption = true, values = Reducer.values(), selected = request.getSeriesReducer())
            </div>
            <div class="row mb-2">
                @template.components.switch(id = "includeNull", label = "Include Null", checked = request.isIncludeNull(), small = true)
                @template.components.switch(id = "useBigDecimal", label = "Use Big Decimal", checked = request.isUseBigDecimal(), small = true)
                @template.components.switch(id = "usePrevious", label = "Use Previous", checked = request.isUsePrevious(), small = true)
            </div>
        </details>
    </div>
</form>
<div class="h-100">
    <canvas id="plot" style="width: 100%; height: 100%;"></canvas>
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
            maintainAspectRatio: true,
            responsive: true,
            elements: {
              line: {
                spanGaps: true
              }
            },
            scales: {
                x: {
                    type: 'time'
                },
                y: {
                    position: 'right'
                },
                y2: {
                    position: 'left',
                    afterBuildTicks: (axis) => {
                      axis.ticks = [...axis.chart.scales.y.ticks];
                      axis.min = axis.chart.scales.y.min;
                      axis.max = axis.chart.scales.y.max;
                    }
                }
            }
          }
        });
    </script>
    <div id="chart-data">
        @template.data.fragments.script(plotData = plotData)
    </div>
</div>
