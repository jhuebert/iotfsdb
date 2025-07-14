@import org.huebert.iotfsdb.api.schema.DateTimePreset
@import org.huebert.iotfsdb.api.schema.FindDataRequest
@import org.huebert.iotfsdb.api.schema.Reducer
@import org.huebert.iotfsdb.api.ui.service.PlotData
@import org.huebert.iotfsdb.api.ui.service.SearchParser

@param FindDataRequest request
@param PlotData plotData

<script>
  function dateTimePresetChanged(self) {
      const isNone = self.value === 'NONE';
      document.getElementById('from').disabled = !isNone;
      document.getElementById('to').disabled = !isNone;
  }

  let periodicSearchEnabled = false;
  let periodicSearchInterval = null;

  function getPeriodicInterval() {
      const val = parseInt(document.getElementById('interval')?.value || NaN, 10);
      return val > 0 ? val : 60000;
  }

  function togglePeriodicSearch(enabled) {
      periodicSearchEnabled = enabled;
      const toggleButton = document.getElementById('toggle-periodic-search');
      toggleButton.classList.toggle('active', enabled);

      clearInterval(periodicSearchInterval);
      if (enabled) {
          periodicSearchInterval = setInterval(triggerSearch, getPeriodicInterval());
      }
  }

  function triggerSearch() {
      document.querySelector('button.btn-primary i.bi-search')?.parentElement?.click();
  }

  function handleIntervalChange() {
      if (periodicSearchEnabled) {
          togglePeriodicSearch(true);
      }
  }

  function zoomIn() {
      adjustTimeRange(0.75);
  }

  function zoomOut() {
      adjustTimeRange(1.25);
  }

  function moveEarlier() {
      shiftTimeRange(-0.25);
  }

  function moveLater() {
      shiftTimeRange(0.25);
  }

  function adjustTimeRange(factor) {
      const [fromInput, toInput] = getRangeInputs();
      const [fromTime, toTime] = getTimeRange(fromInput, toInput);

      const middleTime = (fromTime + toTime) / 2;
      const halfRange = ((toTime - fromTime) / 2) * factor;

      updateRangeInputs(fromInput, toInput, middleTime - halfRange, middleTime + halfRange);
      triggerSearch();
  }

  function shiftTimeRange(factor) {
      const [fromInput, toInput] = getRangeInputs();
      const [fromTime, toTime] = getTimeRange(fromInput, toInput);

      const offset = (toTime - fromTime) * factor;
      updateRangeInputs(fromInput, toInput, fromTime + offset, toTime + offset);
      triggerSearch();
  }

  function getRangeInputs() {
      const fromInput = document.getElementById('from');
      const toInput = document.getElementById('to');

      if (fromInput.disabled || toInput.disabled) {
          document.getElementById('dateTimePreset').value = 'NONE';
          dateTimePresetChanged(document.getElementById('dateTimePreset'));
      }

      return [fromInput, toInput];
  }

  function getTimeRange(fromInput, toInput) {
      return [new Date(fromInput.value).getTime(), new Date(toInput.value).getTime()];
  }

  function updateRangeInputs(fromInput, toInput, fromTime, toTime) {
      fromInput.value = new Date(fromTime).toISOString().slice(0, 16);
      toInput.value = new Date(toTime).toISOString().slice(0, 16);
  }

  document.addEventListener('DOMContentLoaded', () => {
      const toggleButton = document.getElementById('toggle-periodic-search');
      const intervalInput = document.getElementById('interval');
      const zoomInButton = document.getElementById('zoom-in-button');
      const zoomOutButton = document.getElementById('zoom-out-button');
      const moveEarlierButton = document.getElementById('move-earlier-button');
      const moveLaterButton = document.getElementById('move-later-button');

      toggleButton.addEventListener('click', () => togglePeriodicSearch(!periodicSearchEnabled));
      intervalInput?.addEventListener('input', handleIntervalChange);
      zoomInButton?.addEventListener('click', zoomIn);
      zoomOutButton?.addEventListener('click', zoomOut);
      moveEarlierButton?.addEventListener('click', moveEarlier);
      moveLaterButton?.addEventListener('click', moveLater);
  });
</script>
<form>
    <div class="row mb-3">
        <div class="col">
            <input class="form-control form-control-sm"
                   name="search"
                   type="search"
                   placeholder="Search"
                   value="${SearchParser.toSearch(request)}"/>
        </div>
        <div class="col-auto ps-0">
            <button class="btn btn-sm btn-primary"
                    hx-post="/ui/data/search"
                    hx-target="#chart-data"
                    hx-on::response-error="handleHtmxResponseError(event)">
                <i class="bi bi-search"></i>
            </button>
            <button class="btn btn-sm btn-secondary"
                    id="zoom-in-button"
                    type="button"
                    title="Zoom In">
                <i class="bi bi-zoom-in"></i>
            </button>
            <button class="btn btn-sm btn-secondary"
                    id="zoom-out-button"
                    type="button"
                    title="Zoom Out">
                <i class="bi bi-zoom-out"></i>
            </button>
            <button class="btn btn-sm btn-secondary"
                    id="move-earlier-button"
                    type="button"
                    title="Move Earlier">
                <i class="bi bi-chevron-left"></i>
            </button>
            <button class="btn btn-sm btn-secondary"
                    id="move-later-button"
                    type="button"
                    title="Move Later">
                <i class="bi bi-chevron-right"></i>
            </button>
            <button class="btn btn-sm btn-secondary"
                    id="toggle-periodic-search"
                    type="button"
                    title="Toggle periodic search">
                <i class="bi bi-clock"></i>
            </button>
            <button class="btn btn-sm btn-secondary"
                    data-bs-toggle="modal"
                    data-bs-target="#advancedModal">
                <i class="bi bi-gear"></i>
            </button>
            <!-- TODO Add left and right and zoom in and zoom out buttons for for navigating time -->
            <div class="modal fade" id="advancedModal" tabindex="-1"
                 aria-labelledby="advancedModalLabel" aria-hidden="true">
                <div class="modal-dialog modal-lg">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title" id="advancedModalLabel">Advanced Options</h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal"
                                    aria-label="Close"></button>
                        </div>
                        <div class="modal-body">
                            <div class="row mb-3">
                                @template.components.select(id = "dateTimePreset", label = "Preset", onchange = "dateTimePresetChanged(this)", values = DateTimePreset.values(), selected = request.getDateTimePreset())
                                @template.components.datetime(id = "from", label = "From", disabled = request.getDateTimePreset() != null && request.getDateTimePreset() != DateTimePreset.NONE, value = request.getRange().lowerEndpoint())
                                @template.components.datetime(id = "to", label = "To", disabled = request.getDateTimePreset() != null && request.getDateTimePreset() != DateTimePreset.NONE, value = request.getRange().upperEndpoint())
                            </div>
                            <div class="row mb-3">
                                @template.components.number(id = "size", label = "Size", min = 1, value = request.getSize())
                                @template.components.number(id = "interval", label = "Interval (ms)", min = 1L, step = 60000L, value = request.getInterval())
                                @template.components.number(id = "nullValue", label = "Null Value", value = request.getNullValue() != null ? request.getNullValue().doubleValue() : null)
                            </div>
                            <div class="row mb-3">
                                @template.components.select(id = "timeReducer", label = "Time Reducer", values = Reducer.values(), selected = request.getTimeReducer())
                                @template.components.select(id = "seriesReducer", label = "Series Reducer", emptyOption = true, values = Reducer.values(), selected = request.getSeriesReducer())
                            </div>
                            <div class="row mb-3">
                                @template.components.switch(id = "includeNull", label = "Include Null", checked = request.isIncludeNull())
                                @template.components.switch(id = "useBigDecimal", label = "Use Big Decimal", checked = request.isUseBigDecimal())
                                @template.components.switch(id = "usePrevious", label = "Use Previous", checked = request.isUsePrevious())
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</form>
<div style="height: calc(100vh - 150px);">
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
            maintainAspectRatio: false,
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
