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

      if (enabled) {
          toggleButton.classList.add('btn-success');
          toggleButton.classList.remove('btn-secondary');
      } else {
          toggleButton.classList.add('btn-secondary');
          toggleButton.classList.remove('btn-success');
      }

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

  function downloadChartCSV() {

      // Get chart data
      const chartData = chart.data;
      const labels = chartData.labels;
      const datasets = chartData.datasets;

      // Create CSV header with timestamp and series names
      let csvContent = "Timestamp";
      datasets.forEach(dataset => {
          csvContent += "," + dataset.label;
      });
      csvContent += "\n";

      // Add data rows
      for (let i = 0; i < labels.length; i++) {
          csvContent += labels[i];
          datasets.forEach(dataset => {
              csvContent += "," + (dataset.data[i] === null ? "" : dataset.data[i]);
          });
          csvContent += "\n";
      }

      // Create and trigger download
      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.setAttribute('href', url);
      link.setAttribute('download', 'chart_data_' + new Date().toISOString().slice(0, 19).replace(/:/g, '-') + '.csv');
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
  }

  function downloadChartAsImage() {

      // Create an image from the canvas
      const image = chart.toBase64Image();

      // Create and trigger download
      const link = document.createElement('a');
      link.setAttribute('href', image);
      link.setAttribute('download', 'chart_image_' + new Date().toISOString().slice(0, 19).replace(/:/g, '-') + '.png');
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
  }

  const zoomFactor = 0.5;

  function zoomIn() {
      adjustTimeRange(zoomFactor);
  }

  function zoomOut() {
      adjustTimeRange(1.0 / zoomFactor);
  }

  const moveFactor = 0.25;

  function moveEarlier() {
      shiftTimeRange(-moveFactor);
  }

  function moveLater() {
      shiftTimeRange(moveFactor);
  }

  function adjustTimeRange(factor) {
      const [fromInput, toInput] = getRangeInputs();
      const [fromTime, toTime] = getTimeRange(fromInput, toInput);
      const midpoint = (fromTime + toTime) / 2;
      const halfRange = ((toTime - fromTime) / 2) * factor;
      updateRangeInputs(fromInput, toInput, midpoint - halfRange, midpoint + halfRange);
  }

  function shiftTimeRange(factor) {
      const [fromInput, toInput] = getRangeInputs();
      const [fromTime, toTime] = getTimeRange(fromInput, toInput);
      const offset = (toTime - fromTime) * factor;
      updateRangeInputs(fromInput, toInput, fromTime + offset, toTime + offset);
  }

  function getRangeInputs() {
      const fromInput = document.getElementById('from');
      const toInput = document.getElementById('to');

      if (fromInput.disabled || toInput.disabled) {
          document.getElementById('dateTimePreset').value = 'NONE';
          //TODO Need to set the from and to values based on the preset
          dateTimePresetChanged(document.getElementById('dateTimePreset'));
      }

      return [fromInput, toInput];
  }

  function getTimeRange(fromInput, toInput) {
      return [getTime(fromInput.value), getTime(toInput.value)];
  }

  function getTime(input) {
      const local = new Date(input);
      return new Date(Date.UTC(
          local.getFullYear(),
          local.getMonth(),
          local.getDate(),
          local.getHours(),
          local.getMinutes(),
          local.getSeconds(),
          local.getMilliseconds()
      )).getTime();
  }

  function getTimeString(input) {
      return String(input.getUTCFullYear()).padStart(4, "0") + "-" +
          String(input.getUTCMonth() + 1).padStart(2, "0") + "-" +
          String(input.getUTCDate()).padStart(2, "0") + "T" +
          String(input.getUTCHours()).padStart(2, "0") + ":" +
          String(input.getUTCMinutes()).padStart(2, "0") + ":" +
          String(input.getUTCSeconds()).padStart(2, "0");
  }

  function updateRangeInputs(fromInput, toInput, fromTime, toTime) {
      const fromDate = new Date(fromTime);
      const toDate = new Date(toTime);
      if (
          ((toTime - fromTime) > 1000) &&
          (fromDate.getUTCFullYear() > 1900) && (toDate.getUTCFullYear() > 1900) &&
          (fromDate.getUTCFullYear() < 2100) && (toDate.getUTCFullYear() < 2100)
      ) {
          fromInput.value = getTimeString(fromDate);
          toInput.value = getTimeString(toDate);
          triggerSearch();
      }
  }

  document.addEventListener('DOMContentLoaded', () => {
      const toggleButton = document.getElementById('toggle-periodic-search');
      const intervalInput = document.getElementById('interval');
      const zoomInButton = document.getElementById('zoom-in-button');
      const zoomOutButton = document.getElementById('zoom-out-button');
      const moveEarlierButton = document.getElementById('move-earlier-button');
      const moveLaterButton = document.getElementById('move-later-button');
      const searchForm = document.querySelector('form');
      const searchInput = document.querySelector('input[name="search"]');

      // Prevent default form submission and handle Enter key in search input
      searchForm.addEventListener('submit', (e) => {
          e.preventDefault();
          triggerSearch();
      });

      // Add event listener for Enter key on search input as a backup
      searchInput.addEventListener('keydown', (e) => {
          if (e.key === 'Enter') {
              e.preventDefault();
              triggerSearch();
          }
      });

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
        <div class="col pe-1">
            <div class="input-group input-group-sm">
                <input class="form-control"
                       name="search"
                       type="search"
                       placeholder="Search"
                       value="${SearchParser.toSearch(request)}"/>
                <button class="btn btn-secondary"
                        id="toggle-periodic-search"
                        type="button"
                        title="Toggle periodic search">
                    <i class="bi bi-arrow-repeat"></i>
                </button>
                <button class="btn btn-secondary"
                        data-bs-toggle="modal"
                        data-bs-target="#advancedModal"
                        title="Advanced Options">
                    <i class="bi bi-gear"></i>
                </button>
                <button class="btn btn-primary"
                        title="Execute Search"
                        hx-post="/ui/data/search"
                        hx-target="#chart-data"
                        hx-on::response-error="handleHtmxResponseError(event)">
                    <i class="bi bi-search"></i>
                </button>
            </div>
        </div>
        <div class="col-auto ps-0">
            <div class="btn-group btn-group-sm">
                <button class="btn btn-secondary"
                        id="move-earlier-button"
                        type="button"
                        title="Move Earlier">
                    <i class="bi bi-chevron-left"></i>
                </button>
                <button class="btn btn-secondary"
                        id="move-later-button"
                        type="button"
                        title="Move Later">
                    <i class="bi bi-chevron-right"></i>
                </button>
            </div>
            <div class="btn-group btn-group-sm">
                <button class="btn btn-secondary"
                        id="zoom-out-button"
                        type="button"
                        title="Zoom Out">
                    <i class="bi bi-zoom-out"></i>
                </button>
                <button class="btn btn-secondary"
                        id="zoom-in-button"
                        type="button"
                        title="Zoom In">
                    <i class="bi bi-zoom-in"></i>
                </button>
            </div>
            <div class="btn-group btn-group-sm">
                <button class="btn btn-secondary"
                        id="download-csv-button"
                        type="button"
                        title="Download CSV"
                        onclick="downloadChartCSV()">
                    <i class="bi bi-file-earmark-spreadsheet"></i>
                </button>
                <button class="btn btn-secondary"
                        id="download-image-button"
                        type="button"
                        title="Download Image"
                        onclick="downloadChartAsImage()">
                    <i class="bi bi-file-earmark-image"></i>
                </button>
            </div>
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
                            </div>
                            <div class="row mb-3">
                                @template.components.number(id = "nullValue", label = "Null Value", value = request.getNullValue() != null ? request.getNullValue().doubleValue() : null)
                                <div class="col d-flex align-items-center">
                                    @template.components.switch(id = "usePrevious", label = "Use Previous", checked = request.isUsePrevious())
                                </div>
                            </div>
                            <div class="row mb-3">
                                @template.components.select(id = "timeReducer", label = "Time Reducer", values = Reducer.values(), selected = request.getTimeReducer())
                                @template.components.select(id = "seriesReducer", label = "Series Reducer", emptyOption = true, values = Reducer.values(), selected = request.getSeriesReducer())
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
            plugins: {
              legend: {
                labels: {
                  usePointStyle: true,
                },
                onHover: (evt, legendItem, legend) => {
                  const chart = legend.chart;
                  chart.data.datasets[legendItem.datasetIndex].borderWidth = 3;
                  chart.update();
                },
                onLeave: (evt, legendItem, legend) => {
                  const chart = legend.chart;
                  chart.data.datasets[legendItem.datasetIndex].borderWidth = 1;
                  chart.update();
                }
              },
                tooltip: {
                    usePointStyle: true,
                }
            },
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
                    border: {
                        color: '#373737'
                    },
                    grid: {
                        color: '#373737'
                    },
                    type: 'time',
                    ticks: {
                        autoSkip: true,
                        autoSkipPadding: 50,
                        maxRotation: 0,
                    },
                    time: {
                        tooltipFormat: 'YYYY-MM-DD H:mm:ss.SSS',
                        displayFormats: {
                            millisecond: 'H:mm:ss.SSS',
                            second: 'H:mm:ss',
                            minute: 'M/D H:mm',
                            hour: 'M/D H:mm',
                            day: 'M/D',
                            week: 'M/D',
                            month: 'M/YYYY',
                            quarter: 'M/YYYY',
                            year: 'YYYY',
                        }
                    }
                },
                y: {
                    grid: {
                        color: '#373737'
                    },
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
