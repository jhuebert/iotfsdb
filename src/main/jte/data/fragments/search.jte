@import org.huebert.iotfsdb.ui.PlotData

@param PlotData plotData

<form role="search">
    <input name="search" type="search" placeholder="Search"/>
    <button hx-post="/ui/data/search" hx-target="#drawer" hx-swap="innerHTML">Search</button>
</form>
<div>
    <hr/>
    <canvas id="plot"></canvas>
    <script>
        const plotCanvas = document.getElementById('plot');
        const chart = new Chart(plotCanvas, {
          type: 'line',
          data: {
            labels: [],
            datasets: [],
          },
          options: {
          cubicInterpolationMode: 'monotone',
            elements: {
              line: {
                spanGaps: true
              },
              point: {
                pointStyle: 'circle'
              }
            }
  }
        });
    </script>
    <div id="drawer">
        @template.data.fragments.script(plotData = plotData)
    </div>
</div>
