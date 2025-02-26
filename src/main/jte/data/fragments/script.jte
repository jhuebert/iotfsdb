@import org.huebert.iotfsdb.ui.service.PlotData
@import java.time.ZonedDateTime
@import java.time.format.DateTimeFormatter
@import java.util.stream.Collectors

@param PlotData plotData

!{DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");}

<script>
    chart.config.data.labels = [@for(ZonedDateTime label : plotData.getLabels())'${label.format(formatter)}',@endfor];
    chart.config.data.datasets = [@for(PlotData.PlotSeries series : plotData.getData())
        {
            label:'${series.getId()}',
            data:[$unsafe{series.getValues().stream().map(a -> a == null ? "null" : String.valueOf(a)).collect(Collectors.joining(","))}],
            borderWidth: 1,
            pointRadius: 0
        },
    @endfor];
    chart.update();
</script>
