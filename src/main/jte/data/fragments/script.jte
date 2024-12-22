@import org.apache.logging.log4j.util.Strings
@import org.huebert.iotfsdb.ui.PlotData
@import java.time.ZonedDateTime
@import java.time.format.DateTimeFormatter

@param PlotData plotData

!{DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");}

<script>
    chart.config.data.labels = [
        @for(ZonedDateTime label : plotData.getLabels())
            '${label.format(formatter)}',
        @endfor
    ];
    chart.config.data.datasets = [
    @for(PlotData.PlotSeries series : plotData.getData())
    {
        label: '${series.getId()}',
        data: [${Strings.join(series.getValues(), ',')}]
    },
    @endfor
    ];
    chart.update();
</script>
