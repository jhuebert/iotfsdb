package org.huebert.iotfsdb.api.ui.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.huebert.iotfsdb.api.schema.Reducer;

import java.time.ZonedDateTime;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PlotData {

    private List<ZonedDateTime> labels;
    private List<PlotSeries> data;

    public boolean isSeriesReduced() {
        return (data.size() > 1) || (!data.isEmpty() && !Reducer.REDUCED_ID.equals(data.getFirst().getId()));
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PlotSeries {
        private String id;
        private List<Double> values;
    }

}
