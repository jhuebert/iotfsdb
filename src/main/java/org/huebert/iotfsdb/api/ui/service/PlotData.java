package org.huebert.iotfsdb.api.ui.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.ZonedDateTime;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PlotData {

    private List<ZonedDateTime> labels;
    private List<PlotSeries> data;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PlotSeries {
        private String id;
        private List<Double> values;
    }

}
