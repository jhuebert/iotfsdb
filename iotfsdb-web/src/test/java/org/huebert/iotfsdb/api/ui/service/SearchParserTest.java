package org.huebert.iotfsdb.api.ui.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

@ExtendWith(MockitoExtension.class)
public class SearchParserTest {

    @Test
    public void testToSearchNull() {
        assertThat(SearchParser.toSearch((FindSeriesRequest) null)).isNull();
    }

    @Test
    public void testToSearchNullDataRequest() {
        assertThat(SearchParser.toSearch((FindDataRequest) null)).isNull();
    }

    @Test
    public void testToSearch() {
        FindSeriesRequest seriesRequest = new FindSeriesRequest();
        seriesRequest.setPattern(Pattern.compile("test-pattern"));
        Map<String, Pattern> metadata = new TreeMap<>();
        metadata.put("a", Pattern.compile("test-a"));
        metadata.put("b", Pattern.compile("test-b"));
        seriesRequest.setMetadata(metadata);
        FindDataRequest dataRequest = new FindDataRequest();
        dataRequest.setSeries(seriesRequest);
        assertThat(SearchParser.toSearch(dataRequest)).isEqualTo("test-pattern && a ~~ test-a && b ~~ test-b");
    }

    @Test
    public void testFromSearchBlank() {
        FindSeriesRequest request = SearchParser.fromSearch("");
        assertThat(request.getPattern().toString()).isEqualTo(".*");
        assertThat(request.getMetadata()).isEqualTo(Map.of());
    }

    @Test
    public void testFromSearch() {
        FindSeriesRequest request = SearchParser.fromSearch("test-pattern && b ~~ test-b && a ~~ test-a");
        assertThat(request.getPattern().toString()).isEqualTo("test-pattern");
        assertThat(request.getMetadata().size()).isEqualTo(2);
        assertThat(request.getMetadata().get("a").toString()).isEqualTo("test-a");
        assertThat(request.getMetadata().get("b").toString()).isEqualTo("test-b");
    }

}
