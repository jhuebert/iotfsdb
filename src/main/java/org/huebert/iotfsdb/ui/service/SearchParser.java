package org.huebert.iotfsdb.ui.service;

import org.apache.logging.log4j.util.Strings;
import org.huebert.iotfsdb.schema.FindSeriesRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SearchParser {

    public static FindSeriesRequest fromSearch(String search) {
        FindSeriesRequest request = new FindSeriesRequest();
        if (!Strings.isBlank(search)) {
            for (String part : search.split("&&")) {
                if (part.contains("~~")) {
                    String[] metadataParts = part.split("~~");
                    request.getMetadata().put(metadataParts[0].trim(), Pattern.compile(metadataParts[1].trim()));
                } else {
                    request.setPattern(Pattern.compile(part.trim()));
                }
            }
        }
        return request;
    }

    public static String toSearch(FindSeriesRequest request) {
        List<String> parts = new ArrayList<>();
        parts.add(request.getPattern().toString());
        for (Map.Entry<String, Pattern> entry : request.getMetadata().entrySet()) {
            parts.add(entry.getKey() + " ~~ " + entry.getValue());
        }
        return String.join(" && ", parts);
    }

}
