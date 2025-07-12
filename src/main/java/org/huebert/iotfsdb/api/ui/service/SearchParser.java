package org.huebert.iotfsdb.api.ui.service;

import org.apache.logging.log4j.util.Strings;
import org.huebert.iotfsdb.api.schema.FindDataRequest;
import org.huebert.iotfsdb.api.schema.FindSeriesRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class SearchParser {

    public static FindSeriesRequest fromSearch(String search) {
        FindSeriesRequest request = new FindSeriesRequest();
        if (Strings.isNotBlank(search)) {
            for (String part : search.split("&&")) {
                String[] metadataParts = part.split("~~", 2);
                if (metadataParts.length == 2) {
                    request.getMetadata().put(metadataParts[0].trim(), Pattern.compile(metadataParts[1].trim()));
                } else {
                    request.setPattern(Pattern.compile(part.trim()));
                }
            }
        }
        return request;
    }

    public static String toSearch(FindDataRequest request) {
        return (request == null) ? null : toSearch(request.getSeries());
    }

    public static String toSearch(FindSeriesRequest request) {
        if (request == null) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        parts.add(request.getPattern().toString());
        request.getMetadata().forEach((key, value) -> parts.add(key + " ~~ " + value));
        return String.join(" && ", parts);
    }

}
