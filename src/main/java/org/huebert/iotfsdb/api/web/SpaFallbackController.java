package org.huebert.iotfsdb.api.web;

import io.swagger.v3.oas.annotations.Hidden;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Serves the single-page application.
 *
 * <p>The SPA uses root paths ({@code /}, {@code /series}, {@code /data},
 * {@code /transfer}) with client-side routing, so every non-API GET without a
 * file extension returns the SPA shell ({@code index.html}).
 *
 * <p>API prefixes ({@code /v2/**}, {@code /swagger-ui/**}, {@code /v3/**}),
 * asset paths (anything containing a {@code .}) and other multi-segment paths
 * are never intercepted here: they are handled by their own controllers or
 * static resource handlers, and the {@code {path:[^\\.]*}} pattern below cannot
 * match dots.
 */
@Controller
@Hidden
@ConditionalOnExpression("${iotfsdb.api.ui:true}")
public class SpaFallbackController {

    @GetMapping({"/", "/series", "/data", "/transfer", "/{path:[^\\.]*}"})
    public String spaFallback() {
        return "forward:/index.html";
    }
}
