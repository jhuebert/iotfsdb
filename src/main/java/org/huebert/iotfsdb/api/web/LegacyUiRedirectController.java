package org.huebert.iotfsdb.api.web;

import io.swagger.v3.oas.annotations.Hidden;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.net.URI;

/**
 * Legacy {@code /ui*} links are permanently redirected to their new root-path
 * equivalents so bookmarks and old links keep working.
 */
@Controller
@RequestMapping("/ui")
@Hidden
@ConditionalOnExpression("${iotfsdb.api.ui:true}")
public class LegacyUiRedirectController {

    @GetMapping
    public ResponseEntity<Void> redirectRoot() {
        return redirect("/");
    }

    @GetMapping("series")
    public ResponseEntity<Void> redirectSeries() {
        return redirect("/series");
    }

    @GetMapping("data")
    public ResponseEntity<Void> redirectData() {
        return redirect("/data");
    }

    @GetMapping("transfer")
    public ResponseEntity<Void> redirectTransfer() {
        return redirect("/transfer");
    }

    private ResponseEntity<Void> redirect(String location) {
        return ResponseEntity.status(HttpStatus.MOVED_PERMANENTLY)
            .location(URI.create(location))
            .build();
    }
}
