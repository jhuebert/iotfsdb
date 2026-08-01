package org.huebert.iotfsdb.api.web;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest({SpaFallbackController.class, LegacyUiRedirectController.class})
class SpaFallbackTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void testSpaRoutesServeIndex() throws Exception {
        for (String path : new String[]{"/", "/series", "/data", "/transfer"}) {
            mockMvc.perform(get(path))
                .andExpect(status().isOk())
                .andExpect(forwardedUrl("/index.html"));
        }
    }

    @Test
    void testUnknownSingleSegmentPathsServeIndex() throws Exception {
        mockMvc.perform(get("/unknown"))
            .andExpect(status().isOk())
            .andExpect(forwardedUrl("/index.html"));
        mockMvc.perform(get("/series?q=temp.*"))
            .andExpect(status().isOk())
            .andExpect(forwardedUrl("/index.html"));
    }

    @Test
    void testApiPathsAreNotIntercepted() throws Exception {
        // Multi-segment API paths are not matched by the single-segment fallback pattern.
        mockMvc.perform(get("/v2/series/123"))
            .andExpect(status().isNotFound());
        mockMvc.perform(get("/v3/api-docs"))
            .andExpect(status().isNotFound());
    }

    @Test
    void testAssetPathsAreNotIntercepted() throws Exception {
        // Paths with a file extension must never be forwarded to the SPA shell.
        mockMvc.perform(get("/assets/index-abc123.js"))
            .andExpect(status().isNotFound());
        mockMvc.perform(get("/assets/app.css"))
            .andExpect(status().isNotFound());
    }

    @Test
    void testLegacyUiRedirects() throws Exception {
        mockMvc.perform(get("/ui"))
            .andExpect(status().isMovedPermanently())
            .andExpect(redirectedUrl("/"));
        mockMvc.perform(get("/ui/series"))
            .andExpect(status().isMovedPermanently())
            .andExpect(redirectedUrl("/series"));
        mockMvc.perform(get("/ui/data"))
            .andExpect(status().isMovedPermanently())
            .andExpect(redirectedUrl("/data"));
        mockMvc.perform(get("/ui/transfer"))
            .andExpect(status().isMovedPermanently())
            .andExpect(redirectedUrl("/transfer"));
    }
}
