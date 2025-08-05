package org.huebert.iotfsdb.api.ui;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.servlet.view.RedirectView;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Unit tests for the IndexUiController class.
 */
@ExtendWith(SpringExtension.class)
@WebMvcTest(IndexUiController.class)
class IndexUiControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private IndexUiController indexUiController;

    /**
     * Test that the controller is properly autowired.
     */
    @Test
    void testControllerIsAutowired() {
        assertNotNull(indexUiController, "The controller should be autowired");
    }

    /**
     * Test the getIndex method directly to ensure it returns the correct RedirectView.
     */
    @Test
    void testGetIndexMethodDirectly() {
        // Call the getIndex method directly
        RedirectView redirectView = indexUiController.getIndex();

        // Verify the RedirectView properties
        assertNotNull(redirectView, "RedirectView should not be null");
        assertEquals("/ui/series", redirectView.getUrl(), "Redirect URL should be '/ui/series'");
    }

    /**
     * Test that a GET request to the root UI path (/ui) redirects to the series page.
     */
    @Test
    void testGetIndexEndpoint() throws Exception {
        // Perform a GET request to the /ui endpoint
        mockMvc.perform(get("/ui"))
               .andExpect(status().is3xxRedirection())
               .andExpect(redirectedUrl("/ui/series"));
    }

    /**
     * Test the full redirect flow and verify the response properties.
     */
    @Test
    void testRedirectResponseDetails() throws Exception {
        // Perform a GET request and capture the response
        MvcResult result = mockMvc.perform(get("/ui"))
                                  .andExpect(status().is3xxRedirection())
                                  .andReturn();

        // Verify response details
        String redirectUrl = result.getResponse().getRedirectedUrl();
        assertEquals("/ui/series", redirectUrl, "Response should redirect to '/ui/series'");

        // Check headers
        String locationHeader = result.getResponse().getHeader("Location");
        assertEquals("/ui/series", locationHeader, "Location header should be '/ui/series'");
    }
}
