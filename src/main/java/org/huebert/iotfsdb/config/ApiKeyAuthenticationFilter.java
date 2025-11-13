package org.huebert.iotfsdb.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
public class ApiKeyAuthenticationFilter extends OncePerRequestFilter {
    
    @Autowired
    private org.huebert.iotfsdb.IotfsdbProperties iotfsdbProperties;
    
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        
        String apiKey = request.getHeader(SimpleSecurityConfig.API_KEY_HEADER);
        
        // Check if this is a REST API request (v2 endpoints)
        if (request.getRequestURI().startsWith("/v2/") && apiKey != null) {
            String expectedApiKey = iotfsdbProperties.getSecurity().getApiKeys().getRest();
            if (expectedApiKey != null && expectedApiKey.equals(apiKey)) {
                // Create authentication token for REST API
                UsernamePasswordAuthenticationToken authToken = new UsernamePasswordAuthenticationToken("api-rest", "apiKey");
                SecurityContextHolder.getContext().setAuthentication(authToken);
            }
        }
        
        filterChain.doFilter(request, response);
    }
}