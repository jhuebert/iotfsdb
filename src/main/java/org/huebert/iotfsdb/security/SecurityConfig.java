package org.huebert.iotfsdb.security;

import org.huebert.iotfsdb.properties.IotfsdbProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.AuthorizeHttpRequestsConfigurer;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableMethodSecurity
public class SecurityConfig {

    private final IotfsdbProperties properties;

    public SecurityConfig(IotfsdbProperties properties) {
        this.properties = properties;
    }

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        Customizer<AuthorizeHttpRequestsConfigurer<HttpSecurity>.AuthorizationManagerRequestMatcherRegistry> authorizer;
        if (properties.getSecurity().isEnabled()) {
            authorizer = a -> a.anyRequest().authenticated();
        } else {
            authorizer = a -> a.anyRequest().permitAll();
        }
        return http.csrf(AbstractHttpConfigurer::disable)
                .formLogin(Customizer.withDefaults())
                .authorizeHttpRequests(authorizer)
                .httpBasic(Customizer.withDefaults())
                .build();
    }

    @Bean
    public UserDetailsService userDetailsService() {
        UserDetails[] userDetails = properties.getSecurity().getUsers().stream()
                .map(up -> User.builder()
                        .username(up.getUsername())
                        .password(up.getPassword())
                        .roles(up.getRoles().stream().map(Enum::name).toArray(String[]::new))
                        .build())
                .toArray(UserDetails[]::new);
        return new InMemoryUserDetailsManager(userDetails);
    }

}
