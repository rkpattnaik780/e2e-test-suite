package io.managed.services.test.client.github;

import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;

public class BearerAuthFilter implements ClientRequestFilter {
    private final String token;

    public BearerAuthFilter(String token) {
        this.token = token;
    }

    @Override
    public void filter(ClientRequestContext requestContext) {
        String authHeader;
        if (!token.startsWith("Bearer ")) {
            authHeader = "Bearer " + token;
        } else {
            authHeader = token;
        }

        requestContext.getHeaders().add("Authorization", authHeader);
    }
}
