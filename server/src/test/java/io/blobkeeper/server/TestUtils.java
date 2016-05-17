package io.blobkeeper.server;

import org.asynchttpclient.Response;
import org.jetbrains.annotations.NotNull;

import static org.testng.Assert.assertEquals;

/**
 * @author Denis Gabaydulin
 * @since 18.05.16
 */
public class TestUtils {
    private TestUtils() {
    }

    public static void assertResponseOk(
            @NotNull Response response,
            @NotNull String expectedBody,
            @NotNull String expectedContentType
    ) {
        assertEquals(response.getStatusCode(), 200);
        assertEquals(response.getResponseBody(), expectedBody);
        assertEquals(response.getContentType(), expectedContentType);
    }
}
