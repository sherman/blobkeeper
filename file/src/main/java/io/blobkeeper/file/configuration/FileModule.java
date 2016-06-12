package io.blobkeeper.file.configuration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.blobkeeper.common.util.GuavaCollectors.toImmutableMap;
import static java.util.function.Function.identity;

public class FileModule extends AbstractModule {
    private static final Logger log = LoggerFactory.getLogger(FileModule.class);

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    public Map<Integer, DiskConfiguration> diskConfigurations(ObjectMapper objectMapper, @Named("blobkeeper.disk.configuration") String value) {
        try {
            List<DiskConfiguration> config = objectMapper.readValue(value, new TypeReference<List<DiskConfiguration>>() {
            });

            return config.stream().
                    collect(toImmutableMap(DiskConfiguration::getDisk, identity()));
        } catch (IOException e) {
            log.error("Can't read disk configuration");
            throw new IllegalArgumentException(e);
        }
    }
}
