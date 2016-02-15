package io.blobkeeper.index.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.blobkeeper.index.domain.CacheKey;
import io.blobkeeper.index.domain.IndexElt;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;


/**
 * @author Denis Gabaydulin
 * @since 15/02/2016
 */
@Singleton
public class IndexCacheServiceImpl implements IndexCacheService {
    private static final Logger log = LoggerFactory.getLogger(IndexCacheServiceImpl.class);

    private final Cache<CacheKey, IndexElt> cache = CacheBuilder.newBuilder()
            // TODO: move to config
            .maximumSize(1048576)
            .build();

    @Override
    public IndexElt getById(@NotNull CacheKey key) {
        return cache.getIfPresent(key);
    }

    @Override
    public void set(@NotNull IndexElt elt) {
        cache.put(new CacheKey(elt.getId(), elt.getType()), elt);
    }

    @Override
    public void remove(@NotNull CacheKey key) {
        cache.invalidate(key);
    }

    @Override
    public void clear() {
        cache.cleanUp();
    }
}
