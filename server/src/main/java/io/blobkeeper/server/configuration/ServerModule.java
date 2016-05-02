package io.blobkeeper.server.configuration;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import io.blobkeeper.common.configuration.ClassToTypeLiteralMatcherAdapter;
import io.blobkeeper.server.handler.FileWriterHandler;

import static com.google.inject.matcher.Matchers.subclassesOf;

/**
 * @author Denis Gabaydulin
 * @since 02.05.16
 */
public class ServerModule extends AbstractModule {
    @Override
    protected void configure() {
        binder().bindListener(new ClassToTypeLiteralMatcherAdapter(subclassesOf(FileWriterHandler.class)), new TypeListener() {
            @Override
            public <I> void hear(TypeLiteral<I> typeLiteral, TypeEncounter<I> typeEncounter) {
                typeEncounter.register(
                        (InjectionListener<I>) injectedObject -> {
                            FileWriterHandler handler = (FileWriterHandler)injectedObject;
                            handler.init();
                        }
                );
            }
        });
    }
}
