package io.blobkeeper.cluster.configuration;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import io.blobkeeper.cluster.util.ReplicationStatistic;
import io.blobkeeper.common.configuration.ClassToTypeLiteralMatcherAdapter;

import static com.google.inject.matcher.Matchers.subclassesOf;

public class ClusterModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new JGroupsModule());

        binder().bindListener(new ClassToTypeLiteralMatcherAdapter(subclassesOf(ReplicationStatistic.class)), new TypeListener() {
            @Override
            public <I> void hear(TypeLiteral<I> typeLiteral, TypeEncounter<I> typeEncounter) {
                typeEncounter.register(
                        (InjectionListener<I>) injectedObject -> {
                            ReplicationStatistic handler = (ReplicationStatistic) injectedObject;
                            handler.init();
                        }
                );
            }
        });
    }
}
