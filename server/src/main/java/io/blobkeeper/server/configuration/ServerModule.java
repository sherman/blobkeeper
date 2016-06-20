package io.blobkeeper.server.configuration;

/*
 * Copyright (C) 2016 by Denis M. Gabaydulin
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import io.blobkeeper.cluster.configuration.ClusterModule;
import io.blobkeeper.cluster.service.RepairServiceImpl;
import io.blobkeeper.common.configuration.ClassToTypeLiteralMatcherAdapter;
import io.blobkeeper.file.configuration.FileModule;
import io.blobkeeper.server.handler.FileWriterHandler;

import static com.google.inject.matcher.Matchers.subclassesOf;

public class ServerModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new ClusterModule());
        install(new FileModule());

        binder().bindListener(new ClassToTypeLiteralMatcherAdapter(subclassesOf(FileWriterHandler.class)), new TypeListener() {
            @Override
            public <I> void hear(TypeLiteral<I> typeLiteral, TypeEncounter<I> typeEncounter) {
                typeEncounter.register(
                        (InjectionListener<I>) injectedObject -> {
                            FileWriterHandler handler = (FileWriterHandler) injectedObject;
                            handler.init();
                        }
                );
            }
        });

        binder().bindListener(new ClassToTypeLiteralMatcherAdapter(subclassesOf(RepairServiceImpl.class)), new TypeListener() {
            @Override
            public <I> void hear(TypeLiteral<I> typeLiteral, TypeEncounter<I> typeEncounter) {
                typeEncounter.register(
                        (InjectionListener<I>) injectedObject -> {
                            RepairServiceImpl service = (RepairServiceImpl) injectedObject;
                            service.init();
                        }
                );
            }
        });
    }
}
