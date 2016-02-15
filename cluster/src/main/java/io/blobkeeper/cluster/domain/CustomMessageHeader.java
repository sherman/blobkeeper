package io.blobkeeper.cluster.domain;

/*
 * Copyright (C) 2015 by Denis M. Gabaydulin
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

import org.jgroups.Global;
import org.jgroups.Header;

import java.io.DataInput;
import java.io.DataOutput;

public class CustomMessageHeader extends Header {
    public static final short CUSTOM_MESSAGE_HEADER = 1888;

    private Command command;

    public CustomMessageHeader() {
    }

    public CustomMessageHeader(Command command) {
        this.command = command;
    }

    @Override
    public int size() {
        return Global.INT_SIZE;
    }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        out.writeInt(command.ordinal());
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        int type = in.readInt();
        this.command = Command.fromOrdinal(type);
    }

    public Command getCommand() {
        return command;
    }
}
