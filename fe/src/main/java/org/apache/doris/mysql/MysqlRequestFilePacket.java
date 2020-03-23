// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.mysql;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// MySQL protocol request file packet
public class MysqlRequestFilePacket extends MysqlPacket {
    private static final Logger LOG = LogManager.getLogger(MysqlRequestFilePacket.class);

    private static final int REQUEST_FILE_PACKET_INDICATOR = 0XFB;

    private static final int ERROR_PACKET_INDICATOR = 0XFF;

    private String fileName;

    public MysqlRequestFilePacket(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {
        MysqlCapability capability = serializer.getCapability();
        if (capability.isClientLocalFiles()) {
            if (StringUtils.isEmpty(fileName)) {
                serializer.writeInt1(ERROR_PACKET_INDICATOR);
                serializer.writeEofString("The file path cannot be null or empty");
            } else {
                serializer.writeInt1(REQUEST_FILE_PACKET_INDICATOR);
                serializer.writeEofString(fileName);
            }
        } else {
            serializer.writeInt1(ERROR_PACKET_INDICATOR);
            serializer.writeEofString("The client has to set the CLIENT_LOCAL_FILES capability");
        }
    }
}
