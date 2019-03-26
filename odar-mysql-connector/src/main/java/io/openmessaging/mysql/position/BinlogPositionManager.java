/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.mysql.position;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import io.openmessaging.mysql.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinlogPositionManager {
    private Logger logger = LoggerFactory.getLogger(BinlogPositionManager.class);

    private DataSource dataSource;
    private Config config;

    private String binlogFilename;
    private Long nextPosition;

    public BinlogPositionManager(Config config, DataSource dataSource) {
        this.config = config;
        this.dataSource = dataSource;
    }

    public void initBeginPosition() throws Exception {

        if (config.startType == null || config.startType.equals("DEFAULT")) {
//            initPositionDefault();
            initPositionFromBinlogTail();
        } else if (config.startType.equals("NEW_EVENT")) {
            initPositionFromBinlogTail();

        } else if (config.startType.equals("SPECIFIED")) {
            binlogFilename = config.binlogFilename;
            nextPosition = config.nextPosition;

        }

        if (binlogFilename == null || nextPosition == null) {
            throw new Exception("binlogFilename | nextPosition is null.");
        }
    }

    private void initPositionFromBinlogTail() throws SQLException {
        String sql = "SHOW MASTER STATUS";

        Connection conn = null;
        ResultSet rs = null;

        try {
            Connection connection = dataSource.getConnection();
            rs = connection.createStatement().executeQuery(sql);

            while (rs.next()) {
                binlogFilename = rs.getString("File");
                nextPosition = rs.getLong("Position");
            }

        } finally {

            if (conn != null) {
                conn.close();
            }
            if (rs != null) {
                rs.close();
            }
        }

    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public Long getPosition() {
        return nextPosition;
    }
}
