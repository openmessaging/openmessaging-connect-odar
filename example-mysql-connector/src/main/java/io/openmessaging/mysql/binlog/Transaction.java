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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.openmessaging.mysql.binlog;

import com.alibaba.fastjson.JSONObject;
import io.openmessaging.connector.api.data.EntryType;
import io.openmessaging.mysql.Config;
import io.openmessaging.mysql.position.BinlogPosition;
import io.openmessaging.mysql.schema.Table;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Transaction {

    private BinlogPosition nextBinlogPosition;
    private Long xid;

    private Config config;

    private List<DataRow> list = new LinkedList<>();

    public Transaction(Config config) {
        this.config = config;
    }

    public boolean addRow(EntryType type, Table table, Serializable[] row, Serializable[] rowBeforeUpdate) {

        if (list.size() == config.maxTransactionRows) {
            return false;
        } else {
            DataRow dataRow = new DataRow(type, table, row, rowBeforeUpdate);
            list.add(dataRow);
            return true;
        }

    }

    public List<DataRow> getDataRows(){
        return list;
    }

    public String toJson() {

        List<Map> rows = new LinkedList<>();
        for (DataRow dataRow : list) {
            Map rowMap = dataRow.toMap();
            if (rowMap != null) {
                rows.add(rowMap);
            }
        }

        Map<String, Object> map = new HashMap<>();
        map.put("xid", xid);
        map.put("binlogFilename", nextBinlogPosition.getBinlogFilename());
        map.put("nextPosition", nextBinlogPosition.getPosition());
        map.put("rows", rows);

        return JSONObject.toJSONString(map);
    }

    public BinlogPosition getNextBinlogPosition() {
        return nextBinlogPosition;
    }

    public void setNextBinlogPosition(BinlogPosition nextBinlogPosition) {
        this.nextBinlogPosition = nextBinlogPosition;
    }

    public void setXid(Long xid) {
        this.xid = xid;
    }

    public Long getXid() {
        return xid;
    }
}