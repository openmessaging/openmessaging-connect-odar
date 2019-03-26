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

import io.openmessaging.connector.api.data.EntryType;
import io.openmessaging.mysql.schema.Table;
import io.openmessaging.mysql.schema.column.ColumnParser;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataRow {

    private Logger logger = LoggerFactory.getLogger(DataRow.class);

    private EntryType type;
    private Table table;
    private Serializable[] rowBeforeUpdate;
    private Serializable[] row;

    public DataRow(EntryType type, Table table, Serializable[] row, Serializable[] rowBeforeUpdate) {
        this.type = type;
        this.table = table;
        this.row = row;
        this.rowBeforeUpdate = rowBeforeUpdate;
    }

    public Map toMap() {

        try {
            if (table.getColList().size() == row.length) {
                Map<String, Object> beforeDataMap = new HashMap<>();
                Map<String, Object> dataMap = new HashMap<>();
                List<String> keyList = table.getColList();
                List<ColumnParser> parserList = table.getParserList();

                for (int i = 0; i < keyList.size(); i++) {
                    ColumnParser parser = parserList.get(i);
                    if(null != row){
                        Object value = row[i];
                        dataMap.put(keyList.get(i), parser.getValue(value));
                    }
                    if(null != rowBeforeUpdate){
                        beforeDataMap.put(keyList.get(i), parser.getValue(rowBeforeUpdate[i]));
                    }
                }

                Map<String, Object> map = new HashMap<>();
                map.put("database", table.getDatabase());
                map.put("table", table.getName());
                map.put("type", type);
                if(dataMap.size() > 0){
                    map.put("data", dataMap);
                }
                if(beforeDataMap.size() > 0){
                    map.put("beforeData", beforeDataMap);
                }

                return map;
            } else {
                logger.error("Table schema changed,discard data: {} - {}, {}  {}",
                    table.getDatabase().toUpperCase(), table.getName().toUpperCase(), type, row.toString());

                return null;
            }
        } catch (Exception e) {
            logger.error("Row parse error,discard data: {} - {}, {}  {}",
                table.getDatabase().toUpperCase(), table.getName().toUpperCase(), type, row.toString());
        }

        return null;
    }

    public EntryType getType() {
        return type;
    }

    public Table getTable() {
        return table;
    }

    public Serializable[] getRow() {
        return row;
    }

    public Serializable[] getRowBeforeUpdate() {
        return rowBeforeUpdate;
    }
}
