package io.openmessaging.mysql;

public class MysqlConstants {

    public static final String BINLOG_FILENAME = "binlogFilename";

    public static final String NEXT_POSITION = "nextPosition";

    public static String getPartition(String mysqlAddr, int mysqlPort){
        return mysqlAddr+mysqlPort;
    }
}
