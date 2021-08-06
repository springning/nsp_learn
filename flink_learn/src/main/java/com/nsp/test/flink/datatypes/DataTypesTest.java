package com.nsp.test.flink.datatypes;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class DataTypesTest {
    public static void main(String[] args) {
        DataType interval = DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND());

        // tell the runtime to not produce or consume java.time.LocalDateTime instances
        // but java.sql.Timestamp
        DataType t = DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class);

        // tell the runtime to not produce or consume boxed integer arrays
        // but primitive int arrays
        t = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(int[].class);
    }
}
