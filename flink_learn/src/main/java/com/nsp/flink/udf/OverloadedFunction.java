package com.nsp.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;

public class OverloadedFunction extends ScalarFunction {

    // no hint required
    public Long eval(long a, long b) {
        return a + b;
    }

    // 定义 decimal 的精度和小数位
    public @DataTypeHint("DECIMAL(12, 3)") BigDecimal eval(double a, double b) {
        return BigDecimal.valueOf(a + b);
    }

    // 定义嵌套数据类型
    @DataTypeHint("ROW<s STRING, t TIMESTAMP_LTZ(3)>")
    public Row eval(int i) {
        return Row.of(String.valueOf(i), Instant.ofEpochSecond(i));
    }

    // 允许任意类型的符入，并输出序列化定制后的值
    @DataTypeHint(value = "RAW", bridgedTo = ByteBuffer.class)
    public ByteBuffer eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
        return null;
    }
}
