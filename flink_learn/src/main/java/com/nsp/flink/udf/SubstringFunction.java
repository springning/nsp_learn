package com.nsp.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class SubstringFunction extends ScalarFunction {

    public String eval(String s, int begin, int end) {
        return s.substring(begin, end);
    }
}
