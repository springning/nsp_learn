package com.nsp.flink.udf.test;

import com.nsp.flink.udf.SubstringFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.TableEnvironmentDemo;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class SubStringFunctionTest {

    public static void main(String[] args) {
        StreamTableEnvironment blinkStreamTableEnvironment = TableEnvironmentDemo.createBlinkStreamTableEnvironment();
        blinkStreamTableEnvironment.from("mytable").select(call(SubstringFunction.class, $("myfield"), 5, 12));

        // 注册函数
        blinkStreamTableEnvironment.createTemporaryFunction("SubstringFunction", SubstringFunction.class);

        // 在 Table API 里调用注册好的函数
        blinkStreamTableEnvironment.from("MyTable").select(call("SubstringFunction", $("myField"), 5, 12));

        // 在 SQL 里调用注册好的函数
        blinkStreamTableEnvironment.sqlQuery("SELECT SubstringFunction(myField, 5, 12) FROM MyTable");

        blinkStreamTableEnvironment.getConfig().addJobParameter("xxx", "");

        System.out.println("1111");
    }
}
