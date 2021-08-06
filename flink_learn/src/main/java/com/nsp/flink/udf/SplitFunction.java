package com.nsp.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.example.TableEnvironmentDemo;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

@FunctionHint(
        output = @DataTypeHint("ROW<word STRING, length INT>")
)
public class SplitFunction extends TableFunction<Row> {

    public void eval(String string) {
        for (String s : string.split(" ")) {
            // use collect(...) to emit a row
            collect(Row.of(s, s.length()));
        }
    }

    public static void main(String[] args) {
        StreamTableEnvironment streamTableEnvironment = TableEnvironmentDemo.createBlinkStreamTableEnvironment();

        // 在 Table API 里不经注册直接“内联”调用函数
        streamTableEnvironment.from("MyTable")
                .joinLateral(call(SplitFunction.class, $("myField")))
                .select($("myField"), $("word"), $("length"));

        streamTableEnvironment
                .from("MyTable")
                .leftOuterJoinLateral(call(SplitFunction.class, $("myField")))
                .select($("myField"), $("word"), $("length"));

        // 在 Table API 里重命名函数字段
        streamTableEnvironment
                .from("MyTable")
                .leftOuterJoinLateral(call(SplitFunction.class, $("myField")).as("newWord", "newLength"))
                .select($("myField"), $("newWord"), $("newLength"));

        // 注册函数
        streamTableEnvironment.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        // 在 Table API 里调用注册好的函数
        streamTableEnvironment
                .from("MyTable")
                .joinLateral(call("SplitFunction", $("myField")))
                .select($("myField"), $("word"), $("length"));
        streamTableEnvironment
                .from("MyTable")
                .leftOuterJoinLateral(call("SplitFunction", $("myField")))
                .select($("myField"), $("word"), $("length"));

        // 在 SQL 里调用注册好的函数
        streamTableEnvironment.sqlQuery(
                "SELECT myField, word, length " +
                        "FROM MyTable, LATERAL TABLE(SplitFunction(myField))");
        streamTableEnvironment.sqlQuery(
                "SELECT myField, word, length " +
                        "FROM MyTable " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) ON TRUE");

        // 在 SQL 里重命名函数字段
        streamTableEnvironment.sqlQuery(
                "SELECT myField, newWord, newLength " +
                        "FROM MyTable " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) AS T(newWord, newLength) ON TRUE");

    }

}
