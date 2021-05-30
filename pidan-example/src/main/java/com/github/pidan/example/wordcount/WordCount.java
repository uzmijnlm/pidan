package com.github.pidan.example.wordcount;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.batch.environment.ExecutionEnvironment;
import com.github.pidan.core.tuple.Tuple2;

public class WordCount {

    public static void main(String[] args) {
        // 1.获取执行环境。
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.定义输入。
        DataSet<String> dataSet = WordCountData.getDefaultTextLineDataSet(env);

        // 3.定义转换操作。
        DataSet<String> words =
                dataSet
                        .flatMap(input -> input.toLowerCase().split("\\W+"))
                        .filter(input -> !"".equals(input.trim()));

        DataSet<Tuple2<String, Integer>> result = words.map(x -> Tuple2.of(x, 1))
                .groupBy(x -> x.f0)
                .reduce((x, y) ->
                        Tuple2.of(x.f0, x.f1 + y.f1)
                );

        // 4.触发计算。
        result.foreach(x -> System.out.println(x.f0 + ": " + x.f1));

    }

}
