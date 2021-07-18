package com.github.pidan.batch.api;

import com.github.pidan.batch.environment.ExecutionEnvironment;
import com.github.pidan.batch.shuffle.ShuffleManager;
import com.github.pidan.core.function.FlatMapFunction;
import com.github.pidan.core.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DataSetTest {

    private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    @Before
    public void setup() {
        ShuffleManager.clear();
    }

    @Test
    public void testCollect()
    {
        DataSet<Integer> dataSet = env.fromElements(1, 2, 3);
        List<Integer> collect = dataSet.collect();
        Assert.assertEquals(Arrays.asList(1, 2, 3), collect);
    }

    @Test
    public void testFlatMapByFlatMapFunction()
    {
        DataSet<String> dataSet = env.fromElements("to be", "or not to be");
        DataSet<String> flatMappedDataSet = dataSet.flatMap((FlatMapFunction<String, String>) (input, collector) -> {
            String[] words = input.split("\\W+");
            for (String word : words) {
                collector.collect(word);
            }
        });
        List<String> collect = flatMappedDataSet.collect();
        Assert.assertEquals(Arrays.asList("to", "be", "or", "not", "to", "be"), collect);
    }

    @Test
    public void testReduce()
    {
        DataSet<String> dataSet = env.fromElements("to be", "or not to be");
        DataSet<Tuple2<String, Integer>> tuples = dataSet.flatMap((FlatMapFunction<String, String>) (input, collector) -> {
            String[] words = input.split("\\W+");
            for (String word : words) {
                collector.collect(word);
            }
        }).map(word -> new Tuple2<>(word, 1));
        List<Tuple2<String, Integer>> result = tuples
                .groupBy(x -> x.f0)
                .reduce((x, y) ->
                        Tuple2.of(x.f0, x.f1 + y.f1)
                ).collect();
        StringBuilder sb = new StringBuilder();
        for (Tuple2<String, Integer> tuple2 : result) {
            String word = tuple2.f0;
            Integer count = tuple2.f1;
            sb.append(word);
            sb.append(",");
            sb.append(count);
            sb.append("\n");
        }
        String expectedString = "not,1\nor,1\nbe,2\nto,2\n";
        Assert.assertEquals(sb.toString(), expectedString);
    }

    @Test
    public void testCount()
    {
        DataSet<String> dataSet = env.fromElements("to be", "or not to be");
        DataSet<Tuple2<String, Integer>> tuples = dataSet.flatMap((FlatMapFunction<String, String>) (input, collector) -> {
            String[] words = input.split("\\W+");
            for (String word : words) {
                collector.collect(word);
            }
        }).map(word -> new Tuple2<>(word, 1));
        List<Tuple2<String, Integer>> result = tuples
                .groupBy(x -> x.f0)
                .count()
                .collect();
        StringBuilder sb = new StringBuilder();
        for (Tuple2<String, Integer> tuple2 : result) {
            String word = tuple2.f0;
            Integer count = tuple2.f1;
            sb.append(word);
            sb.append(",");
            sb.append(count);
            sb.append("\n");
        }
        String expectedString = "not,1\nor,1\nbe,2\nto,2\n";
        Assert.assertEquals(sb.toString(), expectedString);
    }

    @Test
    public void testFromCollectionParallel()
    {
        DataSet<String> dataSet = env.fromCollection(Arrays.asList("1", "2", "3"), 2);

        DataSet<Integer> mapDataSet = dataSet.map(Integer::parseInt);
        List<Integer> mapList = mapDataSet.collect();
        Assert.assertEquals(Arrays.asList(1, 2, 3), mapList);
    }

    @Test
    public void testReadTextFile() {
        DataSet<String> dataSet = env.textFile("src/test/resources/words.txt");
        List<String> collect = dataSet.flatMap((FlatMapFunction<String, String>) (input, collector) -> {
            String[] words = input.split("\\W+");
            for (String word : words) {
                collector.collect(word);
            }
        }).collect();
        Assert.assertEquals(Arrays.asList("to", "be", "or", "not", "to", "be"), collect);
    }
}
