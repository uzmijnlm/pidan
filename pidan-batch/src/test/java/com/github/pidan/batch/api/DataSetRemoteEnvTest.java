package com.github.pidan.batch.api;

import com.github.pidan.batch.environment.ExecutionEnvironment;
import com.github.pidan.core.JoinType;
import com.github.pidan.core.function.FlatMapFunction;
import com.github.pidan.core.tuple.Tuple2;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataSetRemoteEnvTest {

    private ExecutionEnvironment env;

    @Before
    public void start() {
        env = ExecutionEnvironment.getPseudoRemoteExecutionEnvironment();
    }

    @After
    public void stop() {
        env.stopForcibly();
    }

    @Test
    public void testCollect() {
        DataSet<Integer> dataSet = env.fromElements(1, 2, 3);
        List<Integer> collect = dataSet.collect();
        Assert.assertEquals(Arrays.asList(1, 2, 3), collect);
    }

    @Test
    public void testFlatMapByFlatMapFunction() {
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
    public void testReduce() {
        DataSet<String> dataSet = env.fromElements("to be", "or not to be");
        DataSet<Tuple2<String, Integer>> tuples = dataSet.flatMap((FlatMapFunction<String, String>) (input, collector) -> {
            String[] words = input.split("\\W+");
            for (String word : words) {
                collector.collect(word);
            }
        }).map(word -> new Tuple2<>(word, 1));
        List<Tuple2<String, Integer>> result = tuples
                .groupBy(x -> x.f0)
                .reduce((x, y) -> Tuple2.of(x.f0, x.f1 + y.f1))
                .collect();
        Assert.assertTrue(result.contains(Tuple2.of("not", 1)));
        Assert.assertTrue(result.contains(Tuple2.of("or", 1)));
        Assert.assertTrue(result.contains(Tuple2.of("be", 2)));
        Assert.assertTrue(result.contains(Tuple2.of("to", 2)));
    }

    @Test
    public void testCount() {
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
        Assert.assertTrue(result.contains(Tuple2.of("not", 1)));
        Assert.assertTrue(result.contains(Tuple2.of("or", 1)));
        Assert.assertTrue(result.contains(Tuple2.of("be", 2)));
        Assert.assertTrue(result.contains(Tuple2.of("to", 2)));
    }

    @Test
    public void testFromCollection() {
        DataSet<String> dataSet = env.fromCollection(Arrays.asList("1", "2", "3"));

        DataSet<Integer> mapDataSet = dataSet.map(Integer::parseInt);
        List<Integer> mapList = mapDataSet.collect();
        Assert.assertEquals(Arrays.asList(1, 2, 3), mapList);
    }

    @Test
    public void testFromCollectionParallel() {
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

    @Test
    public void testInnerJoin() {
        // id, name
        DataSet<Tuple2<Integer, String>> dataSet1 = env.fromElements(Tuple2.of(1, "Alice"), Tuple2.of(2, "Sam"), Tuple2.of(3, "Tom"));
        // id, age
        DataSet<Tuple2<Integer, Integer>> dataSet2 = env.fromElements(Tuple2.of(1, 18), Tuple2.of(2, 20), Tuple2.of(3, 22));

        DataSet<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Integer>>> equalTo
                = dataSet1
                .join(dataSet2)
                .where(x -> x.f0)
                .equalTo(x -> x.f0);
        List<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Integer>>> result = equalTo.collect();

        List<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Integer>>> expectedResult = new ArrayList<>();
        expectedResult.add(Tuple2.of(Tuple2.of(1, "Alice"), Tuple2.of(1, 18)));
        expectedResult.add(Tuple2.of(Tuple2.of(2, "Sam"), Tuple2.of(2, 20)));
        expectedResult.add(Tuple2.of(Tuple2.of(3, "Tom"), Tuple2.of(3, 22)));

        Assert.assertEquals(expectedResult, result);
    }

    @Test
    public void testLeftJoin() {
        // id, name
        DataSet<Tuple2<Integer, String>> dataSet1 = env.fromElements(Tuple2.of(1, "Alice"), Tuple2.of(2, "Sam"), Tuple2.of(3, "Tom"));
        // id, age
        DataSet<Tuple2<Integer, Integer>> dataSet2 = env.fromElements(Tuple2.of(2, 20), Tuple2.of(3, 22));

        DataSet<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Integer>>> equalTo
                = dataSet1
                .join(dataSet2, JoinType.LEFT_JOIN)
                .where(x -> x.f0)
                .equalTo(x -> x.f0);
        List<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Integer>>> result = equalTo.collect();

        List<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Integer>>> expectedResult = new ArrayList<>();
        expectedResult.add(Tuple2.of(Tuple2.of(1, "Alice"), null));
        expectedResult.add(Tuple2.of(Tuple2.of(2, "Sam"), Tuple2.of(2, 20)));
        expectedResult.add(Tuple2.of(Tuple2.of(3, "Tom"), Tuple2.of(3, 22)));

        Assert.assertEquals(expectedResult, result);
    }
}
