package com.github.pidan.batch.api;

import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.ReduceFunction;
import com.github.pidan.core.tuple.Tuple2;
import com.github.pidan.core.util.IteratorUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class IteratorUtilTest {
    @Test
    public void testReduceSortedWithSortedIterator() {
        // 构造key有序的列表
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(Tuple2.of(1, "a"));
        list.add(Tuple2.of(1, "b"));
        list.add(Tuple2.of(2, "c"));
        list.add(Tuple2.of(2, "d"));
        list.add(Tuple2.of(3, "e"));

        ReduceFunction<Tuple2<Integer, String>> reduceFunction = (input1, input2) -> Tuple2.of(input1.f0, input1.f1+input2.f1);
        KeySelector<Tuple2<Integer, String>, Integer> keySelector = value -> value.f0;
        Iterator<Tuple2<Integer, String>> resultIter = IteratorUtil.reduceSorted(list.iterator(), keySelector, reduceFunction);
        // 相同key的数据被聚合在了一起
        Tuple2<Integer, String> next1 = resultIter.next();
        Assert.assertEquals(Tuple2.of(1,"ab"), next1);
        Tuple2<Integer, String> next2 = resultIter.next();
        Assert.assertEquals(Tuple2.of(2,"cd"), next2);
        Tuple2<Integer, String> next3 = resultIter.next();
        Assert.assertEquals(Tuple2.of(3,"e"), next3);
        // 不再有数据
        Assert.assertFalse(resultIter.hasNext());
    }

    @Test
    public void testReduceSortedWithUnSortedIterator() {
        // 构造key有序的列表
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(Tuple2.of(1, "a"));
        list.add(Tuple2.of(2, "d"));
        list.add(Tuple2.of(1, "b"));
        list.add(Tuple2.of(2, "c"));
        list.add(Tuple2.of(3, "e"));

        ReduceFunction<Tuple2<Integer, String>> reduceFunction = (input1, input2) -> Tuple2.of(input1.f0, input1.f1+input2.f1);
        KeySelector<Tuple2<Integer, String>, Integer> keySelector = value -> value.f0;
        Iterator<Tuple2<Integer, String>> resultIter = IteratorUtil.reduceSorted(list.iterator(), keySelector, reduceFunction);
        // 相同key的数据没有被聚合在一起，反而由于顺序被错开，被当成了不同的key
        Tuple2<Integer, String> next1 = resultIter.next();
        Assert.assertEquals(Tuple2.of(1,"a"), next1);
        Tuple2<Integer, String> next2 = resultIter.next();
        Assert.assertEquals(Tuple2.of(2,"d"), next2);
        Tuple2<Integer, String> next3 = resultIter.next();
        Assert.assertEquals(Tuple2.of(1,"b"), next3);
        Tuple2<Integer, String> next4 = resultIter.next();
        Assert.assertEquals(Tuple2.of(2,"c"), next4);
        Tuple2<Integer, String> next5 = resultIter.next();
        Assert.assertEquals(Tuple2.of(3,"e"), next5);
        // 不再有数据
        Assert.assertFalse(resultIter.hasNext());
    }

    @Test
    public void testReduce() {
        // 构造key无序的列表
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(Tuple2.of(1, "a"));
        list.add(Tuple2.of(2, "c"));
        list.add(Tuple2.of(1, "b"));
        list.add(Tuple2.of(3, "e"));
        list.add(Tuple2.of(2, "d"));

        ReduceFunction<Tuple2<Integer, String>> reduceFunction = (input1, input2) -> Tuple2.of(input1.f0, input1.f1+input2.f1);
        KeySelector<Tuple2<Integer, String>, Integer> keySelector = value -> value.f0;
        Iterator<Tuple2<Integer, String>> resultIter = IteratorUtil.reduce(list.iterator(), keySelector, reduceFunction);
        // 虽然list无序，但相同key的数据仍被聚合在了一起
        Tuple2<Integer, String> next1 = resultIter.next();
        Assert.assertEquals(Tuple2.of(1,"ab"), next1);
        Tuple2<Integer, String> next2 = resultIter.next();
        Assert.assertEquals(Tuple2.of(2,"cd"), next2);
        Tuple2<Integer, String> next3 = resultIter.next();
        Assert.assertEquals(Tuple2.of(3,"e"), next3);
        // 不再有数据
        Assert.assertFalse(resultIter.hasNext());
    }

    @Test
    public void testSortMergeJoinWithSortedIterators() {
        // 构造key有序的列表
        List<Tuple2<Integer, String>> list1 = new ArrayList<>();
        list1.add(Tuple2.of(1, "a"));
        list1.add(Tuple2.of(2, "c"));
        KeySelector<Tuple2<Integer, String>, Integer> keySelector1 = value -> value.f0;

        // 构造key有序的列表
        List<Tuple2<Integer, String>> list2 = new ArrayList<>();
        list2.add(Tuple2.of(1, "b"));
        list2.add(Tuple2.of(2, "d"));

        KeySelector<Tuple2<Integer, String>, Integer> keySelector2 = value -> value.f0;

        Iterator<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>> resultIter
                = IteratorUtil.sortMergeJoin(Comparator.comparingInt(o -> o),
                list1.iterator(), list2.iterator(), keySelector1, keySelector2);

        Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> next1 = resultIter.next();
        Assert.assertEquals(Tuple2.of(Tuple2.of(1, "a"), Tuple2.of(1, "b")), next1);
        Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> next2 = resultIter.next();
        Assert.assertEquals(Tuple2.of(Tuple2.of(2, "c"), Tuple2.of(2, "d")), next2);
        // 不再有数据
        Assert.assertFalse(resultIter.hasNext());
    }

    @Test
    public void testSortMergeJoinWithUnSortedIterators() {
        // 构造key有序的列表
        List<Tuple2<Integer, String>> list1 = new ArrayList<>();
        list1.add(Tuple2.of(1, "a"));
        list1.add(Tuple2.of(2, "c"));
        KeySelector<Tuple2<Integer, String>, Integer> keySelector1 = value -> value.f0;

        // 构造与上面列表key顺序不同的列表
        List<Tuple2<Integer, String>> list2 = new ArrayList<>();
        list2.add(Tuple2.of(2, "d"));
        list2.add(Tuple2.of(1, "b"));

        KeySelector<Tuple2<Integer, String>, Integer> keySelector2 = value -> value.f0;

        Iterator<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>> resultIter
                = IteratorUtil.sortMergeJoin(Comparator.comparingInt(o -> o),
                list1.iterator(), list2.iterator(), keySelector1, keySelector2);

        // key为1的数据没有join到数据
        Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> next1 = resultIter.next();
        Assert.assertEquals(Tuple2.of(Tuple2.of(2, "c"), Tuple2.of(2, "d")), next1);
        // 仅迭代一次后就不再有数据
        Assert.assertFalse(resultIter.hasNext());
    }
}
