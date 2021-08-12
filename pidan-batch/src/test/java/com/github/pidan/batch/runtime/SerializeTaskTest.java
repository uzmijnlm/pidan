package com.github.pidan.batch.runtime;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.batch.environment.ExecutionEnvironment;
import com.github.pidan.core.partition.Partition;
import com.github.pidan.core.tuple.Tuple2;
import com.github.pidan.core.util.SerializableUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class SerializeTaskTest {

    private final static ExecutionEnvironment env = ExecutionEnvironment.getLocalExecutionEnvironment();

    // 避免在测试函数中产生SerializeTaskTest类的匿名内部类而导致序列化异常，故将测试逻辑全部写在static方法中
    @Test
    public void testTaskSerAndDeSer() throws IOException, ClassNotFoundException {
        SerializeTaskTest.test();
    }

    public static void test() throws IOException, ClassNotFoundException {
        // id, name
        DataSet<Tuple2<Integer, String>> dataSet1 = env.fromElements(Tuple2.of(1, "Alice"), Tuple2.of(2, "Sam"), Tuple2.of(3, "Tom"));
        // id, age
        DataSet<Tuple2<Integer, Integer>> dataSet2 = env.fromElements(Tuple2.of(1, 18), Tuple2.of(2, 20), Tuple2.of(3, 22));

        DataSet<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, Integer>>> equalTo
                = dataSet1
                .join(dataSet2)
                .where(x -> x.f0)
                .equalTo(x -> x.f0);
        Partition[] partitions = equalTo.getPartitions();
        Partition partition = partitions[0];
        int stageId = 1;
        Integer[] dependencies = new Integer[]{0};
        Map<Integer, Map<Integer, InetSocketAddress>> dependMapTasks = new HashMap<>();
        dependMapTasks.put(1, new HashMap<Integer, InetSocketAddress>(){{
            put(1, InetSocketAddress.createUnresolved("localhost", 8080));
        }});
        ShuffleMapTask shuffleMapTask = new ShuffleMapTask(stageId, partition, equalTo, dependMapTasks, dependencies);
        byte[] serializedTask = SerializableUtil.serialize(shuffleMapTask);
        Task<?> o1 = SerializableUtil.byteToObject(serializedTask);
        Assert.assertTrue(o1 instanceof ShuffleMapTask);

        InputStream input = new ByteArrayInputStream(serializedTask);
        Object o2 = SerializableUtil.byteToObject(input);
        Assert.assertTrue(o2 instanceof ShuffleMapTask);
    }
}
