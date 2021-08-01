package com.github.pidan.batch.api;

import com.github.pidan.core.Partition;
import com.github.pidan.core.TaskContext;
import com.github.pidan.core.function.HashPartitioner;
import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.MapFunction;
import com.github.pidan.core.tuple.Tuple2;

import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class JoinedDataSet<I1, I2> {
    protected final DataSet<I1> input1;
    protected final DataSet<I2> input2;

    protected final JoinType joinType;

    public JoinedDataSet(DataSet<I1> input1, DataSet<I2> input2) {
        this(input1, input2, JoinType.INNER_JOIN);
    }

    public JoinedDataSet(DataSet<I1> input1, DataSet<I2> input2, JoinType joinType) {
        this.input1 = input1;
        this.input2 = input2;
        this.joinType = joinType;
    }

    public <KEY> Where<KEY> where(KeySelector<I1, KEY> keySelector) {
        return new Where<>(keySelector);
    }


    public class Where<KEY> {
        private final KeySelector<I1, KEY> keySelector1;

        public Where(KeySelector<I1, KEY> keySelector1) {
            this.keySelector1 = keySelector1;
        }

        public EqualTo<KEY> equalTo(KeySelector<I2, KEY> keySelector2) {
            HashPartitioner<KEY> partitioner = new HashPartitioner<>(input1.numPartitions());
            KeySelector<Tuple2<Integer, I1>, KEY> newKeySelector1 = value -> keySelector1.getKey(value.f1);
            ShuffleMapOperator<KEY, Tuple2<Integer, I1>> shuffleMapOperator1 = new ShuffleMapOperator<>(
                    input1.map((MapFunction<I1, Tuple2<Integer, I1>>) input -> Tuple2.of(1, input)),
                    newKeySelector1,
                    partitioner);
            KeySelector<Tuple2<Integer, I2>, KEY> newKeySelector2 = value -> keySelector2.getKey(value.f1);
            ShuffleMapOperator<KEY, Tuple2<Integer, I2>> shuffleMapOperator2 = new ShuffleMapOperator<>(
                    input2.map((MapFunction<I2, Tuple2<Integer, I2>>) input -> Tuple2.of(2, input)),
                    newKeySelector2,
                    partitioner);
            return new EqualTo<>(
                    shuffleMapOperator1,
                    shuffleMapOperator2,
                    newKeySelector1,
                    newKeySelector2);
        }
    }

    public class EqualTo<KEY> extends DataSet<Tuple2<I1, I2>> {
        private final DataSet<Tuple2<Integer, I1>> input1;
        private final DataSet<Tuple2<Integer, I2>> input2;
        private final KeySelector<Tuple2<Integer, I1>, KEY> keySelector1;
        private final KeySelector<Tuple2<Integer, I2>, KEY> keySelector2;

        protected EqualTo(DataSet<Tuple2<Integer, I1>> input1,
                          DataSet<Tuple2<Integer, I2>> input2,
                          KeySelector<Tuple2<Integer, I1>, KEY> keySelector1,
                          KeySelector<Tuple2<Integer, I2>, KEY> keySelector2) {
            super(input1, input2);
            this.input1 = input1;
            this.input2 = input2;
            this.keySelector1 = keySelector1;
            this.keySelector2 = keySelector2;
        }


        @Override
        public Partition[] getPartitions() {
            return IntStream.range(0, input1.numPartitions())
                    .mapToObj(Partition::new).toArray(Partition[]::new);
        }

        @Override
        public Iterator<Tuple2<I1, I2>> compute(Partition partition, TaskContext taskContext) {
            int[] deps = taskContext.getDependStages();
            Map<KEY, Iterable<?>[]> map = new HashMap<>();
            int index = partition.getIndex();
            for (int i=0; i<2; i++) {
                ShuffleReader shuffleReader = new ShuffleReader(index, deps[i]);
                Iterator<Tuple2<Integer, Object>> iterator = (Iterator<Tuple2<Integer, Object>>) shuffleReader.read();
                while (iterator.hasNext()) {
                    Tuple2<Integer, Object> tuple2 = iterator.next();
                    KEY key;
                    if (tuple2.f0==1) {
                        key = keySelector1.getKey((Tuple2<Integer, I1>) tuple2);
                    } else if (tuple2.f0==2) {
                        key = keySelector2.getKey((Tuple2<Integer, I2>) tuple2);
                    } else {
                        throw new RuntimeException("Join error");
                    }
                    Iterable<?>[] values = map.get(key);
                    if (values == null) {
                        values = new Iterable[2];
                        for (int j = 0; j < 2; j++) {
                            values[j] = new ArrayList<>();
                        }
                        map.put(key, values);
                    }
                    ((List<Object>) values[i]).add(tuple2.f1);
                }
            }
            return map.values().stream().flatMap((Function<Iterable<?>[], Stream<Tuple2<I1, I2>>>) iterables -> {
                Iterable<I1> iterable1 = (Iterable<I1>) iterables[0];
                Iterable<I2> iterable2 = (Iterable<I2>) iterables[1];
                Collection<I2> collection = (Collection<I2>) iterable2;
                Function<I1, Stream<Tuple2<I1, I2>>> function;
                switch (joinType) {
                    case INNER_JOIN:
                        function = left -> collection.stream().map(right -> Tuple2.of(left, right));
                        break;
                    case LEFT_JOIN:
                        function = left -> {
                            if (collection.isEmpty()) {
                                return Stream.of(Tuple2.of(left, null));
                            } else {
                                return collection.stream().map(right -> Tuple2.of(left, right));
                            }
                        };
                        break;
                    case RIGHT_JOIN:
                    case FULL_JOIN:
                    default:
                        throw new RuntimeException("Unsupported join type");
                }
                return ((Collection<I1>) iterable1).stream().flatMap(function);
            }).iterator();
        }
    }
}
