package com.github.pidan.batch.api;

import com.github.pidan.core.JoinType;
import com.github.pidan.core.Partition;
import com.github.pidan.core.TaskContext;
import com.github.pidan.core.function.HashPartitioner;
import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.tuple.Tuple2;
import com.github.pidan.core.util.ComparatorUtil;
import com.github.pidan.core.util.IteratorUtil;

import java.util.Iterator;
import java.util.stream.IntStream;

import static com.github.pidan.core.configuration.Constant.enableSortShuffle;

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
            HashPartitioner partitioner = new HashPartitioner(input1.numPartitions());
            ShuffleMapOperator<KEY, I1> shuffleMapOperator1 = new ShuffleMapOperator<>(
                    input1,
                    keySelector1,
                    partitioner);
            ShuffleMapOperator<KEY, I2> shuffleMapOperator2 = new ShuffleMapOperator<>(
                    input2,
                    keySelector2,
                    partitioner);
            return new EqualTo<>(
                    shuffleMapOperator1,
                    shuffleMapOperator2,
                    keySelector1,
                    keySelector2);
        }
    }

    public class EqualTo<KEY> extends DataSet<Tuple2<I1, I2>> {
        private final KeySelector<I1, KEY> keySelector1;
        private final KeySelector<I2, KEY> keySelector2;

        protected EqualTo(DataSet<I1> input1,
                          DataSet<I2> input2,
                          KeySelector<I1, KEY> keySelector1,
                          KeySelector<I2, KEY> keySelector2) {
            super(input1, input2);
            this.keySelector1 = keySelector1;
            this.keySelector2 = keySelector2;
        }


        @Override
        public Partition[] getPartitions() {
            return IntStream.range(0, getDependencies().get(0).numPartitions())
                    .mapToObj(Partition::new).toArray(Partition[]::new);
        }

        @Override
        public Iterator<Tuple2<I1, I2>> compute(Partition partition, TaskContext taskContext) {
            int[] deps = taskContext.getDependStages();
            int index = partition.getIndex();
            Iterator<I1> leftIterator = (Iterator<I1>) new ShuffleReader(index, deps[0]).read();
            Iterator<I2> rightIterator = (Iterator<I2>) new ShuffleReader(index, deps[1]).read();
            if (enableSortShuffle) {
                Iterator<I1> sortedLeftIter = IteratorUtil.sortIterator(leftIterator, keySelector1);
                Iterator<I2> sortedRightIter = IteratorUtil.sortIterator(rightIterator, keySelector2);
                return IteratorUtil.sortMergeJoin(ComparatorUtil.COMPARATOR, sortedLeftIter, sortedRightIter, keySelector1, keySelector2, joinType);
            } else {
                return IteratorUtil.join(leftIterator, rightIterator, keySelector1, keySelector2, joinType);
            }
        }
    }
}
