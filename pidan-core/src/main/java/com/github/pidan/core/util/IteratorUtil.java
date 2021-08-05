package com.github.pidan.core.util;

import com.github.pidan.core.JoinType;
import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.ReduceFunction;
import com.github.pidan.core.tuple.Tuple2;
import com.google.common.collect.Iterators;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class IteratorUtil {

    public static <ROW, KEY> Iterator<ROW> sortIterator(Iterator<ROW> iterator,
                                                        KeySelector<ROW, KEY> keySelector) {
        List<ROW> list = new ArrayList<>();
        while (iterator.hasNext()) {
            ROW record = iterator.next();
            list.add(record);
        }
        list.sort((o1, o2) -> {
            KEY key1 = keySelector.getKey(o1);
            KEY key2 = keySelector.getKey(o2);
            return ComparatorUtil.COMPARATOR.compare(key1, key2);
        });
        return list.iterator();
    }

    public static <ROW, KEY> Iterator<ROW> reduce(Iterator<ROW> iterator,
                                                  KeySelector<ROW, KEY> keySelector,
                                                  ReduceFunction<ROW> reduceFunction) {
        Map<KEY, Iterable<ROW>> map = new HashMap<>();
        while (iterator.hasNext()) {
            ROW record = iterator.next();
            KEY key = keySelector.getKey(record);
            Iterable<ROW> value = map.computeIfAbsent(key, k -> new ArrayList<>());
            ((List<ROW>) value).add(record);
        }
        return map.values().stream().map(rows -> {
            Iterator<ROW> iter = rows.iterator();
            ROW lastVal = null;
            while (iter.hasNext()) {
                ROW curVal = iter.next();
                if (lastVal != null) {
                    lastVal = reduceFunction.reduce(lastVal, curVal);
                } else {
                    lastVal = curVal;
                }
            }
            return lastVal;
        }).iterator();
    }

    public static <ROW, KEY> Iterator<ROW> reduceSorted(Iterator<ROW> input,
                                                        KeySelector<ROW, KEY> keySelector,
                                                        ReduceFunction<ROW> reduceFunction) {
        return new Iterator<ROW>() {
            private ROW lastRow = input.next();

            @Override
            public boolean hasNext() {
                return input.hasNext() || lastRow != null;
            }

            @Override
            public ROW next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                while (input.hasNext()) {
                    ROW record = input.next();
                    if (!Objects.equals(keySelector.getKey(record), keySelector.getKey(lastRow))) {
                        ROW result = lastRow;
                        this.lastRow = record;
                        return result;
                    }
                    lastRow = reduceFunction.reduce(lastRow, record);
                }
                ROW result = lastRow;
                lastRow = null;
                return result;
            }
        };
    }

    public static <I1, I2, KEY> Iterator<Tuple2<I1, I2>> join(Iterator<I1> leftIterator,
                                                              Iterator<I2> rightIterator,
                                                              KeySelector<I1, KEY> keySelector1,
                                                              KeySelector<I2, KEY> keySelector2,
                                                              JoinType joinType) {
        Map<KEY, Iterable<?>[]> map = new HashMap<>();
        while (leftIterator.hasNext()) {
            I1 record = leftIterator.next();
            KEY key = keySelector1.getKey(record);
            Iterable<?>[] values = getIterables(map, key);
            ((List<Object>) values[0]).add(record);
        }
        while (rightIterator.hasNext()) {
            I2 record = rightIterator.next();
            KEY key = keySelector2.getKey(record);
            Iterable<?>[] values = getIterables(map, key);
            ((List<Object>) values[1]).add(record);
        }

        Iterator<Iterator<Tuple2<I1, I2>>> iterators = map.values().stream().map(iterables -> {
            Iterable<I1> iterable1 = (Iterable<I1>) iterables[0];
            Iterable<I2> iterable2 = (Iterable<I2>) iterables[1];
            return cartesian(iterable1, iterable2, joinType);
        }).iterator();
        return Iterators.concat(iterators);
    }

    private static <KEY> Iterable<?>[] getIterables(Map<KEY, Iterable<?>[]> map, KEY key) {
        Iterable<?>[] values = map.get(key);
        if (values == null) {
            values = new Iterable[2];
            for (int j = 0; j < 2; j++) {
                values[j] = new ArrayList<>();
            }
            map.put(key, values);
        }
        return values;
    }

    private static <I1, I2> Iterator<Tuple2<I1, I2>> cartesian(Iterable<I1> leftIterable,
                                                               Iterable<I2> rightIterable,
                                                               JoinType joinType) {
        Collection<I2> collection = (Collection<I2>) rightIterable;
        Function<I1, Stream<Tuple2<I1, I2>>> function;
        switch (joinType) {
            case INNER_JOIN:
                function = left -> collection.stream().map(right -> Tuple2.of(left, right));
                return ((Collection<I1>) leftIterable).stream().flatMap(function).iterator();
            case LEFT_JOIN:
                function = left -> {
                    if (collection.isEmpty()) {
                        return Stream.of(Tuple2.of(left, null));
                    } else {
                        return collection.stream().map(right -> Tuple2.of(left, right));
                    }
                };
                return ((Collection<I1>) leftIterable).stream().flatMap(function).iterator();
            case RIGHT_JOIN:
            case FULL_JOIN:

            default:
                throw new RuntimeException("Unsupported join type");
        }
    }

    public static <I1, I2, KEY> Iterator<Tuple2<I1, I2>> sortMergeJoin(
            Comparator<Object> comparator,
            Iterator<I1> leftIterator,
            Iterator<I2> rightIterator,
            KeySelector<I1, KEY> keySelector1,
            KeySelector<I2, KEY> keySelector2,
            JoinType joinType) {
        return new Iterator<Tuple2<I1, I2>>() {
            private I1 leftNode = leftIterator.next();
            private I2 rightNode = null;
            private I2 tmpRightNode = null;
            private boolean leftJoin;

            private final List<I1> leftSameKeys = new ArrayList<>();
            private final ResetIterator<I1> leftSameIterator = wrap(leftSameKeys);
            private final Iterator<Tuple2<I1, I2>> child = map(leftSameIterator, x -> Tuple2.of(x, rightNode));

            @Override
            public boolean hasNext() {
                if (child.hasNext()) {
                    return true;
                }
                if (!rightIterator.hasNext()) {
                    return false;
                }

                // 根据不同的JoinType迭代到下一个元素
                iterateToNextNode();

                // 新的rightNode的key与上一个相同，因此可以直接利用先前的leftSameIterator重新迭代
                if (!leftSameKeys.isEmpty() && Objects.equals(keySelector1.getKey(leftSameKeys.get(0)), keySelector2.getKey(rightNode))) {
                    leftSameIterator.reset();
                    return true;
                }
                while (true) {
                    int than = comparator.compare(keySelector1.getKey(leftNode), keySelector2.getKey(rightNode));
                    if (than == 0) {
                        return doWhenEqual();
                    } else if (than > 0) {
                        if (!rightIterator.hasNext()) {
                            return false;
                        }
                        this.rightNode = rightIterator.next();
                    } else {
                        if (!leftIterator.hasNext()) {
                            return false;
                        }
                        if (doWhenRightLarger()) {
                            return true;
                        }
                        this.leftNode = leftIterator.next();
                    }
                }
            }

            private boolean doWhenRightLarger() {
                switch (joinType) {
                    case INNER_JOIN:
                        break;
                    case LEFT_JOIN:
                        // 之前右侧迭代器没有与之相同的key出现过
                        if (leftSameKeys.isEmpty()) {
                            tmpRightNode = rightNode;
                            rightNode = null;
                            leftSameKeys.add(leftNode);
                            this.leftNode = leftIterator.next();
                            leftJoin = true;
                            return true;
                        }
                        return false;
                    case RIGHT_JOIN:
                    case FULL_JOIN:
                    default:
                        throw new RuntimeException("Unsupported join type");
                }
                return false;
            }

            private boolean doWhenEqual() {
                leftSameKeys.clear();
                do {
                    leftSameKeys.add(leftNode);
                    if (leftIterator.hasNext()) {
                        leftNode = leftIterator.next();
                    } else {
                        break;
                    }
                } while (Objects.equals(keySelector1.getKey(leftNode), keySelector2.getKey(rightNode)));
                leftSameIterator.reset();
                return true;
            }

            private void iterateToNextNode() {
                switch (joinType) {
                    case INNER_JOIN:
                        rightNode = rightIterator.next();
                        break;
                    case LEFT_JOIN:
                        if (leftJoin) {
                            rightNode = tmpRightNode;
                            leftJoin = false;
                        } else {
                            rightNode = rightIterator.next();
                        }
                        break;
                    case RIGHT_JOIN:
                    case FULL_JOIN:
                    default:
                        throw new RuntimeException("Unsupported join type");
                }
            }

            @Override
            public Tuple2<I1, I2> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return child.next();
            }
        };
    }

    interface ResetIterator<E> extends Iterator<E> {
        void reset();
    }

    public static <E> ResetIterator<E> wrap(List<E> values) {
        return new ResetIterator<E>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < values.size();
            }

            @Override
            public E next() {
                if (!this.hasNext()) {
                    throw new NoSuchElementException();
                }
                return values.get(index++);
            }

            @Override
            public void reset() {
                this.index = 0;
            }
        };
    }

    public static <I1, I2> Iterator<I2> map(Iterator<I1> iterator, Function<I1, I2> function) {
        return new Iterator<I2>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public I2 next() {
                return function.apply(iterator.next());
            }
        };
    }
}
