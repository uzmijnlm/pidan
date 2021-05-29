package com.github.pidan.core.function;

import com.github.pidan.core.io.Collector;

public interface FlatMapFunction<IN, OUT> extends Function {
    void flatMap(IN input, Collector<OUT> collector);
}
