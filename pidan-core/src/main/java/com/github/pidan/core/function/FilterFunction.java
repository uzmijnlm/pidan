package com.github.pidan.core.function;

public interface FilterFunction<IN> extends Function {
    boolean filter(IN input);
}