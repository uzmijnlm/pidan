package com.github.pidan.core.function;

public interface MapFunction<IN, OUT> extends Function {
    OUT map(IN input);
}