package com.github.pidan.core.function;

public interface ReduceFunction<ROW> extends Function {
    ROW reduce(ROW input1, ROW input2);
}
