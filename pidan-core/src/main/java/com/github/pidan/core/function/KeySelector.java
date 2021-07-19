package com.github.pidan.core.function;

public interface KeySelector<IN, KEY> extends Function {

    KEY getKey(IN value);
}
