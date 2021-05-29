package com.github.pidan.core.function;

import java.io.Serializable;

public interface MapFunction<IN, OUT> extends Serializable {
    OUT map(IN input);
}