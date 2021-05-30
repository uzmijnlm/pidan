package com.github.pidan.core.function;

import java.io.Serializable;

public interface Foreach<ROW> extends Serializable {

    void apply(ROW value);
}
