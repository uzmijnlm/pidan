package com.github.pidan.core.tuple;

import java.util.Objects;

public class Tuple2<T0, T1> extends Tuple {

    public T0 f0;
    public T1 f1;

    public Tuple2() {
    }

    public Tuple2(T0 f0, T1 f1) {
        this.f0 = f0;
        this.f1 = f1;
    }

    public static <T0, T1> Tuple2<T0, T1> of(T0 value0, T1 value1) {
        return new Tuple2<>(value0, value1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;
        return Objects.equals(f0, tuple2.f0) && Objects.equals(f1, tuple2.f1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(f0, f1);
    }

    @Override
    public String toString() {
        return "Tuple2{" +
                "f0=" + f0 +
                ", f1=" + f1 +
                '}';
    }
}
