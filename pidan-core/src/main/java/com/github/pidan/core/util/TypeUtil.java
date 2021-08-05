package com.github.pidan.core.util;

import com.github.pidan.core.function.KeySelector;
import net.jodah.typetools.TypeResolver;

public class TypeUtil {

    public static <ROW, KEY> Class<?> extractKeyType(KeySelector<ROW, KEY> keySelector) {
        Class<?>[] classes = TypeResolver.resolveRawArguments(KeySelector.class, keySelector.getClass());
        return classes[1];
    }

    public static <T, S extends T> Class<?>[] extractRawArgumentsFromLambda(Class<T> type, Class<S> subType) {
        return TypeResolver.resolveRawArguments(type, subType);
    }

}
