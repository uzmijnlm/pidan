package com.github.pidan.core.util;

import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;

public class TypeUtilTest {
    @Test
    public void testExtractKeyType() {
        // 用匿名内部类定义KeySelector
        KeySelector<Tuple2<Integer, String>, Integer> keySelector
                = new KeySelector<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, String> value) {
                return value.f0;
            }
        };
        Class<?> aClass = TypeUtil.extractKeyType(keySelector);
        Assert.assertEquals(Integer.class.getTypeName(), aClass.getTypeName());
    }

    @Test
    public void testExtractKeyTypeFromLambda() {
        // 用Lambda方式定义KeySelector
        KeySelector<Tuple2<Integer, String>, Integer> keySelector = value -> value.f0;
        Class<?> aClass = TypeUtil.extractKeyType(keySelector);
        Assert.assertEquals(Integer.class.getTypeName(), aClass.getTypeName());
    }

}
