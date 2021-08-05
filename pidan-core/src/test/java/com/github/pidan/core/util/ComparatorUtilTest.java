package com.github.pidan.core.util;

import org.junit.Assert;
import org.junit.Test;

public class ComparatorUtilTest {

    @Test
    public void testCompare() {
        Assert.assertEquals(-1, ComparatorUtil.COMPARATOR.compare(1, 2));
        Assert.assertEquals(-1, ComparatorUtil.COMPARATOR.compare("1", "2"));
        Assert.assertEquals(1, ComparatorUtil.COMPARATOR.compare(3.1, 2.0));
        Assert.assertEquals(1, ComparatorUtil.COMPARATOR.compare(100.1d, 99.2d));
    }
}
