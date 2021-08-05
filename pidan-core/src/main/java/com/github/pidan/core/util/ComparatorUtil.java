package com.github.pidan.core.util;

import com.github.pidan.core.Comparator;

public class ComparatorUtil {

    public final static Comparator<Object> COMPARATOR = ComparatorUtil::compare0;

    private static int compare0(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }

        if (o1.getClass()!=o2.getClass()) {
            throw new RuntimeException("Class not same for o1 and o2");
        }

        if (o1.getClass() == String.class) {
            return ((String) o1).compareTo((String) o2);
        }
        else if (o1.getClass() == Integer.class) {
            return ((Integer) o1).compareTo((Integer) o2);
        }
        else if (o1.getClass() == Long.class) {
            return ((Long) o1).compareTo((Long) o2);
        }
        else if (o1.getClass() == Short.class) {
            return ((Short) o1).compareTo((Short) o2);
        }
        else if (o1.getClass() == Float.class) {
            return ((Float) o1).compareTo((Float) o2);
        }
        else if (o1.getClass() == Double.class) {
            return ((Double) o1).compareTo((Double) o2);
        }
        else if (o1.getClass() == Byte.class) {
            return ((Byte) o1).compareTo((Byte) o2);
        }
        else if (o1.getClass() == Character.class) {
            return ((Character) o1).compareTo((Character) o2);
        }
        else if (o1.getClass() == Boolean.class) {
            return ((Boolean) o1).compareTo((Boolean) o2);
        }
        else {
            throw new UnsupportedOperationException("not support " + o1.getClass());
        }
    }
}
