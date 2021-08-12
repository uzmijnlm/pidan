package com.github.pidan.core.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Objects;

public class SerializableUtilTest {

    @Test
    public void testSerAndDeSer() throws IOException, ClassNotFoundException {
        Person person = new Person("Alice", 24);
        byte[] serializedPerson = SerializableUtil.serialize(person);
        Object o1 = SerializableUtil.byteToObject(serializedPerson);
        Assert.assertTrue(o1 instanceof Person);
        Assert.assertEquals(o1, person);

        InputStream input = new ByteArrayInputStream(serializedPerson);
        Object o2 = SerializableUtil.byteToObject(input);
        Assert.assertTrue(o2 instanceof Person);
        Assert.assertEquals(o2, person);
    }

    static class Person implements Serializable {
        private final String name;
        private final int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Person person = (Person) o;
            return age == person.age && Objects.equals(name, person.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age);
        }
    }
}
