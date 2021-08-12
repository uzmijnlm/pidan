package com.github.pidan.core.util;


import java.io.*;

public class SerializableUtil {

    public static <T> T byteToObject(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
             ObjectInputStream oi = new ObjectInputStream(bi)) {
            return (T) oi.readObject();
        }
    }

    public static <T> T byteToObject(InputStream inputStream) throws IOException, ClassNotFoundException {
        try (ObjectInputStream oi = new ObjectInputStream(inputStream)) {
            return (T) oi.readObject();
        }
    }

    public static byte[] serialize(Serializable serializable) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream os = new ObjectOutputStream(bos)) {
            os.writeObject(serializable);
            return bos.toByteArray();
        }
    }
}
