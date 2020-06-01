package com.github.kafka.serializer;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictDecompress;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class KafkaZstdDeserializer implements Deserializer<byte[]> {
    private ZstdDictDecompress dict;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        try (InputStream input = this.getClass().getResourceAsStream("/dict"))
        {
            byte[] buffer = new byte[input.available()];
            input.read(buffer);
            dict = new ZstdDictDecompress(buffer);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SerializationException("load dict error");
        }
    }

    @Override
    public byte[] deserialize(String topic, byte[] data) {
        int originalSize = (int) Zstd.decompressedSize(data);
        return Zstd.decompress(data, dict, originalSize);
    }

    @Override
    public void close() {
        dict.close();
    }
}