package com.github.kafka.serializer;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class KafkaZstdSerializer implements Serializer<byte[]> {
    private ZstdDictCompress dict;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        int level = 3;
        Object level_value = configs.get("zstd.compression.level");
        if (level_value != null) {
            level = (int) level_value;
        }
        try (InputStream input = this.getClass().getResourceAsStream("/dict"))
        {
            byte[] buffer = new byte[input.available()];
            input.read(buffer);
            dict = new ZstdDictCompress(buffer, level);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SerializationException("load dict error");
        }
    }

    @Override
    public byte[] serialize(String topic, byte[] data) {
        return Zstd.compress(data, dict);
    }
}