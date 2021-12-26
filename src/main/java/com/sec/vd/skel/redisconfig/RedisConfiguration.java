package com.sec.vd.skel.redisconfig;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.CompressionCodec;
import io.lettuce.core.codec.CompressionCodec.CompressionType;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Component
public class RedisConfiguration {
    private StatefulRedisConnection<String, byte[]> connr;
    private StatefulRedisConnection<String, byte[]> connr2;
    private StatefulRedisConnection<String, byte[]> connw;
    private StatefulRedisConnection<String, byte[]> connw2;
    public RedisConfiguration() {
        RedisURI redisURIr = RedisURI.builder().withHost("localhost").withPort(6379).build();
        RedisClient redisClientr = RedisClient.create(redisURIr);
        this.connr =redisClientr.connect(CompressionCodec.valueCompressor(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE), CompressionType.DEFLATE ));
        RedisURI redisURIr2 = RedisURI.builder().withHost("localhost").withPort(6379).build();
        RedisClient redisClientr2 = RedisClient.create(redisURIr2);
        this.connr2 =redisClientr2.connect(CompressionCodec.valueCompressor(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE), CompressionType.DEFLATE ));
        RedisURI redisURIw = RedisURI.builder().withHost("localhost").withPort(6379).build();
        RedisClient redisClientw = RedisClient.create(redisURIw);
        this.connw =redisClientw.connect(CompressionCodec.valueCompressor(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE), CompressionType.DEFLATE ));
        RedisURI redisURIw2 = RedisURI.builder().withHost("localhost").withPort(6379).build();
        RedisClient redisClientw2 = RedisClient.create(redisURIw2);
        this.connw2 =redisClientw2.connect(CompressionCodec.valueCompressor(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE), CompressionType.DEFLATE ));
    }
    public Mono<byte[]> get(String key) {
        return this.connr.reactive().get(key).publishOn(Schedulers.parallel());
    }
    public Mono<byte[]> get2(String key) {
        return this.connr2.reactive().get(key).publishOn(Schedulers.parallel());
    }
    public Mono<String> set(String key,byte[] value) {
        return this.connw.reactive().set(key,value).publishOn(Schedulers.parallel());
    }
    public Mono<String> set2(String key,byte[] value) {
        return this.connw2.reactive().set(key,value).publishOn(Schedulers.parallel());
    }

    // 2 static connection of redis,    jmeter 30 thread , 3 rampup , 20초후 5600 tps , cpu 100
}
