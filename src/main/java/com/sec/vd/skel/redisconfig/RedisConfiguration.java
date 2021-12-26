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
    private GenericObjectPool<StatefulRedisConnection<String, byte[]>> poolr;
    private GenericObjectPool<StatefulRedisConnection<String, byte[]>> poolw;
    public RedisConfiguration() {
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();        //poolsize default : Max=8

        RedisURI redisURIr = RedisURI.builder().withHost("localhost").withPort(6379).build();
        RedisClient redisClientr = RedisClient.create(redisURIr);
        this.poolr = ConnectionPoolSupport.createGenericObjectPool(
                () -> redisClientr.connect(CompressionCodec.valueCompressor(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE), CompressionType.DEFLATE ))
                , genericObjectPoolConfig);

        RedisURI redisURIw = RedisURI.builder().withHost("localhost").withPort(6379).build();
        RedisClient redisClientw = RedisClient.create(redisURIw);
        this.poolw = ConnectionPoolSupport.createGenericObjectPool(
                () -> redisClientw.connect(CompressionCodec.valueCompressor(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE), CompressionType.DEFLATE ))
                , genericObjectPoolConfig);
    }
    public Mono<byte[]> get(String key) {
        try (StatefulRedisConnection<String, byte[]> connection = this.poolw.borrowObject()) {
            return connection.reactive().get(key).publishOn(Schedulers.parallel());
        }
        catch(Exception e) {
            log.error(e.getMessage());
            return Mono.empty();
        }
    }
    public Mono<String> set(String key,byte[] value) {
        try (StatefulRedisConnection<String, byte[]> connection = this.poolw.borrowObject()) {
            return connection.reactive().set(key,value).publishOn(Schedulers.parallel());
        }
        catch(Exception e) {
            log.error(e.getMessage());
            return Mono.empty();
        }
    }

    // 30 thread , 3 rampup , 20초후 4900 tps
}
