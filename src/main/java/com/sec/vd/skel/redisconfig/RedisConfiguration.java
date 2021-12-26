package com.sec.vd.skel.redisconfig;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.CompressionCodec;
import io.lettuce.core.codec.CompressionCodec.CompressionType;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.AsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
public class RedisConfiguration {
    private GenericObjectPool<StatefulRedisConnection<String, byte[]>> poolr;
    private GenericObjectPool<StatefulRedisConnection<String, byte[]>> poolw;
    private AsyncPool<StatefulRedisConnection<String, byte[]>> pool;
    public RedisConfiguration() {
        RedisURI redisURI = RedisURI.builder().withHost("localhost").withPort(6379).build();
        RedisClient redisClient = RedisClient.create();
        pool = AsyncConnectionPoolSupport.createBoundedObjectPool(
                () -> redisClient.connectAsync(CompressionCodec.valueCompressor(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE), CompressionType.DEFLATE ),redisURI)
                , BoundedPoolConfig.builder()
                        .testOnAcquire()
                        .testOnCreate()
                        .testOnRelease()
                        .build() );


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
        return Mono.fromFuture(
                this.pool.acquire().thenCompose(connection -> {
                    RedisAsyncCommands<String, byte[]> async = connection.async();
                    return async.get(key).whenComplete((s, ex)-> this.pool.release(connection));
                })
        );
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
}
