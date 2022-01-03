package com.sec.vd.skel.redisconfig;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.CompressionCodec;
import io.lettuce.core.codec.CompressionCodec.CompressionType;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.internal.shaded.reactor.pool.InstrumentedPool;
import reactor.netty.internal.shaded.reactor.pool.PoolBuilder;

@Slf4j
@Component
public class RedisConfiguration {
    private InstrumentedPool<StatefulRedisConnection<String, byte[]>> pool;
    public RedisConfiguration() {
        this.pool = PoolBuilder.from( Mono.fromCallable(() -> {
            RedisURI redisURI = RedisURI.builder().withHost("localhost").withPort(6379).build();
            RedisClient redisClient = RedisClient.create(redisURI);
            StatefulRedisConnection<String, byte[]> conn = redisClient.connect(CompressionCodec.valueCompressor(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE), CompressionType.DEFLATE ));
            return conn;}))
        .acquisitionScheduler(Schedulers.boundedElastic())
        .destroyHandler(conn -> Mono.fromRunnable(conn::close))
        .buildPool();
        this.pool.warmup().block();
    }
    public Mono<byte[]> get(String key) {
        return this.pool.withPoolable(connection -> {
            return connection.reactive().get(key).publishOn(Schedulers.parallel());
        }).last();
    }
    public Mono<String> set(String key,byte[] value) {
        return this.pool.withPoolable(connection -> {
            return connection.reactive().set(key,value).publishOn(Schedulers.parallel());
        }).last();
    }

    // 30 thread , 3 rampup , 20초후 4900 tps
}
