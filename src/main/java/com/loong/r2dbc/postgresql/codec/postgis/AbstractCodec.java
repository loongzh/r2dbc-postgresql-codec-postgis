package com.loong.r2dbc.postgresql.codec.postgis;

import static io.r2dbc.postgresql.client.Parameter.NULL_VALUE;

import io.netty.buffer.ByteBuf;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.codec.Codec;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.type.PostgresqlObjectId;
import io.r2dbc.postgresql.util.Assert;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

/**
 * 此处引用https://github.com/pgjdbc/r2dbc-postgresql/blob/main/src/main/java/io/r2dbc/postgresql/codec/AbstractCodec.java
 * @author: bufanqi
 **/
abstract class AbstractCodec<T> implements Codec<T> {

    private final Class<T> type;

    /**
     * Create a new {@link io.r2dbc.postgresql.codec.AbstractCodec}.
     *
     * @param type the type handled by this codec
     */
    AbstractCodec(Class<T> type) {
        this.type = Assert.requireNonNull(type, "type must not be null");
    }

    @Override
    public boolean canDecode(int dataType, Format format, Class<?> type) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return  (type == Object.class || isTypeAssignable(type)) &&
            doCanDecode(dataType, format);
    }

    @Override
    public boolean canEncode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        return this.type.isInstance(value);
    }

    @Override
    public boolean canEncodeNull(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        return this.type.isAssignableFrom(type);
    }

    @Nullable
    @Override
    public final T decode(@Nullable ByteBuf buffer, int dataType, Format format, Class<? extends T> type) {
        if (buffer == null) {
            return null;
        }

        return doDecode(buffer, dataType, format, type);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final Parameter encode(Object value) {
        Assert.requireNonNull(value, "value must not be null");

        return doEncode((T) value);
    }

    @Override
    public Class<?> type() {
        return this.type;
    }

    /**
     * Create a {@link Parameter}.
     *
     * @param type   the well-known {@link PostgresqlObjectId type OID}
     * @param format the format to use
     * @param value  {@link Publisher} emitting {@link ByteBuf buffers}
     * @return the encoded  {@link Parameter}
     * @implNote use deferred buffer creation instead of {@link Mono#just(Object)}  to avoid memory
     * leaks
     */
    static Parameter create(int type, Format format, Publisher<? extends ByteBuf> value) {
        return new Parameter(format,type, value);
    }

    /**
     * Create a {@link Parameter}.
     *
     * @param type           the well-known {@link PostgresqlObjectId type OID}
     * @param format         the format to use
     * @param bufferSupplier {@link Supplier} supplying the encoded {@link ByteBuf buffer}
     * @return the encoded  {@link Parameter}
     */
    static Parameter create(int type, Format format, Supplier<? extends ByteBuf> bufferSupplier) {
        return new Parameter(format, type, Mono.fromSupplier(bufferSupplier));
    }

    /**
     * Encode a {@code null} value.
     *
     * @param type   the well-known {@link PostgresqlObjectId type OID}
     * @param format the data type {@link Format}, text or binary
     * @return the encoded {@code null} value
     */
    static Parameter createNull(int type, Format format) {
        return create(type, format, NULL_VALUE);
    }

    /**
     * Determine whether this {@link Codec} is capable of decoding column values based on the given {@link Format} and {@link PostgresqlObjectId}.
     *
     * @param type   the well-known {@link PostgresqlObjectId type OID}
     * @param format the data type {@link Format}, text or binary
     * @return {@code true} if this codec is able to decode values of {@link Format} and {@link PostgresqlObjectId}
     */
    abstract boolean doCanDecode(int type, Format format);

    /**
     * Decode the {@link ByteBuf data} into the {@link Class value type}.
     *
     * @param buffer   the data buffer
     * @param dataType the well-known {@link PostgresqlObjectId type OID}
     * @param format   data type format
     * @param type     the desired value type
     * @return the decoded value, can be {@code null} if the column value is {@code null}
     */
    abstract T doDecode(ByteBuf buffer, int dataType, Format format, Class<? extends T> type);

    /**
     * @param value the  {@code value}
     * @return the encoded value
     */
    abstract Parameter doEncode(T value);

    boolean isTypeAssignable(Class<?> type) {
        Assert.requireNonNull(type, "type must not be null");

        return type.isAssignableFrom(this.type);
    }

}