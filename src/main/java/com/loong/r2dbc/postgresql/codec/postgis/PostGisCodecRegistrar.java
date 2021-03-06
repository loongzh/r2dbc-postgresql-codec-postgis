package com.loong.r2dbc.postgresql.codec.postgis;

import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlStatement;
import io.r2dbc.postgresql.codec.CodecRegistry;
import io.r2dbc.postgresql.extension.CodecRegistrar;
import java.util.Collection;
import java.util.Collections;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
/**
* 此处借鉴https://github.com/skroll/r2dbc-postgresql-codec-postgis/blob/master/src/main/java/com/github/skroll/r2dbc/postgresql/codec/postgis/PostGisCodecRegistrar.java
* @author: bufanqi
*/
public class PostGisCodecRegistrar implements CodecRegistrar {
  private static final String GEOMETRY_TYPENAME = "geometry";
  private static final Collection<String> ALL_TYPENAMES = Collections.singleton(GEOMETRY_TYPENAME);
  @Override
  public Publisher<Void> register(
      final PostgresqlConnection connection,
      final ByteBufAllocator allocator,
      final CodecRegistry registry) {
    return getPostGisTypeOids(connection, ALL_TYPENAMES)
        .doOnNext(tuple -> {
          //final String typeName = tuple.getT1();
          final int oid = tuple.getT2();
          registry.addLast(new GeometryCodec(oid,allocator));
        })
        .then();
  }

  static Flux<Tuple2<String, Integer>> getPostGisTypeOids(final PostgresqlConnection connection, final Collection<String> typeNames) {
    if (typeNames.isEmpty()) {
      return Flux.empty();
    }

    final StringBuilder sql = new StringBuilder()
        .append("select pt.typname::text as typname, pt.oid::int as oid ")
        .append("from pg_catalog.pg_extension pe ")
        .append("inner join pg_catalog.pg_type pt on pe.extnamespace = pt.typnamespace ")
        .append("where pe.extname = 'postgis' and pt.typname in (");

    for (int i = 1; i <= typeNames.size(); i++) {
      sql.append("$").append(i);
    }

    sql.append(")");

    final PostgresqlStatement statement = connection.createStatement(sql.toString());

    int index = 1;

    for (final String typeName : typeNames) {
      statement.bind(String.format("$%d", index++), typeName);
    }

    return statement.execute()
        .flatMap(result ->
            result.map((row, rowMetadata) -> {
              final String typeName = row.get(0, String.class);
              final Integer oid = row.get(1, Integer.class);

              if (typeName == null || oid == null) {
                throw new IllegalStateException("query failed");
              }

              return Tuples.of(typeName, oid);
            }));
  }
}
