package com.loong.r2dbc.postgresql.codec.postgis;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.BPCHAR;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.CHAR;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.NAME;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.TEXT;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.UNKNOWN;
import static io.r2dbc.postgresql.type.PostgresqlObjectId.VARCHAR;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import org.geotools.geometry.jts.WKBReader;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

/**
* Geometry编解码器
* @Author: bufanqi
* @Date: 2021/8/6
*/
public class GeometryCodec extends AbstractCodec<Geometry> {
    private final int postGreSqlObjectId=34754;
    private final ByteBufAllocator byteBufAllocator;
    private final int type;
    public GeometryCodec(int type,ByteBufAllocator byteBufAllocator) {
        super(Geometry.class);
        Assert.requireNonNull(type, "type must not be null");
        Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
        this.type=type;
        this.byteBufAllocator=byteBufAllocator;
    }

    @Override boolean doCanDecode(int type, Format format) {
        Assert.requireNonNull(format, "format must not be null");
        Assert.requireNonNull(type, "type must not be null");

        return postGreSqlObjectId == type;
  }

    @Override Geometry doDecode(ByteBuf buffer, int dataType, Format format, Class<? extends Geometry> type) {
        if (buffer == null) {
            return null;
        }
        if(dataType==postGreSqlObjectId){
            WKBReader reader = new WKBReader();
            Geometry geometry = null;
            try {
                geometry = reader.read(WKBReader.hexToBytes(ByteBufUtils.decode(buffer)));
                /*Map<String,String> userData=new HashMap<>(16);
                userData.put("test","11111");
                geometry.setUserData(userData);*/
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return geometry;
        }else {
            System.out.println(ByteBufUtils.decode(buffer));
        }
        return null;
    }


    @Override Parameter doEncode(Geometry value) {
    return create(type,Format.FORMAT_TEXT, () ->  ByteBufUtils.encode(this.byteBufAllocator,value.toText()));
  }

  @Override public Parameter encodeNull() {
      return createNull(postGreSqlObjectId, FORMAT_TEXT);
  }

}
