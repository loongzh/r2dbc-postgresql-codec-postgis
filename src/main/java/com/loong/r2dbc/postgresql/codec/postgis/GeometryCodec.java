package com.loong.r2dbc.postgresql.codec.postgis;

import static io.r2dbc.postgresql.message.Format.FORMAT_TEXT;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.postgresql.client.Parameter;
import io.r2dbc.postgresql.message.Format;
import io.r2dbc.postgresql.util.Assert;
import io.r2dbc.postgresql.util.ByteBufUtils;
import java.util.Arrays;
import java.util.List;
import org.geotools.geometry.jts.WKBReader;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

/**
* Geometry编解码器
* @author bufanqi
* TODO: 2021/8/6
*/
public class GeometryCodec extends AbstractCodec<Geometry> {
    //private final List<Integer> postGreSqlObjectId= Arrays.asList(32028,34754);
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

        //return postGreSqlObjectId.contains(type);
        return true;
    }

    @Override Geometry doDecode(ByteBuf buffer, int dataType, Format format, Class<? extends Geometry> type) {
        if (buffer == null) {
            return null;
        }
        //if(postGreSqlObjectId.contains(dataType)){
            WKBReader reader = new WKBReader();
            Geometry geometry = null;
            try {
                geometry = reader.read(WKBReader.hexToBytes(ByteBufUtils.decode(buffer)));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return geometry;
        /*}else {
            System.out.println(ByteBufUtils.decode(buffer));
        }
        return null;*/
    }


    @Override Parameter doEncode(Geometry value) {
    return create(type,Format.FORMAT_TEXT, () ->  ByteBufUtils.encode(this.byteBufAllocator,value.toText()));
  }

  @Override public Parameter encodeNull() {
      return createNull(type, FORMAT_TEXT);
  }

}
