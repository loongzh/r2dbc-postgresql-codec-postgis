# r2dbc-postgresql-codec-postgis

 > 引用Jackson序列化JTS实体第三方扩展org.n52.jackson:jackson-datatype-jts:1.2.9. 
 >JTS Topology Suite(JTS)拓扑套件 (org.locationtech.jts:jts-core:1.18.1 )  

r2dbc编解码器GeometryCodec实现   
#use
 pom.xml  
    
    <dependency>
      <groupId>io.githubs.loongzh</groupId>
      <artifactId>r2dbc-postgresql-codec-postgis</artifactId>
      <version>0.0.1</version>
    </dependency>
    
    <dependency>
      <groupId>org.n52.jackson</groupId>
      <artifactId>jackson-datatype-jts</artifactId>
      <version>1.2.9</version>
    </dependency>
    
 R2dbcConfig.java  
 
    @Configuration
    public class R2dbcConfig {
        @Bean
        public R2dbcCustomConversions r2dbcCustomConversions() {
            List<Converter<?, ?>> converters = new ArrayList<>();
            converters.add( new GeometryConverter());
            return new R2dbcCustomConversions(converters);
        }
        @Bean
        public Jackson2ObjectMapperBuilder jacksonBuilder() {
            Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
            builder.modulesToInstall(new JtsModule());
            return builder;
        }
    }
    
 GeometryConverter.java
    
    @WritingConverter
    @ReadingConverter
    public class GeometryConverter implements Converter<Geometry, Geometry> {
    
        @Override
        public  Geometry convert(Geometry geometry) {
            return geometry;
        }
    }   