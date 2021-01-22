package org.apache.spark.sql.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.*;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CustomReadSupport extends ReadSupport<List<String>> {

  private MessageType schema ;

  public CustomReadSupport(MessageType schema) {
    this.schema = schema;
  }

  static class CustomParquetConverter extends GroupConverter {

    private static final List<String> data = new LinkedList<>();

    private List<Converter> converters = new LinkedList<>();

    static class MyPrimitiveConverter extends PrimitiveConverter {
      @Override
      public void addBinary(Binary value) {
        String str = new String(value.getBytes());
        data.add(str);
      }
      @Override
      public void addInt(int value) {
         data.add(String.valueOf(value));
      }
    }

    public CustomParquetConverter(MessageType schema) {
      schema.getColumns().forEach(column -> {
        PrimitiveType.PrimitiveTypeName primitiveTypeName = column.getPrimitiveType().getPrimitiveTypeName();
        switch (primitiveTypeName) {
          case INT32:
            this.converters.add(new MyPrimitiveConverter());
            break ;
          case BINARY:
            this.converters.add(new MyPrimitiveConverter());
            break;
          default:
            System.out.println("no type name "+ primitiveTypeName  +" converter");
            break;
        }
      });
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters.get(fieldIndex) ;
    }

    @Override
    public void start() {
      data.clear();
    }

    @Override
    public void end() {

    }

    public List<String> getCurrentRecord() {
      return  data ;
    }

  }

  static class CustomParquetRecordMaterializer extends RecordMaterializer<List<String>> {

    private MessageType schema ;
    private CustomParquetConverter converter ;

    public CustomParquetRecordMaterializer(MessageType schema) {
      this.schema = schema;
      this.converter = new CustomParquetConverter(this.schema) ;
    }

    @Override
    public List<String> getCurrentRecord() {
      return  this.converter.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
      return this.converter;
    }
  }

  @Override
  public ReadContext init(InitContext context) {
    return new ReadContext(this.schema);
  }

  @Override
  public RecordMaterializer<List<String>> prepareForRead(Configuration configuration,
                                                         Map<String, String> keyValueMetaData,
                                                         MessageType fileSchema, ReadContext readContext) {


      return  new CustomParquetRecordMaterializer(fileSchema) ;
  }
}
