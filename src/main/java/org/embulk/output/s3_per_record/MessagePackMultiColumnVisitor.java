package org.embulk.output.s3_per_record;

import org.embulk.output.s3_per_record.S3PerRecordOutputColumnVisitor;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.io.IOException;

public class MessagePackMultiColumnVisitor implements S3PerRecordOutputColumnVisitor {
    final PageReader reader;
    public final ValueFactory.MapBuilder builder;

    MessagePackMultiColumnVisitor(PageReader reader) {
        this.reader = reader;
        this.builder = new ValueFactory.MapBuilder();
    }

    public byte[] getByteArray() {
        Value value = builder.build();
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        try {
            value.writeTo(packer);
            return packer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("cannot write to msgpack");
        }
    }

    @Override
    public void booleanColumn(Column column) {
        Value columnName = ValueFactory.newString(column.getName());
        Value value = ValueFactory.newBoolean(reader.getBoolean(column));
        builder.put(columnName, value);
    }

    @Override
    public void longColumn(Column column) {
        Value columnName = ValueFactory.newString(column.getName());
        Value value = ValueFactory.newInteger(reader.getLong(column));
        builder.put(columnName, value);
    }

    @Override
    public void doubleColumn(Column column) {
        Value columnName = ValueFactory.newString(column.getName());
        Value value = ValueFactory.newFloat(reader.getDouble(column));
        builder.put(columnName, value);
    }

    @Override
    public void stringColumn(Column column) {
        Value columnName = ValueFactory.newString(column.getName());
        Value value = ValueFactory.newString(reader.getString(column));
        builder.put(columnName, value);
    }

    @Override
    public void timestampColumn(Column column) {
        Value columnName = ValueFactory.newString(column.getName());
        Value value = ValueFactory.newInteger(reader.getTimestamp(column).toEpochMilli());
        builder.put(columnName, value);
    }

    @Override
    public void jsonColumn(Column column) {
        Value columnName = ValueFactory.newString(column.getName());
        Value value = reader.getJson(column);
        builder.put(columnName, value);
    }
}
