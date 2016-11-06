package org.embulk.output.s3_per_record.visitor;

import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.nio.charset.StandardCharsets;

public class JsonSingleColumnVisitor implements S3PerRecordOutputColumnVisitor {
    final PageReader reader;
    final TimestampFormatter[] timestampFormatters;
    final StringBuilder sb;

    public JsonSingleColumnVisitor(PageReader reader, TimestampFormatter[] timestampFormatters) {
        this.reader = reader;
        this.timestampFormatters = timestampFormatters;
        this.sb = new StringBuilder();
    }

    public byte[] getByteArray() {
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void booleanColumn(Column column) {
        boolean value = reader.getBoolean(column);
        sb.append(value);
    }

    @Override
    public void longColumn(Column column) {
        long value = reader.getLong(column);
        sb.append(value);
    }

    @Override
    public void doubleColumn(Column column) {
        double value = reader.getDouble(column);
        sb.append(value);
    }

    @Override
    public void stringColumn(Column column) {
        String value = reader.getString(column);
        sb.append(ValueFactory.newString(value).toJson());
    }

    @Override
    public void timestampColumn(Column column) {
        Timestamp value = reader.getTimestamp(column);
        TimestampFormatter formatter = timestampFormatters[column.getIndex()];
        sb.append(ValueFactory.newString(formatter.format(value)).toJson());
    }

    @Override
    public void jsonColumn(Column column) {
        Value value = reader.getJson(column);
        sb.append(value.toJson());
    }
}
