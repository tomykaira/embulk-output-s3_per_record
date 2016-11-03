package org.embulk.output.s3_per_record.visitor;

import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.Value;

import java.io.IOException;

public class MessagePackSingleColumnVisitor implements S3PerRecordOutputColumnVisitor {
    final PageReader reader;
    final TimestampFormatter[] timestampFormatters;
    final MessageBufferPacker packer;

    public MessagePackSingleColumnVisitor(PageReader reader, TimestampFormatter[] timestampFormatters) {
        this.reader = reader;
        this.timestampFormatters = timestampFormatters;
        this.packer = MessagePack.newDefaultBufferPacker();
    }

    public byte[] getByteArray() {
        return packer.toByteArray();
    }

    @Override
    public void booleanColumn(Column column) {
        boolean value = reader.getBoolean(column);
        try {
            packer.packBoolean(value);
        } catch (IOException e) {
            throw new RuntimeException("cannot write to msgpack");
        }
    }

    @Override
    public void longColumn(Column column) {
        long value = reader.getLong(column);
        try {
            packer.packLong(value);
        } catch (IOException e) {
            throw new RuntimeException("cannot write to msgpack");
        }
    }

    @Override
    public void doubleColumn(Column column) {
        double value = reader.getDouble(column);
        try {
            packer.packDouble(value);
        } catch (IOException e) {
            throw new RuntimeException("cannot write to msgpack");
        }
    }

    @Override
    public void stringColumn(Column column) {
        String value = reader.getString(column);
        try {
            packer.packString(value);
        } catch (IOException e) {
            throw new RuntimeException("cannot write to msgpack");
        }
    }

    @Override
    public void timestampColumn(Column column) {
        Timestamp value = reader.getTimestamp(column);
        TimestampFormatter formatter = timestampFormatters[column.getIndex()];
        try {
            packer.packString(formatter.format(value));
        } catch (IOException e) {
            throw new RuntimeException("cannot write to msgpack");
        }
    }

    @Override
    public void jsonColumn(Column column) {
        Value value = reader.getJson(column);
        try {
            value.writeTo(packer);
        } catch (IOException e) {
            throw new RuntimeException("cannot write to msgpack");
        }
    }
}
