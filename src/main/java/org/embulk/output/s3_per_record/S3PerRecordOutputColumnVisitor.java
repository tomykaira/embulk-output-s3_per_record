package org.embulk.output.s3_per_record;

import org.embulk.spi.ColumnVisitor;

public interface S3PerRecordOutputColumnVisitor extends ColumnVisitor {
    public byte[] getByteArray();
}
