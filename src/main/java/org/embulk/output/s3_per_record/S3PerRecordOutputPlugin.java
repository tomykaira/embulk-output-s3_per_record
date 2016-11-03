package org.embulk.output.s3_per_record;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;


public class S3PerRecordOutputPlugin
        implements OutputPlugin
{

    public static final String KEY_COLUMN_START_MARKER = "${";
    public static final String KEY_COLUMN_END_MARKER = "}";

    private static final Logger logger = Exec.getLogger(S3PerRecordOutputPlugin.class);
    private static volatile long nextLoggingRowCount = 1000;
    private static AtomicLong processedRows = new AtomicLong(0);
    private static long startTime = System.currentTimeMillis();

    public interface PluginTask
            extends Task
    {
        // S3 bucket name.
        @Config("bucket")
        String getBucket();

        // S3 key. {{column}} is expanded to column value in a row.
        @Config("key")
        String getKey();

        // Column name.
        @Config("data_columns")
        @ConfigDefault("[]")
        List<String> getDataColumns();

        // AWS access key id.
        @Config("aws_access_key_id")
        @ConfigDefault("null")
        Optional<String> getAwsAccessKeyId();

        // AWS secret access key
        @Config("aws_secret_access_key")
        @ConfigDefault("null")
        Optional<String> getAwsSecretAccessKey();

        @Config("serializer")
        @ConfigDefault("msgpack")
        Serializer getSerializer();

        @Config("mode")
        @ConfigDefault("multi_column")
        Mode getMode();

        // Set retry limit. Default is 2.
        @Config("retry_limit")
        @ConfigDefault("2")
        Integer getRetryLimit();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("s3_per_record output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        S3PerRecordPageOutput output = new S3PerRecordPageOutput(task, schema);
        output.open();
        return output;
    }

    private static class S3PerRecordPageOutput implements TransactionalPageOutput {
        private final TransferManager transferManager;
        private PageReader pageReader;
        private final String bucket;
        private final List<KeyPart> keyPattern;
        private final List<String> dataColumns;
        private final Schema schema;
        private final int retryLimit;
        private final Serializer serializer;
        private final Mode mode;

        public S3PerRecordPageOutput(PluginTask task, Schema schema) {
            this.schema = schema;
            bucket = task.getBucket();
            keyPattern = makeKeyPattern(task.getKey());
            dataColumns = task.getDataColumns();
            retryLimit = task.getRetryLimit();
            serializer = task.getSerializer();
            mode = task.getMode();

            AWSCredentials credentials;
            if (task.getAwsAccessKeyId().isPresent() && task.getAwsSecretAccessKey().isPresent()) {
                credentials = new BasicAWSCredentials(task.getAwsAccessKeyId().get(), task.getAwsSecretAccessKey().get());
            } else {
                credentials = new DefaultAWSCredentialsProviderChain().getCredentials();
            }
            transferManager = new TransferManager(credentials);
            logger.info("Start Upload to bucket \"{}\"", bucket);
        }

        private List<KeyPart> makeKeyPattern(final String key) {
            int offset = 0;
            int nextOffset = 0;
            ArrayList<KeyPart> parts = new ArrayList<>();
            while ((nextOffset = key.indexOf(KEY_COLUMN_START_MARKER, offset)) != -1) {
                parts.add(new ConstantStringPart(key.substring(offset, nextOffset)));
                offset = nextOffset + KEY_COLUMN_START_MARKER.length();
                nextOffset = key.indexOf(KEY_COLUMN_END_MARKER, offset);
                if (nextOffset == -1) {
                    throw new RuntimeException("Key's column name segment is not closed. Check that {{ and }} are corresponding.");
                }
                parts.add(new ColumnPart(schema.lookupColumn(key.substring(offset, nextOffset))));
                offset = nextOffset + KEY_COLUMN_END_MARKER.length();
            }
            if (offset < key.length()) {
                parts.add(new ConstantStringPart(key.substring(offset, key.length())));
            }
            return parts;
        }

        void open() {
            pageReader = new PageReader(schema);
        }

        @Override
        public void add(Page page) {
            pageReader.setPage(page);

            while (pageReader.nextRecord()) {
                String key = buildKey(pageReader);
                final byte[] payloadBytes;

                if (mode == Mode.SINGLE_COLUMN) {
                    S3PerRecordOutputColumnVisitor visitor = new MessagePackSingleColumnVisitor(pageReader);
                    schema.lookupColumn(dataColumns.get(0)).visit(visitor);
                    payloadBytes = visitor.getByteArray();
                } else {
                    S3PerRecordOutputColumnVisitor visitor = new MessagePackMultiColumnVisitor(pageReader);
                    for (String columnName : dataColumns) {
                        schema.lookupColumn(columnName).visit(visitor);
                    }
                    payloadBytes = visitor.getByteArray();
                }
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(payloadBytes.length);

                int retryCount = 0;
                int retryWait = 1000; // ms
                while (true) {
                    try (InputStream is = new ByteArrayInputStream(payloadBytes)) {
                        Upload upload = transferManager.upload(bucket, key, is, metadata);
                        upload.waitForUploadResult();
                        long rows = processedRows.incrementAndGet();
                        if (rows == nextLoggingRowCount) {
                            double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
                            logger.info(String.format("> Uploaded %,d rows in %.2f seconds", rows, seconds));
                            nextLoggingRowCount *= 2;
                        }
                        break;
                    } catch (AmazonS3Exception e) {
                        if (!e.isRetryable())
                            throw e;

                        if (retryCount > retryLimit)
                           throw e;

                        retryCount++;
                        logger.warn(String.format("> Upload failed by %s, Retry Uploading in after %d ms (%d of %d)", e.getMessage(), retryWait, retryCount, retryLimit));
                        try {
                            Thread.sleep(retryWait);
                            retryWait = retryWait * 2;
                        } catch (InterruptedException e1) {
                            Thread.currentThread().interrupt();
                        }
                    } catch (InterruptedException | IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        private String buildKey(@NotNull PageReader pageReader) {
            StringBuilder sb = new StringBuilder();
            for (KeyPart p : keyPattern) {
                sb.append(p.resolve(pageReader));
            }
            return sb.toString();
        }

        @Override
        public void finish()
        {
            close();
        }

        @Override
        public void close()
        {
            if (pageReader != null) {
                pageReader.close();
                pageReader = null;
            }
        }

        @Override
        public void abort() {
        }

        @Override
        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }
    }

    private interface KeyPart {
        @NotNull String resolve(PageReader reader);
    }

    private static class ConstantStringPart implements KeyPart {
        @NotNull private final String data;

        private ConstantStringPart(@NotNull String data) {
            this.data = data;
        }

        @Override
        public String resolve(@NotNull PageReader reader) {
            return data;
        }
    }

    private static class ColumnPart implements KeyPart {
        @NotNull private final Column column;

        private ColumnPart(@NotNull Column column) {
            this.column = column;
        }

        @Override
        public String resolve(@NotNull PageReader reader) {
            return reader.getString(column);
        }
    }

    public enum Serializer {
        MSGPACK;

        @JsonValue
        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        public static Serializer fromString(String name)
        {
            switch(name) {
                case "msgpack":
                    return MSGPACK;
                default:
                    throw new ConfigException(String.format("Unknown format '%s'. Supported formats are msgpack only", name));
            }
        }
    }

    public enum Mode {
        SINGLE_COLUMN,
        MULTI_COLUMN;

        @JsonValue
        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        public static Mode fromString(String name)
        {
            switch(name) {
                case "single_column":
                    return SINGLE_COLUMN;
                case "multi_column":
                    return MULTI_COLUMN;
                default:
                    throw new ConfigException(String.format("Unknown mode '%s'. Supported formats are single_column, multi_column", name));
            }
        }
    }
}
