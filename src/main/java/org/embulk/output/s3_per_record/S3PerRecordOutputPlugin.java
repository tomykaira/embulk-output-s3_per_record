package org.embulk.output.s3_per_record;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.base.Optional;


public class S3PerRecordOutputPlugin
        implements OutputPlugin
{

    public static final String KEY_COLUMN_START_MARKER = "${";
    public static final String KEY_COLUMN_END_MARKER = "}";

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
        @Config("data_column")
        String getDataColumn();

        // AWS access key id.
        @Config("aws_access_key_id")
        @ConfigDefault("null")
        Optional<String> getAwsAccessKeyId();

        // AWS secret access key
        @Config("aws_secret_access_key")
        @ConfigDefault("null")
        Optional<String> getAwsSecretAccessKey();
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
        private final Column dataColumn;
        private final Schema schema;
        private List<Upload> uploads;

        public S3PerRecordPageOutput(PluginTask task, Schema schema) {
            this.schema = schema;
            bucket = task.getBucket();
            keyPattern = makeKeyPattern(task.getKey());
            dataColumn = schema.lookupColumn(task.getDataColumn());

            AWSCredentials credentials;
            if (task.getAwsAccessKeyId().isPresent() && task.getAwsSecretAccessKey().isPresent()) {
                credentials = new BasicAWSCredentials(task.getAwsAccessKeyId().get(), task.getAwsSecretAccessKey().get());
            } else {
                credentials = new DefaultAWSCredentialsProviderChain().getCredentials();
            }
            transferManager = new TransferManager(credentials);

            uploads = new ArrayList<>();
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

                String payload = pageReader.getString(dataColumn);
                byte[] payloadBytes = payload.getBytes();
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(payloadBytes.length);

                try (InputStream is = new ByteArrayInputStream(payloadBytes)) {
                    Upload upload = transferManager.upload(bucket, key, is, metadata);
                    uploads.add(upload);
                } catch (IOException e) {
                    throw new RuntimeException(e);
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
            try {
                for (Upload upload : uploads) {
                    upload.waitForUploadResult();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                close();
            }
        }

        @Override
        public void close()
        {
            if (pageReader != null) {
                pageReader.close();
            }
        }

        @Override
        public void abort() {
            for (Upload upload : uploads) {
                upload.abort();
            }
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
}
