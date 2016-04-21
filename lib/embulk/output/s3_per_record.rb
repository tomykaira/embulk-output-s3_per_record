Embulk::JavaPlugin.register_output(
  "s3_per_record", "org.embulk.output.s3_per_record.S3PerRecordOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
