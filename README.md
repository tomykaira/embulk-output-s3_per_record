# S3 Per Record output plugin for Embulk

This plugin uploads a column's value to S3 as one S3 object per row.
S3 object key can be composed of another column.

## Breaking Changes from 0.3.x
This plugin serialize data columns by itself.
at present, Supported formats are `msgpack` and `json`.
Because of it, config parameters are changed.

- Rename `data_column` to `data_columns`, and change type from `string` to `array`
- Add `mode`
- Add `serializer`
- Add `column_options`.

Please update your configs.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no

## Configuration

- **bucket**: S3 bucket name.
- **key**: S3 object key. `${column}` is replaced by the column's value.
- **data_columns**: Columns for object's body.
- **serializer**: Serializer format. Supported formats are `msgpack` and `json`. default is `msgpack`.
- **mode**: Set mode. Supported modes are `multi_column` and `single_column`. default is `multi_column`.
- **aws_access_key_id**: (optional) AWS access key id. If not given, [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) is used to get credentials.
- **aws_secret_access_key**: (optional) AWS secret access key. Required if `aws_access_key_id` is given.
- **retry_limit**: (default 2) On connection errors,this plugin automatically retry up to this times.
- **column_options**: Timestamp formatting option for columns.

## Example

### multi_column mode

```yaml
out:
  type: s3_per_record
  bucket: your-bucket-name
  key: "sample/${id}.txt"
  mode: multi_column
  serializer: msgpack
  data_columns: [id, payload]
```

```
id | payload (json type)
------------
 1 | hello
 5 | world
12 | embulk
```

This generates `s3://your-bucket-name/sample/1.txt` with its content `{"id": 1, "payload": "hello"}` that is formatted with msgpack format,
`s3://your-bucket-name/sample/5.txt` with its content `{"id": 5, "payload": "world"}`, and so on.

### single_column mode

```yaml
out:
  type: s3_per_record
  bucket: your-bucket-name
  key: "sample/${id}.txt"
  mode: single_column
  serializer: msgpack
  data_columns: [payload]
```

```
id | payload (json type)
------------
 1 | hello
 5 | world
12 | embulk
```

This generates `s3://your-bucket-name/sample/1.txt` with its content `"hello"` that is formatted with msgpack format,
`s3://your-bucket-name/sample/5.txt` with its content `"world"`, and so on.

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
