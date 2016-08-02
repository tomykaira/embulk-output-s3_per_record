# S3 Per Record output plugin for Embulk

This plugin uploads a column's value to S3 as one S3 object per row.
S3 object key can be composed of another column.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no

## Configuration

- **bucket**: S3 bucket name.
- **key**: S3 object key. `${column}` is replaced by the column's value.
- **data_column**: Column for object's body.
- **aws_access_key_id**: (optional) AWS access key id. If not given, [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) is used to get credentials.
- **aws_secret_access_key**: (optional) AWS secret access key. Required if `aws_access_key_id` is given.
- **base64**: (default false) If true, decode the value as Base64 before uploading.
- **retry_limit**: (default 2) On connection errors,this plugin automatically retry up to this times.

## Example

```yaml
out:
  type: s3_per_record
  bucket: your-bucket-name
  key: "sample/${id}.txt"
  data_column: payload
```

```
id | payload
------------
 1 | hello
 5 | world
12 | embulk
```

This generates `s3://your-bucket-name/sample/1.txt` with its content `hello`,
`s3://your-bucket-name/sample/5.txt` with its content `world`, and so on.


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
