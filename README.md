# promkv

`promkv` is a joke key-value store backed by Prometheus.

Key-value pairs are stored as three different time series:

* `promkv_file_timestamp_seconds`: The timestamp where the most recent write of
  the key-value pair starts.
* `promkv_file_size_bytes`: The size of the most recent write, in bytes.
* `promkv_file_content`: The content of the most recent write. Each byte of the
  value is uploaded as a different sample one second apart.

The name of the key is stored as the `key` label.

Do not use this in production; `promkv` is almost guaranteed to do something
wrong and will probably lose data.
