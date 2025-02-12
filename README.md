# sqlite-change-stream

`sqlite-change-stream` is a small command-line tool developed that monitors changes in a SQLite database in real-time and outputs change events in JSON format to stdout.

## Usage

```sh
sqlite-change-stream <database_file>
```

### example

```sh
$ sqlite-change-stream ./test.db
Database journal mode: wal
Starting database monitoring: /tmp/test.dh
{"action":"insert","id":1,"new_data":"{\"name\":\"user1\",\"age\":18,\"address\":\"address1\",\"phone\":\"12345678\",\"sex\":1}","old_data":null,"rowid":2,"table":"user","timestamp":"2025-02-12 16:17:39"}
{"action":"update","changed_fields":{"age":{"new":19,"old":18},"phone":{"new":"1345678","old":"12345678"}},"id":2,"rowid":2,"table":"user","timestamp":"2025-02-12 16:18:00"}
{"action":"delete","id":3,"new_data":null,"old_data":"{\"name\":\"user1\",\"age\":19,\"address\":\"address1\",\"phone\":\"1345678\",\"sex\":1}","rowid":2,"table":"user","timestamp":"2025-02-12 16:18:09"}
^C
Received interrupt signal, cleaning up...
Cleanup completed


$ sqlite-change-stream ./test.db | jq
{
    "action":"insert",
    "id":1,
    "new_data":"{\"name\":\"user1\",\"age\":18,\"address\":\"address1\",\"phone\":\"12345678\",\"sex\":1}",
    "old_data":null,
    "rowid":2,
    "table":"user",
    "timestamp":"2025-02-12 16:17:39"
}


$ sqlite-change-stream ./test.db | jq '.changed_fields.name'
{
  "new": "user2",
  "old": "user1"
}
```
