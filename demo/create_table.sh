curl -X POST \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":"id","method":"create_table","params":{"db_name":"people", "table_name": "friends", "columns": [["Name", "String"], ["Age", "Integer"], ["Gender", "String"]], "constraints": [{"PrimaryKey": ["Name"]}]}}' \
     http://localhost:3000/api
