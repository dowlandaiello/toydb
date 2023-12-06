curl -X POST \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":"id","method":"project","params":{"db_name":"people", "table_name": "friends", "input":[[{"String":"Jenna"},{"Integer":18},{"String":"Woman"}],[{"String":"Lawwrence"},{"Integer":18},{"String":"Man"}]], "columns": ["Name", "Age"]}}' \
     http://localhost:3000/api
