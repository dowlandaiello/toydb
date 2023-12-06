curl -X POST \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":"id","method":"create_database","params":{"db_name":"people"}}' \
     http://localhost:3000/api
