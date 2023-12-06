curl -X POST \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":"id","method":"insert","params":{"db_name":"people", "table_name": "friends", "values": ["Maggie", 20, "Woman"]}}' \
     http://localhost:3000/api
curl -X POST \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":"id","method":"insert","params":{"db_name":"people", "table_name": "friends", "values": ["Jenna", 18, "Woman"]}}' \
     http://localhost:3000/api
curl -X POST \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":"id","method":"insert","params":{"db_name":"people", "table_name": "friends", "values": ["Lawwrence", 18, "Man"]}}' \
     http://localhost:3000/api
