curl -X POST \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":"id","method":"select","params":{"db_name":"people", "table_name": "friends"}}' \
     http://localhost:3000/api

echo "\n"

curl -X POST \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":"id","method":"select","params":{"db_name":"people", "table_name": "friends", "filter": {"Eq": [{ "Col": "Age"}, {"Val": {"Integer": 18}}]}}}' \
     http://localhost:3000/api
