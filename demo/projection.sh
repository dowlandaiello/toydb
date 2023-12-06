curl -X POST \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","id":"id","method":"project","params":{"db_name":"people", "table_name": "friends", "input":[[["Name",{"String":"Maggie"}],["Age",{"Integer":20}],["Gender",{"String":"Woman"}]],[["Name",{"String":"Jenna"}],["Age",{"Integer":18}],["Gender",{"String":"Woman"}]],[["Name",{"String":"Lawwrence"}],["Age",{"Integer":18}],["Gender",{"String":"Man"}]]], "columns": ["Name", "Age"]}}' \
     http://localhost:3000/api
