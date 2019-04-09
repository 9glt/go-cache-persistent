# Golang Persistent Cache via HTTP api


start daemon
```
./cache -db ./db.db -bind 127.0.0.1:1251
```


put value
```
curl -X POST --data "value "localhost:1251/put?key=test&ttl=60"
```

get value
```
curl localhost:1251/get?key=test
```
