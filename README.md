RedisMapDB
==========

This project aims to be a drop-in replacement for redis backed by mapdb


The server/parsing was forked from this project:
https://github.com/spullara/redis-protocol

Backend Datbase:
https://github.com/jankotek/MapDB


Compile
```
 ./gradlew fatJar
```

Run
```
 java -jar build/libs/redis-mapdb-all-1.0.jar -threads 5
```

Options
```
Usage: redis.server.Main
  -port (-p) [Integer]  (6380)
  -location (-l) [String]  (/tmp/redismapdb)
  -dbMemory (-m) [flag] 
  -dbTransactions (-t) [flag] 
  -dbCommit (-c) [flag] 
  -backend (-backend) [String]  (mapdb)
  -threads (-threads) [Integer]  (1)
```


