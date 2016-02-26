# In Place Upgrader

## Setup
1. Copy `cassandra.yaml.example` to `cassandra.yaml`

## Running

```bash
java -jar target/ipu-1.0-SNAPSHOT.jar \
  -Dcassandra.config=file://./cassandra.yaml \
  -Dcassandra.storagedir=file://./data
```

## Known problems
1. Paths may need to be changed from relative to absolute.
