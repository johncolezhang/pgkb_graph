## Gene variant chemical drug graph.

## Deploy Neo4j graph in server

### 1. Create neo4j folder if it's not existed
```
mkdir -p /home/zhangke/docker_volume/neo4j/upload
```

### 2. Move dump file into upload folder
```
cp <source_dump_file> /home/zhangke/docker_volume/neo4j/upload/neo4j.db.dump
```

### 3. Load data into neo4j, then persist data into localhost neo4j/data folder
```
docker run -it \
    -v=/home/zhangke/docker_volume/neo4j/data:/data \
    -v=/home/zhangke/docker_volume/neo4j/upload:/upload \
    neo4j:4.3.1 \
neo4j-admin load --from=/upload/neo4j.db.dump --database=neo4j --force
```

### 4. Delete existed neo4j container, use docker compose to create a new neo4j service.
```docker-compose -f <neo4j_compose_file> up -d```

### 5. Upgrade neo4j volume
1. Stop running neo4j container.
2. Delete data folder and upload new dump file to upload folder.
3. Initialize a new neo4j instance to load new dump file.
4. Run compose file to start neo4j service.

### dump file
```neo4j-admin dump --database=neo4j --to=d:/<file_path>```

### load file
```neo4j-admin load --from=/upload/neo4j.db.dump --database=neo4j --force```
