# Overview

Replication log is a distributed systems course homework [assignment](https://docs.google.com/document/d/13akys1yQKNGqV9dGzSEDCGbHPDiKmqsZFOxKhxz841U/edit).

# Running the App

```bash
    docker-compose up --build
```

# Using the App

```bash
    # ping master
    curl localhost:8080/ping
    # ping secondary-1
    curl localhost:8081/ping
    # ping secondary-2
    curl localhost:8082/ping

    # post a message (master only)
    curl -H "Content-Type: application/json" -X POST -d '{"message":"abc"}' localhost:8080/messages

    # post a message with delay (all secondaries sleep for 5 seconds before responding)
    curl -H "Content-Type: application/json" -X POST -d '{"message":"123","delay":5}' localhost:8080/messages

    # list messages of master
    curl localhost:8080/messages
    # list messages of secondary-1
    curl localhost:8081/messages
    # list messages of secondary-2
    curl localhost:8082/messages
```

# Running Tests

```bash
    # with docker-compose
    docker-compose up --build test

    # locally (from the folder ./integration)
     MASTER_HOST=localhost MASTER_PORT=8080 SECONDARY_1_HOST=localhost SECONDARY_1_PORT=8081 SECONDARY_2_HOST=localhost SECONDARY_2_PORT=8082 go test -count=1
```
