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
export message='{"message": "123", "secondary-1":{"delay":5}, "secondary-2":{"delay":5}}'
curl -H "Content-Type: application/json" -X POST -d ${message} localhost:8080/messages

# post a message and force secondary-2 not to reply and ignore the message (the error is expected because the default write concern is w=3)
export message='{"message": "yai", "secondary-2":{"noreply":true}}'
curl -H "Content-Type: application/json" -X POST -d ${message} localhost:8080/messages

# post a message with write concern 1 (possible values are 1, 2, and 3) and expect success (even if all the replicas error)
export message='{"message": "yai", "w": 1, "secondary-1":{"noreply":true}, "secondary-2":{"noreply":true}}'
curl -H "Content-Type: application/json" -X POST -d ${message} localhost:8080/messages

# post a message with write concern 2 and expect success when a single replica errors
export message='{"message": "yai", "w": 2, "secondary-1":{"noreply":true}}'
curl -H "Content-Type: application/json" -X POST -d ${message} localhost:8080/messages

# post a message with write concern 1 doesn't wait for slow nodes
export message='{"message": "yai", "w": 1, "secondary-1":{"delay":3}}'
curl -H "Content-Type: application/json" -X POST -d ${message} localhost:8080/messages

# list messages of master
curl localhost:8080/messages
# list messages of secondary-1
curl localhost:8081/messages
# list messages of secondary-2
curl localhost:8082/messages

# flush messages of master
curl -H "Content-Type: application/json" -X POST localhost:8080/flush
# flush messages of secondary-1
curl -H "Content-Type: application/json" -X POST localhost:8081/flush
# flush messages of secondary-2
curl -H "Content-Type: application/json" -X POST localhost:8082/flush
```

# Running Tests

```bash
# with docker-compose
docker-compose up --build test

# locally (from the folder ./integration)
MASTER_HOST=localhost MASTER_PORT=8080 SECONDARY_1_HOST=localhost SECONDARY_1_PORT=8081 SECONDARY_2_HOST=localhost SECONDARY_2_PORT=8082 go test -count=1 -p=1
```
