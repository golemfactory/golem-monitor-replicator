# golem-monitor-replicator

Golem Monitor Replicator accepts json messages from golem nodes, parses them and puts data into redis.

By default it:
* listens on `0.0.0.0:8081` with `/` and `/update` routes.
* writes to a local redis instance at `127.0.0.1:6379`

It can also support `/ping-me` route but it is disabled by default (feature `pingme`). 

It is back compatible with [golem-monitor](https://github.com/golemfactory/golem-monitor).

## dev
To run in development mode use e.g.
```
RUST_LOG="actix_web=info,actix_redis=info,golem_monitor_rust=debug" \
GOLEM_MONITOR_ADDRESS="0.0.0.0:2732" \
cargo run
```

## contributing

### Commit msg standard
We are generating change-log's via [git journal](https://github.com/saschagrunert/git-journal).
This requires each commit to have category. For category list see [.gitjournal.toml](.gitjournal.toml).

Please install it locally when you're planning to contribute:
1. `cargo install git-journal`
1. `git journal setup`

### versioning
To bump version we are using `cargo release minor`.

### local e2e test
1. make sure local redis server is running
1. run golem monitor replicator (e.g. `cargo run`)
1. then post some request to it
```
curl \
    --header "Content-Type: application/json" \
    --request POST \
    --data '{
        "proto_ver": 2,
        "data": {
            "known_tasks": 0,
            "supported_tasks": 2721,
            "computed_tasks": 0,
            "tasks_with_errors": 0,
            "tasks_with_timeout": 0,
            "tasks_requested": 314,
            "sessid": "5bbeefa5-423e-425c-92dd-bde94e2c9777",
            "cliid": "some-fake-cliid",
            "timestamp": 1,
            "type": "Stats"
        }
    }' \
    http://localhost:8081
```    