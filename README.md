# golem-monitor-replicator

Golem Monitor Replicator accepts json messages from golem nodes, parses them and puts data into redis.

By default it listens on `0.0.0.0:8081` with `/update` route.

It also support `/ping-me` route but it is disabled by default.

It is back compatible with [golem-monitor](https://github.com/golemfactory/golem-monitor).

## dev
To run in development mode use e.g.
```
RUST_LOG="actix_web=info,actix_redis=info,golem_monitor_rust=debug" \
GOLEM_MONITOR_ADDRESS="0.0.0.0:2732" \
cargo run
```

## contributing
To bump version we are using `cargo release`.
