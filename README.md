# Golem Monitor Backend

A backend for https://stats.golem.network serving handful of REST endpoints.
Uses [redis](https://redis.io/) as a data store.

## features

Server has three features switchable at compile time. Two of them are enabled by default. 

| | stats_update | list_nodes | pingme |
| - | - | - | - |
| is default | &check; | &check; | &#10007; |
| endpoints | `/`, `/update` | `/dump`, `/v1/nodes`  | `/ping-me` | 

## endpoints

Depending on selected features server supports following endpoints:

| endpoint | http method | description |
| - | :-: | - |
| `/` | GET | redirects to `/show` (configurable) |
| `/` and `/update`| POST | accept `{json}` messages from [Golem](https://github.com/golemfactory/golem) nodes. Number of types are supported. Most notable are: node info, usage stats and p2p network info. Writes data into redis |
| `/dump` | GET | dumps whole redis store into `csv` format (compatible with [old monitor frontend](https://github.com/golemfactory/golem-monitor/blob/7cb724957247584147b50501361a8acd7f7220d7/models/dumper.js#L33))|
| `/v1/nodes` | GET | responds with `{json}` containing info about active nodes. Golem node is considered active when it has triggered  `/update` within last 120 s (configurable). Used by [new monitor frontend](https://github.com/golemfactory/golem-monitor-frontend)
| `/ping-me` | POST | accept `{json}` request to scan up to `5` ports at origin IP (read from `x-forwarded-for` header; it is by design to be deployed behind some load balancer e.g. nginx ) |

## configuration

Server can be configured through environment variables. Here are their default values
```
GOLEM_MONITOR_ADDRESS=0.0.0.0:8081
GOLEM_MONITOR_REDIS=127.0.0.1:6379
GOLEM_MONITOR_REDIRECT=/show
GOLEM_MONITOR_REDIRECT=120

# additionally, this rust built-in env var is preset to
RUST_LOG=actix_web=info,actix_redis=info,golem_monitor_rust=info
```

which means by default the backend server:
* listens on `0.0.0.0:8081`
* writes to a local redis instance at `127.0.0.1:6379`

## dev
To run in development mode use e.g.
```
RUST_LOG="actix_web=info,actix_redis=info,golem_monitor_rust=debug" \
GOLEM_MONITOR_ADDRESS="0.0.0.0:2732" \
cargo run
```

## contributing

### commit msg standard
We are generating change-log's via [git journal](https://github.com/saschagrunert/git-journal).
This requires each commit to have category. For category list see [.gitjournal.toml](.gitjournal.toml).

Please install it locally when you're planning to contribute:
1. `cargo install git-journal`
1. `git journal setup`

### versioning
 
To bump version we are using [`cargo release minor`](https://github.com/sunng87/cargo-release) or `... patch`.

Use `cargo install cargo-release` to get this tool.

### building

To build and release `.deb` package use [`cargo deb`](https://github.com/mmstick/cargo-deb) 

Use `cargo install cargo-deb` to get it.

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

### ping-me test
1. start local redis
1. run `cargo build --features=pingme`
1. run
```
curl \
    --header "Content-Type: application/json" \
    --request POST \
    --data '{"timestamp":1, "ports":[40102, 40103, 3282]}' \
    http://localhost:8081/ping-me
```
