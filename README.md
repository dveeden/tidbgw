# TiDB Gateway

## Building

```
go build
```

## Running

```
./tidbgw
```

Optionally set the address of PD and address to listen on via flags.


## Using

Connect your MySQL client to port 4009. TiDB Gateway will pick a random TiDB backend to route the connection to.

The gateway will detect new TiDB hosts and add these to the list of backends.

## Future

* Important: Detect removed/down hosts and automatically remove
* Use the HTTP Status API
* Filter based on labels of servers
