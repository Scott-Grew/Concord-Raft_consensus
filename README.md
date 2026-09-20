# Concord

Raft consensus in Go.

Raft is how a group of servers agrees on a single ordered log even when some of them crash or
lose contact. One server is elected leader. It takes new entries, sends them to the others, and
marks an entry committed once a majority has it. If the leader dies, the rest elect a new one and
carry on without losing anything that was committed.

This covers leader election and log replication. It follows Figure 2 of the Raft paper, including
the two rules people usually get wrong. A leader only commits entries from its own term by
counting replicas. A follower that rejects an append tells the leader which term conflicted, so
the leader backs up a whole term at a time instead of one entry.

## Tests

The test harness runs each server as its own process and talks to them over RPC. The tests kill
servers, cut the network, drop and reorder messages, and check that the log never disagrees.

All 13 tests pass 50 runs in a row with Go's race detector on.

```
go test -race
```

## Running it

The test harness is a separate package and is not included here. `raft.go` drops into it as the
`raft1` package.

## Not done

Persistence and snapshots are stubbed. A server that restarts starts from an empty log.
