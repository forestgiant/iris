# Iris
This microservice provides mechanisms for the organization, storage, and retrieval of data in the form of key-value pairs.  These pairs can be stored into logical groups, and shared with any device capable of communicating with Iris using Google's gRPC protocol.  Iris instances can easily be joined into a cluster, and will then use the Raft Consensus Algorithm to maintain data integrity and provide fault-tolerance.

## Running Iris Nodes
Iris uses the Raft Consensus Algorithm.  For more information, please read that section below.
The first instance of Iris on a given computer can be started with the following command:

```
iris -cert /path/to/server.cer -key /path/to/server.key
```

If the SSL files you wish to use are stored as `server.cer` and `server.key` in the directory where the command is issued, you can omit the `cert` and `key` parameters.

```
iris
```

By default, running the command will use Stela to discover other instances of Iris.  If other instances are found, then this instance will be joined to that existing raft-cluster.  If no other instances are found, then this instance will start as the leader of a new raft-cluster.

If you need to run multiple instances of Iris on a single computer, you must provide an alternative `port` and `raftdir` for each instance to use at startup.  Here is an example of starting two more instances of Iris on a computer that is already running Iris using the default `port` and `raftdir`.

```
iris -port 55000 -raftdir raftDir2
iris -port 56000 -raftdir raftDir3
```

If you would prefer not to run the Stela service alongside Iris, you can indicate this by providing the `-nostela` parameter.  This will disable service registration and discovery at runtime.  If stela usage is disabled, the service will automatically start as the leader of a new raft cluster unless the address of an existing raft-cluster leader is provided using the `join` parameter.  Here is an example of starting two instances of Iris on the same machine without using stela.

```
// iris defaults to port 32000
iris -nostela       
iris -port 55000 -raftdir raftDir2 -nostela -join :32000
```

## Sources, Keys, and Values
At it's simplest, Iris is about storing and communicating key-value pairs.  In these pairs, the `Value` is represented by a series of bytes, meaning you can share just about any value you need with Iris.  When you send data to Iris, you will also send a `Key` to associate with the value.  This is simply a string you will use to refer to this specific data in the future.  Finally, you can group a set of key-value pairs into what we call a `Source`, identified by a provided string.  This allows you to manage multiple values that may share the same key across different logical contexts.

- **Source:** (string) What logical group would you like to store this information in?
- **Key**: (string) What name do you want to use to refer to this data?
- **Value**: ([]byte) What value would you like to store or communicate?

## Subscriptions
Using gRPC's streaming capabilities, Iris can publish data updates to clients that are listening for them. If desired, clients can subscribe and unsubscribe to an entire source, receiving updates when **any** value is changed for a specified source.  Alternatively, clients can be more selective, subscribing and unsubscribing individually to specific key-value pairs for specified sources.

## Raft Consensus
When joined as a cluster, Iris instances will use the Raft Consensus Algorithm to elect a leader and maintain data integrity as well as fault-tolerance.  Under the hood, we use Hashicorp's [raft](https://github.com/hashicorp/raft) pacakge to manage this behavior.

## Data Persistence
After the raft log has been updated with a given value, the data managed by Iris is stored in a [Bolt](https://github.com/boltdb/bolt) database titled `raft.db` within the raft directory specified at startup.

## Network Security
Each instance of Iris listens on 2 TCP ports.  One port is used for the gRPC API and the other is used for communications between raft-members.  The raft port is automatically assigned to the port after the configured for the gRPC API.  While the gRPC port needs to be accessible to any clients wishing to use the API, the raft port needs only be accessible to other members of the raft-cluster.

The gRPC API can be secured using Transport Layer Security (TLS) by providing the paths to both a SSL certificate and private key at startup.  This will ensure that all communication between the server and its clients is encrypted.
