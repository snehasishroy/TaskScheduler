### This Readme is WIP

### Starting Zookeeper Server
This service utilizes TTL Nodes which requires Zookeeper Server >= 3.5.4.

It also requires the `extendedTypesEnabled` to be set while starting the ZK Server.

```
vim {ZK}/bin/zkEnv.sh

export SERVER_JVMFLAGS="-Xmx${ZK_SERVER_HEAP}m $SERVER_JVMFLAGS -Dzookeeper.extendedTypesEnabled=true"
```

Sample `zoo.cfg`

```
tickTime = 200
dataDir = /data/zookeeper
clientPort = 2181
initLimit = 5
syncLimit = 2
```

Starting the ZK Server
```
sudo ./zkServer.sh start-foreground
```

### Tools for Zookeeper Visualization
https://github.com/elkozmon/zoonavigator

  <img alt="img.png" height="400" src="docs/zoonavigator.png" width="500"/>