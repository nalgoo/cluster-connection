# ClusterConnection

Alows Doctrine to connect to Galera cluster (multiple "master" nodes). Initial connection will be made to node1,
but when query fails because of "cluster not ready" error, connection will be made to another node and query will
be executed again.

#### Usage:

```
$em = EntityManager::create(['url' => 'mysql://user:pass@node1/db_name', 'wrapperClass' => ClusterConnection::class], $config);
$em->getConnection()->addNode('node2:3000');
$em->getConnection()->addNode('node3');
```

OR

```
$connection = ClusterConnection::createFromUrl('mysql://user:pass@node1,node2:3000,node3/db_name');
$em = EntityManager::create($connection, $config);
```

#### TODO

- Manage priority of nodes
- Transaction support

#### License

MIT
