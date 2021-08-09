# tcli
The ultimate CLI tool for TiKV


```
>>> help

Commands:
  .connect      connect to a tikv cluster, usage: [.connect|.conn|.c] [pd addr], example: .c 192.168.1.1:2379
  .stores       list tikv stores in cluster
  bench         bench [type] config1=value1 config2=value2 ...
                  type: ycsb
  clear         clear the screen
  echo          echo [string lit]
  exit          exit the program
  get           get [string lit]
  help          display help
  put           put [key] [value]
  scan          Scan key-value pairs in range, usage: scan [start key] [opts]
                opt format: key1=value1,key2=value2,key3=value3, 
                scan options:
                    limit: integer, default:100
                    key-only: true|false
                    count-only: true|false             
```
