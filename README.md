# tcli
The ultimate CLI tool for TiKV


```
>>> help

Commands:
  .config       edit tikv config
  .connect      connect to a tikv cluster, usage: [.connect|.conn|.c] [pd addr], example: .c 192.168.1.1:2379
  .stores       list tikv stores in cluster
  bench         bench [type] config1=value1 config2=value2 ...
                  type: ycsb
  clear         clear the screen
  del           delete a single kv pair, usage: del(delete/rm/remove) [key or keyPrefix] [opts]
  delp          delete kv pairs with specific prefix, usage: delp(deletep/rmp) keyPrefix [opts]
  echo          echo $<varname>
  exit          exit the program
  get           get [string lit]
  help          display help
  loadfile      loadfile [filename] [key prefix] [opts], only supports CSV now, when "key prefix" is set, will automatically add prefix to the original key,
             opts:
             batch-size: int, how many records in one tikv transaction, default: 1000
  put       put [key] [value]
  scan      Scan key-value pairs in range, usage: scan [start key] [opts]
                opt format: key1=value1,key2=value2,key3=value3,
                scan options:
                  limit: integer, default:100
                  key-only: true(1)|false(0)
                  strict-prefix: true(1)|false(0)
                  count-only: true(1)|false(0)
  var      set variables, usage:
         let <varname>=<string value>, variable name and value are both string

      Example: scan $varname / get $varname


```
