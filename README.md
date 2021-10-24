# tcli
The ultimate CLI tool for TiKV, for human being :).


```
>>> help
Commands:
  .stores      list tikv stores in cluster
  backup       backup <prefix> <outfile> <opts>, opts: type=csv batch-size=1000(default) concurrent=1, example: backup * outfile
  bench        bench [type] config1=value1 config2=value2 ...
                  type: ycsb
  clear        clear the screen
  count        count [*|key prefix], count all keys or keys with specific prefix
  del          delete a single kv pair, usage: del(delete/rm/remove) [key]
  delall       remove all key-value pairs, DANGEROUS
  delp         delete kv pairs with specific prefix, usage: delp(deletep/rmp) keyPrefix [opts]
  echo         echo $<varname>
  env          print env variables
  exit         exit the program
  get          get [key]
  head         scan keys from $head, equals to "scan $head limit=N", usage: head <limit>
  help         display help
  hexdump      hexdump <string>
  loadcsv      loadfile [filename] [key prefix] [opts], only supports CSV now, when "key prefix" is set, will automatically add prefix to the original key,
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
  scanp       scan keys with prefix, equals to "scan [key prefix] strict-prefix=true"
  sysenv      print system env variables
  var         set variables, usage:
                var <varname>=<string value>, variable name and value are both string
                example: scan $varname or get $varname


```
