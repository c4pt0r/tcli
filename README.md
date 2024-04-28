# tcli
The ultimate CLI tool for TiKV, for human being :).

(Only support Transaction KV API now)

```
Commands:
  .stores      list tikv stores in cluster
  backup       dumps kv pairs to a csv file
  bench        bench [type], type: ycsb
  clear        clear the screen
  count        count keys or keys with specific prefix
  del          delete a single kv pair
  delall       remove all key-value pairs, DANGEROUS
  delp         delete kv pairs with specific prefix
  echo         echo $<varname>
  env          print env variables
  exit         exit the program
  get          get [key]
  head         scan keys from $head, equals to "scan $head limit=N", usage: head <limit>
  help         display help
  hexdump      hexdump <string>
  loadcsv      load csv file, use "loadcsv --help" for more details
  put          put [key] [value]
  scan         Scan keys from start key, use "scan --help" for more details
  scanp        scan keys with prefix, equals to "scan [key prefix] strict-prefix=true"
  sysenv       print system env variables
  sysvar       set system variables, usage:
                 sysvar <varname>=<string value>, variable name and value are both string
                 example: scan $varname or get $varname
  var          set variables, usage:
                 var <varname>=<string value>, variable name and value are both string
                 example: scan $varname or get $varname
```

Have a try:

1. Install `tiup` 

`curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh`

2. Deploy TiKV using `tiup`

`tiup playground --mode tikv-slim`

3. Launch `tcli`

```
$ tcli -pd localhost:2379
2021/10/24 14:57:19 main.go:98: I | pd instance info: name:"pd-0" member_id:3474484975246189105 peer_urls:"http://127.0.0.1:2380" client_urls:"http://127.0.0.1:2379"
2021/10/24 14:57:19 main.go:105: I | tikv instance info: store_id:"1" version:"5.2.1" addr:"127.0.0.1:20160" state:"Up" status_addr:"127.0.0.1:20180"
>>>
```
4. Have a try:

```
>>> put hello world
Input: put hello world
Success, Elapse: 33 ms

>>> get hello
Input: get hello
|-------|-------|
|  KEY  | VALUE |
|-------|-------|
| hello | world |
|-------|-------|
1 Record Found
Success, Elapse: 8 ms

>>> put hello_world world
Input: put hello_world world
Success, Elapse: 7 ms

>>> scanp hello
Input: scanp hello
|-------------|-------|
|     KEY     | VALUE |
|-------------|-------|
| hello       | world |
| hello_world | world |
|-------------|-------|
2 Records Found
Success, Elapse: 5 ms
```