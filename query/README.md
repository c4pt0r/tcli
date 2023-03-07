# Query Syntax

Basic Types:

```
Number: number such as integer or float

String: string around by ', ", \`,

Boolean: true or false
```

Select Statement:

```
SelectStmt ::= "SELECT" Fields "WHERE" WhereConditions ("ORDER" "BY" OrderByFields)? ("GROUP" "BY" GroupByFields)? ("LIMIT" LimitParameter)?

Fields ::= Field (, Field)* |
           "*"

Field ::= Expression ("AS" FieldName)?

FieldName ::= String

OrderByFields ::= OrderByField (, OrderByField)*

OrderByField ::= FieldName ("ASC" | "DESC")*

GroupByFields ::= FieldName (, FieldName)*

LimitParameter ::= Number "," Number |
                   Number

WhereConditions ::= "!"? Expression

Expression ::= "("? BinaryExpression | UnaryExpression ")"?

UnaryExpression ::= KeyValueField | String | Number | Boolean | FunctionCall

BinaryExpression ::= Expression Operator Expression |
                     Expression "BETWEEN" Expression "AND" Expression |
                     Expression "IN" "(" Expression (, Expression)* ")"

Operator ::= MathOperator | CompareOperator | AndOrOperator

AndOrOperator ::= "&" | "|"

MathOperator ::= "+" | "-" | "*" | "/"

CompareOperator ::= "=" | "!=" | "^=" | "~=" | ">" | ">=" | "<" | "<="

KeyValueField ::= "KEY" | "VALUE"

FunctionCall ::= FunctionName "(" FunctionArgs ")" |
					FunctionName "(" FunctionArgs ")" FieldAccessExpression*

FunctionName ::= String

FunctionArgs ::= FunctionArg ("," FunctionArg)*

FunctionArg ::= Expression

FieldAccessExpression ::= "[" String "]"
```

Features:

1. Scan ranger optimize: EmptyResult, PrefixScan, RangeScan, MultiGet
2. Plan support Volcano model and Batch model
3. Expression constant folding
4. Support scalar function and aggregate function
5. Support hash aggregate plan
6. Support JSON and field access expression

Examples:

```
# Simple query
q select * where key ^= 'k'

# Projection and complex condition
q select key, int(value) + 1 where key in ('k1', 'k2', 'k3') & is_int(value)

# Aggregation query
q select count(1), sum(int(value)) as sum, substr(key, 0, 2) as kprefix where key between 'k' and 'l' group by kprefix order by sum desc

# JSON access
q select key, json(value)['x']['y'] where key ^= 'k' & int(json(value)['test']) >= 1
```