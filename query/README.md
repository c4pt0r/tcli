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

FunctionCall ::= FunctionName "(" FunctionArgs ")"

FunctionName ::= String

FunctionArgs ::= FunctionArg ("," FunctionArg)*

FunctionArg ::= Expression
```

Features:

1. Scan ranger optimize: EmptyResult, PrefixScan, RangeScan, MultiGet
2. Plan support Volcano model and Batch model
3. Expression constant folding
4. Support scalar function and aggregate function
5. Support hash aggregate plan
