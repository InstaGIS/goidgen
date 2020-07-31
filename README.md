# ID Generator library

This GO library provides a simple hashid-backed ID generator, keeping its state DynamoDB. 

## Requirements

* GO 1.14

## Usage

```go
package main

import (
    "context"
    "log"

    "github.com/aws/aws-sdk-go-v2/aws/external"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/InstaGIS/goidgen"
)

func main() {
    cfg, err := external.LoadDefaultAWSConfig()
    if err != nil {
      log.Fatal(err)
    }
    dynamoCli := dynamodb.New(cfg)
    table := "MyServiceTable"

    // new Audience IDGenerator
    ctx := context.Background()
    idGenerator, err := goidgen.New(ctx, dynamoCli, table, false, "Audience")
    if err != nil {
        log.Fatal(err)
    }

    // get an ID
    nextID, err := idGenerator.Next(ctx)
    if err != nil {
        log.Fatal(err)
    }
    log.Println(nextID)

    // and the next one
    nextID, err = idGenerator.Next(ctx)
    if err != nil {
        log.Fatal(err)
    }
    log.Println(nextID)
}
```

## Dynamo table design

Properties:
* PK: partition key (in the form ID_GENERATOR#{{name}}).
* SK: sort key, optional. If used, its value is the same as PK.
* TYPE: entity type (fixed value: ID_GENERATOR).
* name: generator name, unique (enforced by PK).
* lastNumber: last used int to generate an id (number).

We work with single-table modeling when possible, so this entity design follows some conventions to play nicely along
other entities in complex tables:
* The metadata fields are in uppercase and the remaining properties in camelcase.
* The name of the partition key is PK.
* The name of the sort key is SK, and it's optional.
* There's a TYPE property to make easier identify the type of the items (to perform migrations or any other task).

There are only three access patterns:

* New: puts an IdGenerator item.
* Open: gets an IdGenerator item.
* Next: updates the lastNumber of an existing IdGenerator item, increasing its lastNumber by 1.
