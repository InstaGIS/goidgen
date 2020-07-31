package goidgen_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/InstaGIS/godynamodb"
	"github.com/InstaGIS/goidgen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Example() {
	// Here we're using the test instance to get a configured DynamoDB client (obviously under regular circumstances you
	// would use the AWS SDK)
	dynamoCli, err := test.GetClient()
	if err != nil {
		panic(err)
	}

	// new Audience IDGenerator
	idGenerator, err := goidgen.New(context.Background(), dynamoCli, tableWithPK, false, "Audience")
	if err != nil {
		panic(err)
	}

	// get an ID
	nextID, err := idGenerator.Next(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(nextID)
	// and the next one
	nextID, err = idGenerator.Next(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(nextID)
	// Output:
	// BK
	// Kx
}

func TestNewOK(t *testing.T) {
	t.Parallel()
	svc, err := test.GetClient()
	require.Nil(t, err)

	// test
	t.Run("PK", func(t *testing.T) {
		t.Parallel()
		idGenerator, err := goidgen.New(context.Background(), svc, tableWithPK, false, "Organization")
		assert.Nil(t, err)
		assert.NotNil(t, idGenerator)
	})
	t.Run("PKSK", func(t *testing.T) {
		t.Parallel()
		idGenerator, err := goidgen.New(context.Background(), svc, tableWithPKSK, true, "Organization")
		assert.Nil(t, err)
		assert.NotNil(t, idGenerator)
	})
}

func TestNewErrExists(t *testing.T) {
	t.Parallel()
	svc, err := test.GetClient()
	require.Nil(t, err)

	// test
	t.Run("PK", func(t *testing.T) {
		t.Parallel()
		idGenerator, err := goidgen.New(context.Background(), svc, tableWithPK, false, "UserFile")
		assert.Nil(t, idGenerator)
		assert.EqualError(t, err, "an IDGenerator named UserFile already exists")
	})
	t.Run("PKSK", func(t *testing.T) {
		t.Parallel()
		idGenerator, err := goidgen.New(context.Background(), svc, tableWithPKSK, true, "UserFile")
		assert.Nil(t, idGenerator)
		assert.EqualError(t, err, "an IDGenerator named UserFile already exists")
	})
}

func TestOpenOK(t *testing.T) {
	t.Parallel()
	svc, err := test.GetClient()
	require.Nil(t, err)

	// test
	t.Run("PK", func(t *testing.T) {
		t.Parallel()
		idGenerator, err := goidgen.Open(context.Background(), svc, tableWithPK, false, "UserFile")
		assert.Nil(t, err)
		assert.NotNil(t, idGenerator)
	})
	t.Run("PKSK", func(t *testing.T) {
		t.Parallel()
		idGenerator, err := goidgen.Open(context.Background(), svc, tableWithPKSK, true, "UserFile")
		assert.Nil(t, err)
		assert.NotNil(t, idGenerator)
	})
}

func TestOpenErrNotFound(t *testing.T) {
	t.Parallel()
	svc, err := test.GetClient()
	require.Nil(t, err)

	// test
	t.Run("PK", func(t *testing.T) {
		t.Parallel()
		idGenerator, err := goidgen.Open(context.Background(), svc, tableWithPK, false, "NonexistentGenerator")
		assert.Nil(t, idGenerator)
		assert.Equal(t, err, goidgen.ErrNotFound)
	})
	t.Run("PKSK", func(t *testing.T) {
		t.Parallel()
		idGenerator, err := goidgen.Open(context.Background(), svc, tableWithPKSK, true, "NonexistentGenerator")
		assert.Nil(t, idGenerator)
		assert.Equal(t, err, goidgen.ErrNotFound)
	})
}

func TestIDGenerator_NextOKExistingCounter(t *testing.T) {
	t.Parallel()
	svc, err := test.GetClient()
	require.Nil(t, err)

	// test
	t.Run("PK", func(t *testing.T) {
		t.Parallel()
		idGenerator, err := goidgen.Open(context.Background(), svc, tableWithPK, false, "UserFile")
		require.Nil(t, err)
		// lastNumber in db for UserFile generator is 2, so next id should be hasdhid(UserFile, 3) = 6l
		nextID, err := idGenerator.Next(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, "6l", nextID)
		// check item in dynamo
		atts := godynamodb.GetItem(t, svc, tableWithPK, map[string]string{"PK": "ID_GENERATOR#UserFile"})
		assert.NotEmpty(t, atts)
		assert.Equal(t, "ID_GENERATOR#UserFile", *atts["PK"].S)
		assert.Equal(t, "ID_GENERATOR", *atts["TYPE"].S)
		assert.Equal(t, "UserFile", *atts["name"].S)
		assert.Equal(t, "3", *atts["lastNumber"].N)
		// again: next id should be hasdhid(UserFile, 4) = E9
		nextID, err = idGenerator.Next(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, "E9", nextID)
		// check item in dynamo
		atts = godynamodb.GetItem(t, svc, tableWithPK, map[string]string{"PK": "ID_GENERATOR#UserFile"})
		assert.NotEmpty(t, atts)
		assert.Equal(t, "ID_GENERATOR#UserFile", *atts["PK"].S)
		assert.Equal(t, "ID_GENERATOR", *atts["TYPE"].S)
		assert.Equal(t, "UserFile", *atts["name"].S)
		assert.Equal(t, "4", *atts["lastNumber"].N)
	})
	t.Run("PKSK", func(t *testing.T) {
		t.Parallel()
		idGenerator, err := goidgen.Open(context.Background(), svc, tableWithPKSK, true, "UserFile")
		require.Nil(t, err)
		// lastNumber in db for UserFile generator is 2, so next id should be hasdhid(UserFile, 3) = 6l
		nextID, err := idGenerator.Next(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, "6l", nextID)
		// check item in dynamo
		atts := godynamodb.GetItem(t, svc, tableWithPKSK, map[string]string{
			"PK": "ID_GENERATOR#UserFile",
			"SK": "ID_GENERATOR#UserFile",
		})
		assert.NotEmpty(t, atts)
		assert.Equal(t, "ID_GENERATOR#UserFile", *atts["PK"].S)
		assert.Equal(t, "ID_GENERATOR#UserFile", *atts["SK"].S)
		assert.Equal(t, "ID_GENERATOR", *atts["TYPE"].S)
		assert.Equal(t, "UserFile", *atts["name"].S)
		assert.Equal(t, "3", *atts["lastNumber"].N)
		// again: next id should be hasdhid(UserFile, 4) = E9
		nextID, err = idGenerator.Next(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, "E9", nextID)
		// check item in dynamo
		atts = godynamodb.GetItem(t, svc, tableWithPKSK, map[string]string{
			"PK": "ID_GENERATOR#UserFile",
			"SK": "ID_GENERATOR#UserFile",
		})
		assert.NotEmpty(t, atts)
		assert.Equal(t, "ID_GENERATOR#UserFile", *atts["PK"].S)
		assert.Equal(t, "ID_GENERATOR#UserFile", *atts["SK"].S)
		assert.Equal(t, "ID_GENERATOR", *atts["TYPE"].S)
		assert.Equal(t, "UserFile", *atts["name"].S)
		assert.Equal(t, "4", *atts["lastNumber"].N)
	})
}

func TestIDGenerator_NextOKNewCounter(t *testing.T) {
	t.Parallel()
	svc, err := test.GetClient()
	require.Nil(t, err)

	// test
	t.Run("PK", func(t *testing.T) {
		idGenerator, err := goidgen.New(context.Background(), svc, tableWithPK, false, "Dataset")
		require.Nil(t, err)
		// new generator, so next id should be hasdhid(Dataset, 1) = Mp
		nextID, err := idGenerator.Next(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, "Mp", nextID)
		// check item in dynamo
		atts := godynamodb.GetItem(t, svc, tableWithPK, map[string]string{"PK": "ID_GENERATOR#Dataset"})
		assert.NotEmpty(t, atts)
		assert.Equal(t, "ID_GENERATOR#Dataset", *atts["PK"].S)
		assert.Equal(t, "ID_GENERATOR", *atts["TYPE"].S)
		assert.Equal(t, "Dataset", *atts["name"].S)
		assert.Equal(t, "1", *atts["lastNumber"].N)
		// again: next id should be hasdhid(Dataset, 2) = Pk
		nextID, err = idGenerator.Next(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, "Pk", nextID)
		// check item in dynamo
		atts = godynamodb.GetItem(t, svc, tableWithPK, map[string]string{"PK": "ID_GENERATOR#Dataset"})
		assert.NotEmpty(t, atts)
		assert.Equal(t, "ID_GENERATOR#Dataset", *atts["PK"].S)
		assert.Equal(t, "ID_GENERATOR", *atts["TYPE"].S)
		assert.Equal(t, "Dataset", *atts["name"].S)
		assert.Equal(t, "2", *atts["lastNumber"].N)
	})
	t.Run("PKSK", func(t *testing.T) {
		idGenerator, err := goidgen.New(context.Background(), svc, tableWithPKSK, true, "Dataset")
		require.Nil(t, err)
		// new generator, so next id should be hasdhid(Dataset, 1) = Mp
		nextID, err := idGenerator.Next(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, "Mp", nextID)
		// check item in dynamo
		atts := godynamodb.GetItem(t, svc, tableWithPKSK, map[string]string{
			"PK": "ID_GENERATOR#Dataset",
			"SK": "ID_GENERATOR#Dataset",
		})
		assert.NotEmpty(t, atts)
		assert.Equal(t, "ID_GENERATOR#Dataset", *atts["PK"].S)
		assert.Equal(t, "ID_GENERATOR#Dataset", *atts["SK"].S)
		assert.Equal(t, "ID_GENERATOR", *atts["TYPE"].S)
		assert.Equal(t, "Dataset", *atts["name"].S)
		assert.Equal(t, "1", *atts["lastNumber"].N)
		// again: next id should be hasdhid(Dataset, 2) = Pk
		nextID, err = idGenerator.Next(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, "Pk", nextID)
		// check item in dynamo
		atts = godynamodb.GetItem(t, svc, tableWithPKSK, map[string]string{
			"PK": "ID_GENERATOR#Dataset",
			"SK": "ID_GENERATOR#Dataset",
		})
		assert.NotEmpty(t, atts)
		assert.Equal(t, "ID_GENERATOR#Dataset", *atts["PK"].S)
		assert.Equal(t, "ID_GENERATOR#Dataset", *atts["SK"].S)
		assert.Equal(t, "ID_GENERATOR", *atts["TYPE"].S)
		assert.Equal(t, "Dataset", *atts["name"].S)
		assert.Equal(t, "2", *atts["lastNumber"].N)
	})
}

var benchmarkID string

func BenchmarkIDGenerator_Next(b *testing.B) {
	// get new IDGenerator
	svc, err := test.GetClient()
	require.Nil(b, err)
	idGenerator, err := goidgen.New(context.Background(), svc, tableWithPK, false, "Benchmark"+strconv.Itoa(b.N))
	require.Nil(b, err)
	require.NotNil(b, idGenerator)

	// generate ids...
	for i := 0; i < b.N; i++ {
		benchmarkID, err = idGenerator.Next(context.Background())
		assert.Nil(b, err)
		assert.NotEmpty(b, benchmarkID)
	}
}
