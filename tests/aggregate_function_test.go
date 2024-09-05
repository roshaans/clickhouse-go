// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tests

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestAggregateFunction(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 1, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}

	const ddl = `
		CREATE TABLE test_aggregate_function (
			  Col1 UInt64
			, Col2 AggregateFunction(sum, UInt64)
			, Col3 AggregateFunction(count, UInt64)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_aggregate_function")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))

	const rawTableDDL = `
		CREATE TABLE raw_data (
			  Col1 UInt64
			, Col2 UInt64
			, Col3 UInt64
		) ENGINE = MergeTree() ORDER BY tuple()
	`
	defer func() {
		conn.Exec(ctx, "DROP TABLE raw_data")
	}()
	require.NoError(t, conn.Exec(ctx, rawTableDDL))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO raw_data VALUES (42, 100, 5)
	`))

	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO test_aggregate_function
		SELECT
			Col1,
			sumState(Col2),
			countState(Col3)
		FROM raw_data
		GROUP BY Col1
	`))

	var result struct {
		Col1 uint64
		Col2 uint64
		Col3 uint64
	}
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT
				Col1,
				sumMerge(Col2) AS Col2,
				countMerge(Col3) AS Col3
		FROM test_aggregate_function
		GROUP BY Col1;
	`).ScanStruct(&result))

	assert.Equal(t, uint64(42), result.Col1)
	assert.Equal(t, uint64(100), result.Col2)
	assert.Equal(t, uint64(1), result.Col3)
}
