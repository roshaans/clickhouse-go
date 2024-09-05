package column

import (
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"reflect"
	"strings"
	"time"
)

type AggregateFunction struct {
	base   Interface
	chType Type
	name   string
}

func (agg *AggregateFunction) Reset() {
	agg.base.Reset()
}

func (agg *AggregateFunction) Name() string {
	return agg.name
}

func (agg *AggregateFunction) Type() Type {
	return agg.chType
}

func (agg *AggregateFunction) ScanType() reflect.Type {
	return agg.base.ScanType()
}

func (agg *AggregateFunction) Rows() int {
	return agg.base.Rows()
}

func (agg *AggregateFunction) Row(i int, ptr bool) any {
	return agg.base.Row(i, ptr)
}

func (agg *AggregateFunction) ScanRow(dest any, rows int) error {
	return agg.base.ScanRow(dest, rows)
}

func (agg *AggregateFunction) Append(v any) ([]uint8, error) {
	return agg.base.Append(v)
}

func (agg *AggregateFunction) AppendRow(v any) error {
	return agg.base.AppendRow(v)
}

func (agg *AggregateFunction) Decode(reader *proto.Reader, rows int) error {
	return agg.base.Decode(reader, rows)
}

func (agg *AggregateFunction) Encode(buffer *proto.Buffer) {
	agg.base.Encode(buffer)
}

func parseAggregateFunction(name, chType string, timezone *time.Location) (_ Interface, err error) {
	agg := &AggregateFunction{
		name:   name,
		chType: Type(chType),
	}

	baseType := strings.TrimSpace(strings.SplitN(chType[18:len(chType)-1], ",", 2)[1])
	if agg.base, err = Type(baseType).Column(name, timezone); err == nil {
		return agg, nil
	}
	return nil, fmt.Errorf("AggregateFunction: %v", err)
}

var _ Interface = (*AggregateFunction)(nil)
