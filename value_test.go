// Test Value Types
package pqutil

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
)

type Case struct {
	param  Value             // setup param values
	result Value             // setup result values
	test   func(Value) error // test result values
}

var date1 = time.Date(2009, time.November, 10, 15, 0, 0, 0, time.UTC)
var date2 = time.Date(2012, time.January, 1, 23, 0, 0, 0, time.UTC)
var ok = false

var _ IterValue = &pgArray{}
var _ IterValue = &pgRecord{}
var _ MapValue = &pgRecord{}
var _ MapValue = &pgHStore{}

func gobang(t *testing.T, c *Case, msg string, q string, err error) {
	var drv driver.Value
	var by []byte
	if c.param != nil {
		drv, _ = c.param.Value()
		by, _ = c.param.bytes()
	}
	resv, _ := c.result.Value()
	resby, _ := c.result.bytes()
	t.Fatalf(`%s.
err: %v
sql: %s
---
param is: %T
param Value(): %v
param bytes(): %v
---
result is: %T
param Value(): %v
result bytes(): %s`,
		msg,
		err, q, c.param, drv, string(by),
		c.result, resv, string(resby))
}

// test helper that just returns a value and panics if it can't create it
func NewValue(k Valstructor, data ...interface{}) Value {
	if len(data) == 0 {
		data = append(data, nil)
	}
	if len(data) > 1 {
		panic("must only give ZERO or ONE arg to NewValue")
	}
	v, err := k(data[0])
	if err != nil {
		panic(err)
	}
	return v
}

var cases = map[string]*Case{
	// test encode/decode smallint
	"SELECT 128 WHERE 101 = $1": &Case{
		NewValue(SmallInt, 101),
		NewValue(SmallInt),
		func(v Value) error {
			if v.Val().(int64) != 128 {
				return fmt.Errorf(`expected value to be 128 got: %v`, v.Val())
			}
			return nil
		},
	},
	// test encode/decode integer
	"SELECT 128 WHERE 102 = $1": &Case{
		NewValue(Int, 102),
		NewValue(Int),
		func(v Value) error {
			if v.Val().(int64) != 128 {
				return fmt.Errorf(`expected value to be 128 got: %v`, v.Val())
			}
			return nil
		},
	},
	// test encode/decode bigint
	"SELECT 128 WHERE 103 = $1": &Case{
		NewValue(BigInt, 103),
		NewValue(BigInt),
		func(v Value) error {
			if v.Val().(int64) != 128 {
				return fmt.Errorf(`expected value to be 128 got: %v`, v.Val().(int64))
			}
			return nil
		},
	},
	// test encode/decode text
	"SELECT 'xyz' WHERE 'abc' = $1": &Case{
		NewValue(Text, "abc"),
		NewValue(Text),
		func(v Value) error {
			if v.String() != "xyz" {
				return fmt.Errorf(`expected value to be "xyz" got: %v`, v.String())
			}
			return nil
		},
	},
	// test encode/decode varchar(1)
	"SELECT 'A' WHERE 'B' = $1": &Case{
		NewValue(VarChar(1), "BB"), // BB will be truncated to B
		NewValue(VarChar(1)),
		func(v Value) error {
			if v.String() != "A" {
				return fmt.Errorf(`expected value to be "A" got: %v`, v.String())
			}
			return nil
		},
	},
	// test encode/decode char(1)
	"SELECT 'C' WHERE 'D' = $1": &Case{
		NewValue(Char(1), "D"),
		NewValue(Char(1)),
		func(v Value) error {
			if v.String() != "C" {
				return fmt.Errorf(`expected value to be "C" got: %v`, v.String())
			}
			return nil
		},
	},
	// test encode/decode bool
	"SELECT true WHERE $1": &Case{
		NewValue(Bool, true),
		NewValue(Bool),
		func(v Value) error {
			if !v.Val().(bool) {
				return fmt.Errorf(`expected value to be true got: %v`, v.Val().(bool))
			}
			return nil
		},
	},
	// test encode/decode real
	"SELECT 1.99 WHERE 23.34::real = $1": &Case{
		NewValue(Real, 23.34),
		NewValue(Real),
		func(v Value) error {
			if v.Val().(float64) != 1.99 {
				return fmt.Errorf(`expected value to be true got: %v`, v.Val())
			}
			return nil
		},
	},
	// test encode/decode double
	"SELECT 999.99 WHERE 1.5 = $1": &Case{
		NewValue(Double, 1.5),
		NewValue(Double),
		func(v Value) error {
			if v.Val().(float64) != float64(999.99) {
				return fmt.Errorf(`expected value to be true got: %v`, v.Val())
			}
			return nil
		},
	},
	// test encode/decode numeric(10,2)
	"SELECT 100.23::numeric(10,2) WHERE 1.11::numeric(10,2) = $1::numeric(10,2)": &Case{
		NewValue(Numeric(10, 2), "1.1111"),
		NewValue(Numeric(10, 2)),
		func(v Value) error {
			if v.String() != "100.23" {
				return fmt.Errorf(`expected value to be "100.23" got: %v`, v.String())
			}
			return nil
		},
	},
	// test encode/decode bytea
	`SELECT E'\\xDEADBEEF'::bytea WHERE E'\\xDEADBEEF'::bytea = $1`: &Case{
		NewValue(Bytea, []byte{222, 173, 190, 239}),
		NewValue(Bytea),
		func(v Value) error {
			b1 := string([]byte{222, 173, 190, 239})
			b2 := v.String()
			if b1 != b2 {
				return fmt.Errorf(`expected value to be %v got: %v`, b1, b2)
			}
			return nil
		},
	},
	// test encode/decode timestamp
	"SELECT '2012-01-01 23:00:00'::timestamp WHERE '2009-11-10 15:00:00'::timestamp = $1": &Case{
		NewValue(Timestamp, date1),
		NewValue(Timestamp),
		func(v Value) error {
			if v.Val().(time.Time) != date2 {
				return fmt.Errorf(`expected TIME Value to be %v got: %v`, date2, v.Val().(time.Time))
			}
			return nil
		},
	},
	// test encode/decode enum
	"SELECT 'dog' WHERE 'cat' = $1": &Case{
		NewValue(Enum("cat", "dog"), "cat"),
		NewValue(Enum("cat", "dog")),
		func(v Value) error {
			if v.String() != "dog" {
				return fmt.Errorf(`expected Enum Value to be "dog" got: %v`, v.String())
			}
			return nil
		},
	},
	// test encode/decode int array
	"SELECT ARRAY[1,2,3] WHERE ARRAY[1,2,3] = $1": &Case{
		NewValue(Array(Int), []interface{}{1, 2, 3}),
		NewValue(Array(Int)),
		func(v Value) error {
			if v, ok := v.(IterValue); ok {
				if len(v.Values()) != 3 {
					return fmt.Errorf("expected 3 array elements")
				}
				for i, vx := range v.Values() {
					if vx.Val().(int64) != int64(i+1) {
						return fmt.Errorf(`expected Array element #%d to be %d got %d`, i, i+1, vx.Val())
					}
				}
			} else {
				return fmt.Errorf(`expected an ARRAY Value`)
			}
			return nil
		},
	},
	// test encode/decode text array
	`SELECT ARRAY['x','"y"', 'z'] WHERE ARRAY['a','b','c'] = $1`: &Case{
		NewValue(Array(Text), []interface{}{"a", "b", "c"}),
		NewValue(Array(Text)),
		func(v Value) error {
			if v, ok := v.(IterValue); ok {
				vals := []string{"x", `"y"`, "z"}
				if len(v.Values()) != 3 {
					return fmt.Errorf("expected 3 array elements")
				}
				for i, vx := range v.Values() {
					if vx.String() != vals[i] {
						return fmt.Errorf(`expected Array element #%d to be %s got %s`, i, vals[i], vx.String())
					}
				}
			} else {
				return fmt.Errorf(`expected an ARRAY Value`)
			}
			return nil
		},
	},
	// test encode/decode bytea array
	`SELECT ARRAY[E'\\xBEEF'::bytea]`: &Case{
		nil,
		NewValue(Array(Bytea)),
		func(v Value) error {
			if v, ok := v.(IterValue); ok {
				b1 := v.Values()[0].String()
				b2 := string([]byte{190, 239})
				if b1 != b2 {
					return fmt.Errorf("expected b1 to be %v got %v", b2, b1)
				}
			} else {
				return fmt.Errorf(`expected an ARRAY Value`)
			}
			return nil
		},
	},
	// test encode/decode multi-dim int array
	"SELECT ARRAY[ARRAY[0,0],ARRAY[0,9],ARRAY[0,0]] WHERE ARRAY[ARRAY[1,2]] = $1": &Case{
		NewValue(
			Array(Array(Int)),
			[]interface{}{
				NewValue(
					Array(Int),
					[]interface{}{1, 2},
				),
			},
		),
		NewValue(Array(Array(BigInt))),
		func(v Value) error {
			if v, ok := v.(IterValue); ok {
				if len(v.Values()) != 3 {
					return fmt.Errorf("expected 3 sub-values in first array got %d", len(v.Values()))
				}
				v2 := v.ValueAt(1).(IterValue)
				if len(v2.Values()) != 3 {
					return fmt.Errorf("expected 2 sub-values in second array got %d", len(v2.Values()))
				}
				n := v2.ValueAt(1).Val().(int64)
				if n != 9 {
					return fmt.Errorf("expected result[1][1] to be 9 got: %v", n)
				}
			} else {
				return fmt.Errorf(`expected an array value`)
			}
			return nil
		},
	},
	// test encode/decode multi-dim text array
	"SELECT ARRAY[ARRAY['',''],ARRAY['','A'],ARRAY['B','C']] WHERE ARRAY[ARRAY['A','']] = $1": &Case{
		NewValue(Array(Array(Text)), []interface{}{NewValue(Array(Text), []interface{}{`A`, ``})}),
		NewValue(Array(Array(Text))),
		func(v Value) error {
			iv := v.(IterValue)
			if len(iv.Values()) != 3 {
				return fmt.Errorf("expected 3 sub-values in first array got %d", len(iv.Values()))
			}
			iv2 := iv.ValueAt(1).(IterValue)
			if len(iv2.Values()) != 3 {
				return fmt.Errorf("expected 2 sub-values in second array got %d", len(iv2.Values()))
			}
			s := iv2.ValueAt(1).String()
			if s != "A" {
				return fmt.Errorf(`expected result[1][1] to be "A" got: %v`, s)
			}
			return nil
		},
	},
	// test encode/decode bytea in row
	`SELECT Row(E'\\xBEEF'::bytea)`: &Case{
		nil,
		NewValue(Row(Bytea)),
		func(v Value) error {
			if v, ok := v.(IterValue); ok {
				b1 := v.Values()[0].String()
				b2 := string([]byte{190, 239})
				if b1 != b2 {
					return fmt.Errorf("expected b1 to be %v got %v", b2, b1)
				}
				return nil
			} else {
				return fmt.Errorf(`expected an RECORD Value`)
			}
		},
	},
	// a simple row type
	`SELECT ROW(1, '"txt"', 2.3)`: &Case{
		nil,
		NewValue(Row(BigInt, Text, Real)),
		func(v Value) error {
			vals := v.(IterValue).Values()
			if vals[0].Val().(int64) != 1 {
				return fmt.Errorf(`expected vals[0] to be 1 got: %v`, vals[0].Val())
			}
			if vals[1].String() != `"txt"` {
				return fmt.Errorf(`expected vals[1] to be "txt" got: %v`, vals[1].String())
			}
			if vals[2].Val().(float64) != 2.3 {
				return fmt.Errorf(`expected vals[2] to be "txt" got: %v`, vals[2].String())
			}
			return nil
		},
	},
	// a named record type
	`SELECT ROW('jeff', 53)`: &Case{
		nil,
		NewValue(Record(
			Col("name", Text),
			Col("age", BigInt),
		)),
		func(v Value) error {
			if v, ok := v.(MapValue); ok {
				s := v.ValueBy("name").String()
				if s != "jeff" {
					return fmt.Errorf(`expected v#name to be jeff got: %v`, s)
				}
				n := v.ValueBy("age").Val().(int64)
				if n != 53 {
					return fmt.Errorf(`expected v#age to be 5.3 got: %v`, n)
				}
			} else {
				return fmt.Errorf(`expected a MapValue`)
			}
			return nil
		},
	},
	// an array of Rows
	"SELECT ARRAY[Row(1,2),Row(2,1)]": &Case{
		nil,
		NewValue(Array(Row(BigInt, BigInt))),
		func(v Value) error {
			row := v.(IterValue).Values()[0]
			f := row.(IterValue).Values()[0]
			if f.Val().(int64) != 1 {
				return fmt.Errorf(`expected vals[0][0] to be 1 got: %v`, f.Val())
			}
			row = v.(IterValue).Values()[1]
			f = row.(IterValue).Values()[0]
			if f.Val().(int64) != 2 {
				return fmt.Errorf(`expected vals[0][0] to be 2 got: %v`, f.Val())
			}
			return nil
		},
	},
	// a more complex row type with array
	"SELECT ROW(1, 2, ARRAY['x','y'])": &Case{
		nil,
		NewValue(Row(BigInt, BigInt, Array(Text))),
		func(v Value) error {
			vals := v.(IterValue).Values()
			if vals[0].Val().(int64) != 1 {
				return fmt.Errorf(`expected vals[0] to be 1 got: %v`, vals[0].Val())
			}
			if vals[1].Val().(int64) != 2 {
				return fmt.Errorf(`expected vals[1] to be 2 got: %v`, vals[1].Val())
			}
			parts := []string{"x", "y"}
			els := vals[2].(IterValue).Values()
			if len(parts) != len(els) {
				return fmt.Errorf(`expected 2 array children got: %d`, len(els))
			}
			for i, vx := range els {
				if vx.String() != parts[i] {
					return fmt.Errorf(`expected vals[2][%d] to be %v got: %v`, i, parts[i], vx.String())
				}
			}
			return nil
		},
	},
	// an array of Rows with arrays of rows with named value
	`SELECT ARRAY[Row(ARRAY[ROW('"hello"')])]`: &Case{
		nil,
		NewValue(Array(Row(
			Array(Record(
				Col("name", Text),
			)),
		))),
		func(v Value) error {
			s := v.(IterValue).ValueAt(0).(IterValue).ValueAt(0).(IterValue).ValueAt(0).(MapValue).ValueBy("name").String()
			if s != `"hello"` {
				return fmt.Errorf(`expected this mess to have value of "\"hello\"" got: %v`, s)
			}
			return nil
		},
	},
	// HStore read/write
	// REQUIRES HSTORE EXTENSION
	`SELECT '"k1" => "v1", "k\"2" => "\"v2\"", "k3" => NULL'::hstore
	 WHERE ($1::hstore -> 'a') = 'b'`: &Case{
		NewValue(HStore, []byte(`"a" => "b", "x" => "y"`)),
		NewValue(HStore),
		func(v Value) error {
			m1 := v.(MapValue).ValueBy(`k1`).String()
			m2 := v.(MapValue).ValueBy(`k"2`).String()
			m3 := v.(MapValue).ValueBy(`k3`)
			if m1 != `v1` {
				return fmt.Errorf(`expected "k1" => "v1" got: %v`, m1)
			}
			if m2 != `"v2"` {
				return fmt.Errorf(`expected "k\"2" => "\"v1\"" got: %v`, m2)
			}
			if m3 != nil {
				return fmt.Errorf(`expected "k3" => nil got: %v`, m3)
			}
			return nil
		},
	},
}

// manuall construct a customer type

// create a customer record, read it back out from the db and print out the name"

// the standard database/sql way
// type Customer struct {
// 	id int
// 	name string
// }
//
//
//
// // the really manual way (using Values with database/sql)
// Customer := Record(
// 	Col("id", Integer, "pk").
// 	Col("name", Text),
// )
// db = sql.Open("postgres", "")
// c := Customer(&Values{1, "bob"})
// c.Set("id", 1)
// c.Set("name", "bob")
// rows,_ := db.Query(`INSERT INTO customer (id,name) VALUES ($1,$2) RETURNING *`, c.Get("id"), c.Get("name"))
// for rows.Next() {
// 	res := Customer()
// 	rows.Scan(res.Get("id"), res.Get("name"))
// }
// fmt.Println("name", res.Get("name"))
//
//
//
// // the manual way
// // * let pqutil.DB handle the boring bits of INSERT
// // * let pqutil.DB handle the scanning and get access to record
// Customer := Record(
// 	Col("id", Integer, "pk").
// 	Col("name", Text),
// )
// db,_ := Open("")
// c := Customer()
// c.Set("id", 1)
// c.Set("name", "bob")
// db.Insert("customer", c)
// fmt.Println("name", res.Get("name"))
//
// // the automagical way
// // * let pqutil.DB create the customer type for you via pqutil.Relation
// // * let pqutil.Relation handle the INSERT SQL
// // * let pqutil.Relation handle record creation and scanning after a Query
// db,_ := Open("")
// customers,_ := db.Relation("customer") // reads type into from pg_attribute etc
// c := customers.New() // new  customer Value
// c.Set("id", 1)
// c.Set("name", "bob")
// customers.Insert(c) // no need to type any SQL
// fmt.Println("name", res.Get("name"))

func TestCases(t *testing.T) {
	db, err := sql.Open("postgres", "sslmode=disable dbname=pql_test")
	if err != nil {
		t.Fatal(err)
	}
	for q, c := range cases {
		// make a query using the param Value from the
		// case if we have one
		var rows *sql.Rows
		if c.param == nil {
			rows, err = db.Query(q)
		} else {
			rows, err = db.Query(q, &c.param)
		}
		if err != nil {
			switch e := err.(type) {
			case pq.PGError:
				gobang(t, c, e.Get('M'), q, err)
			default:
				gobang(t, c, "Error during Query", q, err)
			}
		}
		// scan first row into result Value from case
		gotResult := 0
		for rows.Next() {
			err = rows.Scan(c.result)
			if err != nil {
				gobang(t, c, "Error during Scan", q, err)
			}
			gotResult++
		}
		err = rows.Err()
		if err != nil {
			gobang(t, c, "Error after row", q, err)
		}
		// check that we have a single row result
		if gotResult != 1 {
			gobang(t, c, "Expected a result row", q, nil)
		}
		// check result is not null
		if c.result.IsNull() {
			gobang(t, c, "Result should never be null", q, nil)
		}
		// run the associated test func
		err = c.test(c.result)
		if err != nil {
		}
	}
}

func TestTextVal(t *testing.T) {
	v, err := Text("aaa")
	if err != nil {
		t.Error(err)
	}
	err = v.Scan(v.Val())
	if err != nil {
		t.Error(err)
	}
	if v.IsNull() {
		t.Errorf("expected val to not be NULL")
	}
	if v.Val().(string) != "aaa" {
		t.Errorf("unexpected val: %v", v.Val())
	}
	v.Scan(nil)
	if v.Val() != nil {
		t.Errorf("expected val to be nil got: %v", v.Val())
	}
	if !v.IsNull() {
		t.Errorf("expected val to be NULL")
	}
}

func TestByteaVal(t *testing.T) {
	v, err := Bytea([]byte("aaa"))
	if err != nil {
		t.Error(err)
	}
	err = v.Scan(v.Val())
	if err != nil {
		t.Error(err)
	}
	if v.IsNull() {
		t.Errorf("expected val to not be NULL")
	}
	if string(v.Val().([]byte)) != "aaa" {
		t.Errorf("unexpected val: %v", v.Val())
	}
	v.Scan(nil)
	if v.Val() != nil {
		t.Errorf("expected val to be nil got: %v", v.Val())
	}
	if !v.IsNull() {
		t.Errorf("expected val to be NULL")
	}
}

func TestIntVal(t *testing.T) {
	v, err := BigInt(123)
	if err != nil {
		t.Error(err)
	}
	err = v.Scan(v.Val())
	if err != nil {
		t.Error(err)
	}
	if v.IsNull() {
		t.Errorf("expected val to not be NULL")
	}
	if v.Val().(int64) != 123 {
		t.Errorf("unexpected val: %v", v.Val())
	}
	v.Scan(nil)
	if v.Val() != nil {
		t.Errorf("expected val to be nil got: %v", v.Val())
	}
	if !v.IsNull() {
		t.Errorf("expected val to be NULL")
	}
}

func TestFloatVal(t *testing.T) {
	v, err := Real(1.23)
	if err != nil {
		t.Error(err)
	}
	err = v.Scan(v.Val())
	if err != nil {
		t.Error(err)
	}
	if v.IsNull() {
		t.Errorf("expected val to not be NULL")
	}
	if v.Val().(float64) != 1.23 {
		t.Errorf("unexpected val: %v", v.Val())
	}
	v.Scan(nil)
	if v.Val() != nil {
		t.Errorf("expected val to be nil got: %v", v.Val())
	}
	if !v.IsNull() {
		t.Errorf("expected val to be NULL")
	}
}

func TestBoolVal(t *testing.T) {
	v, err := Bool(true)
	if err != nil {
		t.Error(err)
	}
	err = v.Scan(v.Val())
	if err != nil {
		t.Error(err)
	}
	if v.IsNull() {
		t.Errorf("expected val to not be NULL")
	}
	if v.Val().(bool) != true {
		t.Errorf("unexpected val: %v", v.Val())
	}
	v.Scan(nil)
	if v.Val() != nil {
		t.Errorf("expected val to be nil got: %v", v.Val())
	}
	if !v.IsNull() {
		t.Errorf("expected val to be NULL")
	}
}

func TestEnumVal(t *testing.T) {
	v, err := Enum("x", "y")("y")
	if err != nil {
		t.Error(err)
	}
	err = v.Scan(v.Val())
	if err != nil {
		t.Error(err)
	}
	if v.IsNull() {
		t.Errorf("expected val to not be NULL")
	}
	if v.Val().(string) != "y" {
		t.Errorf("unexpected val: %v", v.Val())
	}
	v.Scan(nil)
	if v.Val() != nil {
		t.Errorf("expected val to be nil got: %v", v.Val())
	}
	if !v.IsNull() {
		t.Errorf("expected val to be NULL")
	}
}

func TestNumericVal(t *testing.T) {
	v, err := Numeric(5, 2)("100.12")
	if err != nil {
		t.Error(err)
	}
	err = v.Scan(v.Val())
	if err != nil {
		t.Error(err)
	}
	if v.IsNull() {
		t.Errorf("expected val to not be NULL")
	}
	if v.Val().(string) != "100.12" {
		t.Errorf("unexpected val: %v", v.Val())
	}
	v.Scan(nil)
	if v.Val() != nil {
		t.Errorf("expected val to be nil got: %v", v.Val())
	}
	if !v.IsNull() {
		t.Errorf("expected val to be NULL")
	}
}

func TestTimestampVal(t *testing.T) {
	v, err := Timestamp("2001-02-03")
	if err != nil {
		t.Error(err)
	}
	err = v.Scan(v.Val())
	if err != nil {
		t.Error(err)
	}
	if v.IsNull() {
		t.Errorf("expected val to not be NULL")
	}
	if v.String() != "2001-02-03T00:00:00Z" {
		t.Errorf("unexpected val: %v", v.Val())
	}
	v.Scan(nil)
	if v.Val() != nil {
		t.Errorf("expected val to be nil got: %v", v.Val())
	}
	if !v.IsNull() {
		t.Errorf("expected val to be NULL")
	}
}

func TestArrayVal(t *testing.T) {
	v, err := Array(Int)([]interface{}{1, 2})
	if err != nil {
		t.Error(err)
	}
	err = v.Scan(v.Val())
	if err != nil {
		t.Error(err)
	}
	if v.IsNull() {
		t.Errorf("expected val to not be NULL")
	}
	switch vals := v.Val().(type) {
	case nil:
		t.Errorf("expected val to be []interface{}")
	case []interface{}:
		if len(vals) != 2 {
			t.Errorf("expected val to be [2]interface{}")
		}
		if vals[0].(int64) != 1 {
			t.Errorf("vals[0] should be 1 got: %v", vals[0])
		}
		if vals[1].(int64) != 2 {
			t.Errorf(`vals[0] should be "A" got: %v`, vals[1])
		}
		v.Scan(nil)
		if v.Val() != nil {
			t.Errorf("expected val to be nil got: %v", v.Val())
		}
		if !v.IsNull() {
			t.Errorf("expected val to be NULL")
		}
	default:
		t.Errorf("unexpected return type %T for Val()", v.Val())
	}
}

func TestRowVal(t *testing.T) {
	v, err := Row(Int, Text)([]interface{}{1, "A"})
	if err != nil {
		t.Error(err)
	}
	err = v.Scan(v.Val())
	if err != nil {
		t.Error(err)
	}
	if v.IsNull() {
		t.Errorf("expected val to not be NULL")
	}
	switch vals := v.Val().(type) {
	case nil:
		t.Errorf("expected val to be []interface{}")
	case []interface{}:
		if len(vals) != 2 {
			t.Errorf("expected val to be [2]interface{}")
		}
		if vals[0].(int64) != 1 {
			t.Errorf("vals[0] should be 1 got: %v", vals[0])
		}
		if vals[1].(string) != "A" {
			t.Errorf(`vals[0] should be "A" got: %v`, vals[1])
		}
		v.Scan(nil)
		if v.Val() != nil {
			t.Errorf("expected val to be nil got: %v", v.Val())
		}
		if !v.IsNull() {
			t.Errorf("expected val to be NULL")
		}
	default:
		t.Errorf("unexpected return type %T for Val()", v.Val())
	}
}

func TestRecordVal(t *testing.T) {
	v, err := Record(
		Col("a", Int),
		Col("b", Text),
	)([]interface{}{1, "A"})
	if err != nil {
		t.Error(err)
	}
	err = v.Scan(v.Val())
	if err != nil {
		t.Error(err)
	}
	if v.IsNull() {
		t.Errorf("expected val to not be NULL")
	}
	switch vals := v.Val().(type) {
	case nil:
		t.Errorf("expected val to be []interface{}")
	case []interface{}:
		if len(vals) != 2 {
			t.Errorf("expected val to be [2]interface{}")
		}
		if vals[0].(int64) != 1 {
			t.Errorf("vals[0] should be 1 got: %v", vals[0])
		}
		if vals[1].(string) != "A" {
			t.Errorf(`vals[0] should be "A" got: %v`, vals[1])
		}
		v.Scan(nil)
		if v.Val() != nil {
			t.Errorf("expected val to be nil got: %v", v.Val())
		}
		if !v.IsNull() {
			t.Errorf("expected val to be NULL")
		}
	default:
		t.Errorf("unexpected return type %T for Val()", v.Val())
	}
}

func TestHStoreVal(t *testing.T) {
	v, err := HStore(map[string]string{
		"k1": "v1",
		"k2": "v2",
	})
	if err != nil {
		t.Error(err)
	}
	err = v.Scan(v.Val())
	if err != nil {
		t.Error(err)
	}
	if v.IsNull() {
		t.Errorf("expected val to not be NULL")
	}
	switch vals := v.Val().(type) {
	case nil:
		t.Errorf("expected val to be []interface{}")
	case map[string]string:
		if len(vals) != 2 {
			t.Errorf("expected 2 key/vals ")
		}
		if vals["k1"] != "v1" {
			t.Errorf("expected k1 => v1 got: %v", vals["k1"])
		}
		if vals["k2"] != "v2" {
			t.Errorf("expected k2 => v2 got: %v", vals["k2"])
		}
		v.Scan(nil)
		if v.Val() != nil {
			t.Errorf("expected val to be nil got: %v", v.Val())
		}
		if !v.IsNull() {
			t.Errorf("expected val to be NULL")
		}
	default:
		t.Errorf("unexpected return type %T for Val()", v.Val())
	}
}
