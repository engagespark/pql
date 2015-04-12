// utility package for working with PostgreSQL arrays and composite types.
package pqutil

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

var nullb = []byte("NULL")

// quote a byte string
// where t is the string quoting type
// 		0 = none
// 		1 = double quoted escaped \" (array style)
// 		2 = double quoted escaped "" (row style)
func escape(s []byte, t int) []byte {
	if t == 0 {
		return s
	}
	// replace all \ with \\
	s = bytes.Replace(s, []byte(`\`), []byte(`\\`), -1)
	switch t {
	case 1:
		// replace all " with \"
		s = bytes.Replace(s, []byte(`"`), []byte(`\"`), -1)
	case 2:
		// replace all " with ""
		s = bytes.Replace(s, []byte(`"`), []byte(`""`), -1)
	}
	return s
}

// converts n to int64
// returns error if n does not fit into the int bitsize
func fitInt(v interface{}, bitSize int) (r int64, err error) {
	// convert to int64
	switch n := v.(type) {
	case int:
		r = int64(n)
	case int8:
		r = int64(n)
	case int16:
		r = int64(n)
	case int32:
		r = int64(n)
	case int64:
		r = n
	case uint8:
		r = int64(n)
	case uint16:
		r = int64(n)
	case uint32:
		r = int64(n)
	case uint:
		if n < math.MaxInt64 {
			r = int64(n)
		} else {
			return 0, fmt.Errorf("Cannot fit %v into int%d", n, bitSize)
		}
	case uint64:
		if n < math.MaxInt64 {
			r = int64(n)
		} else {
			return 0, fmt.Errorf("Cannot fit %v into int%d", n, bitSize)
		}
	}
	// check fits
	ok := false
	switch bitSize {
	case 8:
		ok = r < math.MaxInt8
	case 16: // INT2
		ok = r < math.MaxInt16
	case 32: // INT4
		ok = r < math.MaxInt32
	case 64: // INT8
		ok = true
	default:
		return 0, fmt.Errorf("invalid bitSize %d", bitSize)
	}
	if !ok {
		return 0, fmt.Errorf("Cannot fit %v into int%d", r, bitSize)
	}
	return r, nil
}

func srcToBytes(src interface{}) (b []byte, err error) {
	switch x := src.(type) {
	case string:
		b = []byte(x)
	case []byte:
		b = x
	default:
		err = fmt.Errorf("Cannot parse %T into Value expected []byte or string", src)
	}
	return
}

func parseHStore(s []byte) (map[string]string, error) {
	m := make(map[string]string)
	st := 0 // 0=waiting-for-key, 1=inkey 2=waiting-for-val 3=inval
	ka := -1
	kz := -1
	va := -1
	vz := -1
	for i := 0; i < len(s); i++ {
		b := s[i]
		switch {
		case b == '\\':
			i++
		case st == 0:
			switch {
			case b == '"':
				ka = i + 1
				st++
			}
		case st == 1:
			switch {
			case b == '"':
				kz = i - 1
				st++
			}
		case st == 2:
			switch {
			case b == 'N' && s[i+1] == 'U' && s[i+2] == 'L' && s[i+3] == 'L':
				va = i
				vz = i + 3
				st = 0
			case b == '"':
				va = i + 1
				st++
			}
		case st == 3:
			switch {
			case b == '"':
				vz = i - 1
				st = 0
			}
		}
		if kz != -1 && vz != -1 {
			k := s[ka : kz+1]
			v := s[va : vz+1]
			if string(v) == "NULL" {
				// do something? .. for now just ignore NULL vals
			} else {
				k = bytes.Replace(k, []byte(`\\`), []byte(`\`), -1)
				k = bytes.Replace(k, []byte(`\"`), []byte(`"`), -1)
				v = bytes.Replace(v, []byte(`\\`), []byte(`\`), -1)
				v = bytes.Replace(v, []byte(`\"`), []byte(`"`), -1)
				m[string(k)] = string(v)
			}
			ka = -1
			kz = -1
			va = -1
			vz = -1
		}
	}
	return m, nil
}

// take a byte representation of an array or row and return
// each element unescaped
// will also decode any hex bytea fields (although not sure if that should be done here really)
func split(s []byte) ([][]byte, error) {
	// debug
	// fmt.Println("---------------")
	// fmt.Println(string(s))
	parts := make([][]byte, 0)
	ignore := false
	dep := 0
	var mode byte // }=array )=record
	var closer byte
	a := -1
	z := -1
	for i, b := range s {
		switch {
		// sanity check
		case i == 0:
			switch b {
			case '{':
				mode = '}'
			case '(':
				mode = ')'
			default:
				return nil, fmt.Errorf("cannot split data. Unknown format: %s", string(s))
			}
		// if not inside value
		case a == -1:
			switch {
			// skip whitespace
			case b == ' ' || b == ',':
				// consume whitespace or commas
			// mark val wrapped in { }
			case b == '{':
				a = i
				dep++
				closer = '}'
			// mark val wrapped in "
			case b == '"':
				a = i + 1
				closer = '"'
			// anything else mark
			default:
				a = i
				closer = ','
			}
		// EOF
		case i == len(s)-1:
			if b != mode {
				return nil, fmt.Errorf("cannot split data. missing '%s': %s", string([]byte{mode}), string(s))
			}
			z = i - 1
		// start collecting val
		case a != -1:
			switch {
			// skip esc char and mark next char as unimportant (for array escaping)
			case !ignore && mode == '}' && b == '\\':
				ignore = true
			// treat "" as " (for row escaping)
			case !ignore && mode == ')' && b == '"' && s[i+1] == '"':
				ignore = true
			// this byte will not cause end
			case ignore:
				ignore = false
			// mark end of array
			case closer == '}' && (b == '}' || b == '}'):
				switch {
				case b == '{':
					dep++
				case b == '}':
					dep--
					if dep == 0 {
						z = i
					}
				}
			// mark end of quoted
			case closer == '"' && b == closer:
				z = i - 1
			// mark end of simple , val
			case closer == ',' && b == closer:
				z = i - 1
			}
		}
		// check for end
		if z != -1 {
			part := s[a : z+1]
			// unescape
			part = bytes.Replace(part, []byte(`\\`), []byte(`\`), -1)
			if mode == '}' {
				part = bytes.Replace(part, []byte(`\"`), []byte(`"`), -1)
			} else if mode == ')' {
				part = bytes.Replace(part, []byte(`""`), []byte(`"`), -1)
			}
			// check if it looks like a hex bytea in here and try to decode it
			if len(part) >= 2 && part[0] == '\\' && part[1] == 'x' {
				part, _ = hex.DecodeString(string(part[2:len(part)]))
			}
			parts = append(parts, part)
			a = -1
			z = -1
			dep = 0
		}
	}
	// debug
	// for i, p := range parts {
	// 	fmt.Printf("%d: %s\n", i, string(p))
	// }
	return parts, nil
}

// Value is the interface for all value kinds
// this interface along with IterValue and MapValue
// will allow you to access any combination of types
type Value interface {
	// Return wether this Value is currently NULL
	IsNull() bool
	// Return a representation of this Value as a string
	String() string
	// return a postgresql compatible string-literal bytes
	bytes() ([]byte, error)
	// Return the underlying data as an interface
	Val() interface{}
	// Values are Valuable
	driver.Valuer
	// Values are Scannable
	sql.Scanner
}

// values that have a slice of sub-values will
// impliment this interface
type IterValue interface {
	// IterValue also fulfils Value
	Value
	// Fetch all sub-values as a slice
	Values() []Value
	// Fetch a sub-value by slice index
	ValueAt(int) Value
	// Add a value to the list of sub-values
	Append(interface{}) error
}

// values that have named sub-values (like Record)
// this interface will be implimented
type MapValue interface {
	// MapValue but also fulfil Value
	Value
	// fetch all sub-values as a map
	Map() map[string]Value
	// Fetch a sub-value by name
	ValueBy(name string) Value
	// Equivilent to calling ValueBy(name).Val()
	Get(name string) interface{}
	// Equivilent to calling ValueBy(name).Scan(src)
	Set(name string, src interface{}) error
}

// RecordValuess are both MapValues and IterValues
type RecordValue interface {
	IterValue
	// fetch all sub-values as a map
	Map() map[string]Value
	// Fetch a sub-value by name
	ValueBy(name string) Value
	// Equivilent to calling ValueBy(name).Val()
	Get(name string) interface{}
	// Equivilent to calling ValueBy(name).Scan(src)
	Set(name string, src interface{}) error
	// Return the relation (if any) that this Record belongs to
	Relation() *Relation
	// Set the parent relation for this RecordValue
	SetRelation(*Relation)
}

// A `Valstructor` creates and initializes a new
// `Value`
type Valstructor func(data interface{}) (Value, error)

// a generic record or composite type constructor
func Row(ks ...Valstructor) Valstructor {
	return func(data interface{}) (v Value, err error) {
		k := new(pgRow)
		k.vs = make([]Value, len(ks))
		// create blank values for each col
		for i, vk := range ks {
			k.vs[i], err = vk(nil)
			if err != nil {
				return nil, err
			}
		}
		return k, k.Scan(data)
	}
}

type pgRow struct {
	vs    []Value
	valid bool
}

func (k *pgRow) IsNull() bool {
	return !k.valid
}

func rowScanner(src interface{}, dests []Value) error {
	switch srcs := src.(type) {
	// assert
	case nil:
		panic("Should not be possible - check nil before calling rowScanner")
	// handle case of src being a list of init vals
	case []interface{}:
		if len(dests) != len(srcs) {
			return fmt.Errorf("Number of input values does not match number of Row columns. Need %d Got: %d", len(dests), len(srcs))
		}
		for i, vx := range dests {
			err := vx.Scan(srcs[i])
			if err != nil {
				return err
			}
		}
	// normal scan ([]bytes, string, etc)
	default:
		// src -> bytes
		b, err := srcToBytes(src)
		if err != nil {
			return err
		}
		// split into parts
		parts, err := split(b)
		if err != nil {
			return err
		}
		// check col lengths match
		if len(parts) != len(dests) {
			return fmt.Errorf("Number of input columns does not match number of Row columns. Need: %d Got %d parts: %v",
				len(dests), len(parts), string(bytes.Join(parts, []byte(","))))
		}
		// parse each part
		for i, vx := range dests {
			// parse
			err = vx.Scan(parts[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (k *pgRow) Scan(src interface{}) (err error) {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	return rowScanner(src, k.vs)
}
func (k *pgRow) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.bytes()
}

func (k *pgRow) String() string {
	if !k.valid {
		return ""
	}
	s, _ := k.bytes()
	return string(s)
}

func (k *pgRow) Val() interface{} {
	if !k.valid {
		return nil
	}
	vals := make([]interface{}, len(k.vs))
	for i, v := range k.vs {
		vals[i] = v.Val()
	}
	return vals
}

func (k *pgRow) Values() []Value {
	return k.vs
}

func (k *pgRow) ValueAt(idx int) Value {
	return k.vs[idx]
}

func (k *pgRow) Append(src interface{}) error {
	return fmt.Errorf("Cannot append to Row as it already has %d (max) cols", len(k.vs))
}

func (k *pgRow) bytes() ([]byte, error) {
	return rowBytes(k.valid, k.vs)
}

// allows you name fields before passing them to
// the Record constructor
//
//    myRecordKind := Record( Col("name", Text) )
//    v := myRecordKind()
//
// Then any values created with names can be fetched
// by name from the value:
//
//    name := v.Get("name").String()
//
func Col(name string, k Valstructor) *col {
	c := new(col)
	c.k = k
	c.name = name
	return c
}

// same as Row, but takes a list of Col's
// as arguments that allow you to name the
// fields
func Record(cols ...*col) Valstructor {
	return func(data interface{}) (v Value, err error) {
		k := new(pgRecord)
		k.cs = cols
		k.vs = make([]Value, len(cols))
		// create blank values for each col
		for i, c := range cols {
			k.vs[i], err = c.k(nil)
			if err != nil {
				return nil, err
			}
		}
		return k, k.Scan(data)
	}
}

type pgRecord struct {
	vs    []Value
	cs    []*col
	valid bool
	rel   *Relation
}

func (k *pgRecord) Relation() *Relation {
	return k.rel
}

func (k *pgRecord) SetRelation(rel *Relation) {
	k.rel = rel
}

func (k *pgRecord) IsNull() bool {
	return !k.valid
}

func (k *pgRecord) Scan(src interface{}) (err error) {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	return rowScanner(src, k.vs)
}

func (k *pgRecord) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.bytes()
}

func (k *pgRecord) String() string {
	if !k.valid {
		return ""
	}
	s, _ := k.bytes()
	return string(s)
}

func (k *pgRecord) Val() interface{} {
	if !k.valid {
		return nil
	}
	vals := make([]interface{}, len(k.vs))
	for i, v := range k.vs {
		vals[i] = v.Val()
	}
	return vals
}

func (k *pgRecord) Map() map[string]Value {
	m := make(map[string]Value)
	for i, v := range k.vs {
		m[k.cs[i].name] = v
	}
	return m
}

func (k *pgRecord) Values() []Value {
	return k.vs
}

func (k *pgRecord) ValueAt(idx int) Value {
	return k.vs[idx]
}

func (k *pgRecord) ValueBy(name string) Value {
	for i, c := range k.cs {
		if c.name == name {
			return k.vs[i]
		}
	}
	return nil
}

func (k *pgRecord) Get(name string) interface{} {
	v := k.ValueBy(name)
	if v == nil {
		panic(fmt.Sprintf("No column %s", name))
	}
	return v.Val()
}

func (k *pgRecord) Set(name string, src interface{}) error {
	v := k.ValueBy(name)
	if v == nil {
		return fmt.Errorf("No column %s", name)
	}
	return v.Scan(src)
}

func (k *pgRecord) Append(src interface{}) error {
	return fmt.Errorf("Cannot append more than %d Values to record", len(k.vs))
}

func rowBytes(valid bool, vs []Value) ([]byte, error) {
	if !valid {
		return nullb, nil
	}
	b := bytes.NewBufferString("")
	b.WriteString("(")
	last := len(vs) - 1
	for i, child := range vs {
		cb, err := child.bytes()
		if err != nil {
			return nil, err
		}
		switch child.(type) {
		case *pgNumeric, *pgInteger, *pgFloat, *pgBool:
			b.Write(cb)
		default:
			b.WriteString(`"`)
			b.Write(escape(cb, 2))
			b.WriteString(`"`)
		}
		if i != last {
			b.WriteString(",")
		}
	}
	b.WriteString(")")
	return b.Bytes(), nil
}

func (k *pgRecord) bytes() ([]byte, error) {
	return rowBytes(k.valid, k.vs)
}

// return a Query for a referenced table
func (k *pgRecord) From(refname string) {

}

func Array(el Valstructor) Valstructor {
	return func(data interface{}) (v Value, err error) {
		k := new(pgArray)
		k.el = el
		return k, k.Scan(data)
	}
}

type pgArray struct {
	vs    []Value
	el    Valstructor
	valid bool
}

func (k *pgArray) Scan(src interface{}) (err error) {
	// reset
	k.vs = make([]Value, 0)
	// check null
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	// check supported scan types
	switch x := src.(type) {
	case []interface{}:
		for _, d := range x {
			err = k.Append(d)
			if err != nil {
				return err
			}
		}
		k.valid = true
	default:
		k.valid = true
		// src -> string
		b, err := srcToBytes(src)
		if err != nil {
			return err
		}
		// split on ','
		parts, err := split(b)
		if err != nil {
			return err
		}
		// add vals
		for _, part := range parts {
			err = k.Append(part)
			if err != nil {
				return err
			}
		}
	}
	return
}

func (k *pgArray) IsNull() bool {
	return !k.valid
}

func (k *pgArray) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.bytes()
}

func (k *pgArray) bytes() ([]byte, error) {
	if !k.valid {
		return nullb, nil
	}
	b := bytes.NewBufferString("")
	b.WriteString("{")
	last := len(k.vs) - 1
	for i, child := range k.vs {
		cb, err := child.bytes()
		if err != nil {
			return nil, err
		}
		switch child.(type) {
		case *pgNumeric, *pgInteger, *pgFloat, *pgBool, *pgArray, *pgTimestamp:
			b.Write(cb)
		default:
			b.WriteString(`"`)
			b.Write(escape(cb, 1))
			b.WriteString(`"`)
		}
		if i != last {
			b.WriteString(",")
		}
	}
	b.WriteString("}")
	return b.Bytes(), nil
}

func (k *pgArray) String() string {
	if !k.valid {
		return ""
	}
	s, _ := k.bytes()
	return string(s)
}

func (k *pgArray) Val() interface{} {
	if !k.valid {
		return nil
	}
	vals := make([]interface{}, len(k.vs))
	for i, v := range k.vs {
		vals[i] = v.Val()
	}
	return vals
}

func (k *pgArray) Values() []Value {
	return k.vs
}

func (k *pgArray) ValueAt(idx int) Value {
	return k.vs[idx]
}

func (k *pgArray) Append(src interface{}) error {
	switch v := src.(type) {
	case Value:
		// TODO: check v is type t
		k.vs = append(k.vs, v)
	default:
		vx, err := k.el(nil)
		if err != nil {
			return err
		}
		err = vx.Scan(src)
		if err != nil {
			return err
		}
		k.vs = append(k.vs, vx)
	}
	k.valid = true // ensure not null
	return nil
}

func SmallInt(data interface{}) (Value, error) {
	return newInt(16, data)
}

func Integer(data interface{}) (Value, error) {
	return newInt(32, data)
}

func BigInt(data interface{}) (Value, error) {
	return newInt(64, data)
}

func newInt(bs int, data interface{}) (Value, error) {
	k := &pgInteger{0, bs, false}
	return k, k.Scan(data)
}

type pgInteger struct {
	n     int64
	bs    int
	valid bool
}

func (k *pgInteger) Scan(src interface{}) (err error) {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	switch x := src.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		k.n, err = fitInt(src, k.bs)
		if err != nil {
			return err
		}
	case []byte:
		k.n, err = strconv.ParseInt(string(x), 10, k.bs)
		if err != nil {
			return err
		}
	case string:
		k.n, err = strconv.ParseInt(x, 10, k.bs)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cannot set %dbit Integer Value with %T -> %v", k.bs, src, src)
	}
	return nil
}

func (k *pgInteger) IsNull() bool {
	return !k.valid
}

func (k *pgInteger) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.n, nil
}

func (k *pgInteger) bytes() ([]byte, error) {
	if !k.valid {
		return nullb, nil
	}
	return []byte(fmt.Sprintf("%d", k.n)), nil
}

func (k *pgInteger) String() string {
	if !k.valid {
		return ""
	}
	return fmt.Sprintf("%d", k.n)
}

func (k *pgInteger) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.n
}

// stored as string currently
// TODO: use some Value of arbitary precision for this
func Numeric(prec int, scale int) Valstructor {
	return func(data interface{}) (Value, error) {
		k := &pgNumeric{"", prec, scale, false}
		return k, k.Scan(data)
	}
}

type pgNumeric struct {
	s     string
	prec  int
	scale int
	valid bool
}

func (k *pgNumeric) Scan(src interface{}) (err error) {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	switch x := src.(type) {
	case float32:
		k.s = strconv.FormatFloat(float64(x), 'f', k.scale, 64)
	case float64:
		k.s = strconv.FormatFloat(x, 'f', k.scale, 64)
	case string:
		k.s = x
	case []byte:
		k.s = string(x)
	default:
		return fmt.Errorf("cannot set Numeric(%d,%d) Value with %T -> %v", k.prec, k.scale, src, src)
	}
	return nil
}
func (k *pgNumeric) IsNull() bool {
	return !k.valid
}

func (k *pgNumeric) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.s, nil
}

func (k *pgNumeric) bytes() ([]byte, error) {
	if !k.valid {
		return nullb, nil
	}
	return []byte(k.s), nil
}

func (k *pgNumeric) String() string {
	if !k.valid {
		return ""
	}
	return k.s
}

func (k *pgNumeric) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.s
}

// Text field with limited values
func Enum(labels ...string) Valstructor {
	if len(labels) == 0 {
		panic("Cannot create Enum Value with no labels")
	}
	return func(data interface{}) (Value, error) {
		k := &pgEnum{"", labels, false}
		return k, k.Scan(data)
	}
}

type pgEnum struct {
	s     string
	ls    []string
	valid bool
}

func (k *pgEnum) Scan(src interface{}) error {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	var s string
	switch x := src.(type) {
	case string:
		k.s = x
	case []byte:
		k.s = string(x)
	default:
		return fmt.Errorf("cannot set Enum Value with %T -> %v", src, src)
	}
	// check it's valid
	var ok bool
	for _, s2 := range k.ls {
		if k.s == s2 {
			ok = true
		}
	}
	if !ok {
		return fmt.Errorf("Value should be one of %s got %s", strings.Join(k.ls, ","), s)
	}
	return nil
}
func (k *pgEnum) IsNull() bool {
	return !k.valid
}

func (k *pgEnum) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.s, nil
}

func (k *pgEnum) bytes() ([]byte, error) {
	if !k.valid {
		return nullb, nil
	}
	return []byte(k.s), nil
}

func (k *pgEnum) String() string {
	if !k.valid {
		return ""
	}
	return k.s
}

func (k *pgEnum) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.s
}

// float32

func newfloat(bs int, data interface{}) (Value, error) {
	k := &pgFloat{0, bs, false}
	return k, k.Scan(data)
}

func Real(data interface{}) (Value, error) {
	return newfloat(32, data)
}

func Double(data interface{}) (Value, error) {
	return newfloat(64, data)
}

type pgFloat struct {
	n     float64
	bs    int
	valid bool
}

func (k *pgFloat) Scan(src interface{}) (err error) {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	switch x := src.(type) {
	case float64:
		if k.bs == 32 && x > math.MaxFloat32 {
			return fmt.Errorf("cannot fit float64 %f into REAL Value", x)
		}
		k.n = x
	case float32:
		k.n = float64(x)
	case string:
		k.n, err = strconv.ParseFloat(x, k.bs)
		if err != nil {
			return err
		}
	case []byte:
		k.n, err = strconv.ParseFloat(string(x), k.bs)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cannot set %dbit Float Value with %T -> %v", k.bs, src, src)
	}
	return nil
}

func (k *pgFloat) IsNull() bool {
	return !k.valid
}

func (k *pgFloat) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.n, nil
}

func (k *pgFloat) bytes() ([]byte, error) {
	if !k.valid {
		return nullb, nil
	}
	return []byte(k.String()), nil
}

func (k *pgFloat) String() string {
	if !k.valid {
		return ""
	}
	return strconv.FormatFloat(k.n, 'f', -1, 32)
}

func (k *pgFloat) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.n
}

func Bool(data interface{}) (Value, error) {
	k := &pgBool{false, false}
	return k, k.Scan(data)
}

type pgBool struct {
	b     bool
	valid bool
}

func (k *pgBool) Scan(src interface{}) (err error) {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	k.b = false
	switch x := src.(type) {
	case string:
		if x[0] == 't' || x[0] == '1' {
			k.b = true
		}
	case int:
		if x == 1 {
			k.b = true
		}
	case []byte:
		if x[0] == 't' || x[0] == '1' {
			k.b = true
		}
	case bool:
		k.b = x
	default:
		return fmt.Errorf("cannot set Boolean Value with %T -> %v", src, src)
	}
	return nil
}

func (k *pgBool) IsNull() bool {
	return !k.valid
}

func (k *pgBool) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.b, nil
}

func (k *pgBool) bytes() ([]byte, error) {
	if !k.valid {
		return nullb, nil
	}
	return []byte(k.String()), nil
}

func (k *pgBool) String() string {
	if !k.valid {
		return ""
	}
	if k.b {
		return "t"
	}
	return "f"
}

func (k *pgBool) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.b
}

func Text(data interface{}) (Value, error) {
	k := &pgText{"", 0, false, false}
	return k, k.Scan(data)
}

func VarChar(n int) Valstructor {
	return func(data interface{}) (Value, error) {
		k := &pgText{"", n, false, false}
		err := k.Scan(data)
		if err != nil {
			return nil, err
		}
		return k, nil
	}
}

func Char(n int) Valstructor {
	return func(data interface{}) (Value, error) {
		k := &pgText{"", n, true, false}
		err := k.Scan(data)
		if err != nil {
			return nil, err
		}
		return k, nil
	}
}

type pgText struct {
	s     string // data
	n     int    // limit to n chars
	p     bool   // padding
	valid bool
}

func (k *pgText) Scan(src interface{}) error {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	switch x := src.(type) {
	case string:
		k.s = x
	case []byte:
		k.s = string(x)
	default:
		return fmt.Errorf("cannot set string Value with %T -> %v", src, src)
	}
	if k.p {
		// trim - weird bit of SQL standard where space is ignored
		k.s = strings.TrimRight(k.s, " ")
		// error if does not fit
		if len(k.s) > k.n {
			return fmt.Errorf("cannot fit %s into Char(%d) Value", k.s, k.n)
		}
		// space-pad string to fit n
		if len(k.s) < k.n {
			k.s = fmt.Sprintf("%-"+strconv.Itoa(k.n)+"s", k.s)
		}
	} else if k.n > 0 && len(k.s) > k.n {
		// silently truncate value as per SQL standard
		k.s = k.s[0:k.n]
	}
	return nil
}

func (k *pgText) IsNull() bool {
	return !k.valid
}

func (k *pgText) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.s, nil
}

func (k *pgText) bytes() ([]byte, error) {
	if !k.valid {
		return nullb, nil
	}
	return []byte(k.s), nil
}

func (k *pgText) String() string {
	if !k.valid {
		return ""
	}
	return k.s
}

func (k *pgText) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.s
}

func Bytea(data interface{}) (Value, error) {
	k := new(pgBytea)
	return k, k.Scan(data)
}

type pgBytea struct {
	b     []byte
	valid bool
}

func (k *pgBytea) Scan(src interface{}) (err error) {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	switch s := src.(type) {
	case []byte:
		k.b = s
	default:
		return fmt.Errorf("cannot set BYTEA value with %T -> %v", src, src)
	}
	return nil
}

func (k *pgBytea) IsNull() bool {
	return !k.valid
}

func (k *pgBytea) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.b, nil
}

func (k *pgBytea) bytes() ([]byte, error) {
	if !k.valid {
		return nullb, nil
	}
	return []byte(fmt.Sprintf("\\x%x", k.b)), nil
}

func (k *pgBytea) String() string {
	if !k.valid {
		return ""
	}
	return string(k.b)
}

func (k *pgBytea) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.b
}

func HStore(data interface{}) (Value, error) {
	k := &pgHStore{}
	return k, k.Scan(data)
}

type pgHStore struct {
	m     map[string]Value
	valid bool
}

func (k *pgHStore) Scan(src interface{}) (err error) {
	// reset
	k.m = make(map[string]Value)
	// check null
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	// get src into a valid type
	var keyvals map[string]string
	switch s := src.(type) {
	case []byte:
		// do the parsing
		keyvals, err = parseHStore(s)
		if err != nil {
			return err
		}
	case map[string]string:
		keyvals = s
	default:
		return fmt.Errorf("cannot set HSTORE value with %T -> %v", src, src)
	}
	for key, val := range keyvals {
		vx, err := Text(val)
		if err != nil {
			return err
		}
		k.m[key] = vx
	}
	return nil
}

func (k *pgHStore) IsNull() bool {
	return !k.valid
}

func (k *pgHStore) Value() (driver.Value, error) {
	if !k.valid {
		return nullb, nil
	}
	return k.bytes()
}

func (k *pgHStore) String() string {
	if !k.valid {
		return ""
	}
	s, _ := k.bytes()
	return string(s)
}

// return all hstore values
func (k *pgHStore) Map() map[string]Value {
	return k.m
}

func (k *pgHStore) ValueBy(name string) Value {
	if v, ok := k.m[name]; ok {
		return v
	}
	return nil
}

func (k *pgHStore) Get(name string) interface{} {
	return k.ValueBy(name).Val()
}

func (k *pgHStore) Set(name string, src interface{}) error {
	return k.ValueBy(name).Scan(src)
}

func (k *pgHStore) Val() interface{} {
	if !k.valid {
		return nil
	}
	vals := make(map[string]string)
	for key, v := range k.m {
		vals[key] = v.Val().(string)
	}
	return vals
}

// TODO: this was just a quick test.. does not quote fields!
func (k *pgHStore) bytes() ([]byte, error) {
	buf := make([][]byte, len(k.m))
	i := 0
	for key, val := range k.m {
		buf[i] = []byte(fmt.Sprintf(`"%s" => "%s"`, key, val))
		i++
	}
	return bytes.Join(buf, []byte(`,`)), nil
}

func Timestamp(data interface{}) (Value, error) {
	k := new(pgTimestamp)
	return k, k.Scan(data)
}

type pgTimestamp struct {
	t     time.Time
	tz    string
	valid bool
}

var timeFormats = []string{
	"2006-01-02 15:04:05-07",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"15:04:05-07",
	"15:04:05",
	"2006-01-02",
}

func parseTime(s string, t *time.Time) (err error) {
	// Special case until time.Parse bug is fixed:
	// http://code.google.com/p/go/issues/detail?id=3487
	if s[len(s)-2] == '.' {
		s += "0"
	}
	// check timestampz for a 30-minute-offset timezone
	// s[len(s)-3] == ':' {
	// f += ":00"

	// try to parse each format til will find one
	for _, f := range timeFormats {
		*t, err = time.Parse(f, s)
		if err == nil {
			break
		} else {
			err = fmt.Errorf("could not parse time string %s", s)
		}
	}
	return err
}

func (k *pgTimestamp) Scan(src interface{}) error {
	if src == nil {
		k.valid = false
		return nil
	}
	k.valid = true
	switch x := src.(type) {
	case time.Time:
		k.t = x
	case string:
		return parseTime(x, &k.t)
	case []byte:
		return parseTime(string(x), &k.t)
	default:
		return fmt.Errorf("cannot set TIMESTAMP value with %T -> %v", src, src)
	}
	return nil
}

func (k *pgTimestamp) IsNull() bool {
	return !k.valid
}

func (k *pgTimestamp) Value() (driver.Value, error) {
	if !k.valid {
		return nil, nil
	}
	return k.t, nil
}

func (k *pgTimestamp) bytes() ([]byte, error) {
	if !k.valid {
		return nullb, nil
	}
	return []byte(k.t.Format(time.RFC3339Nano)), nil
}

func (k *pgTimestamp) String() string {
	if !k.valid {
		return ""
	}
	return k.t.Format(time.RFC3339Nano)
}

func (k *pgTimestamp) Val() interface{} {
	if !k.valid {
		return nil
	}
	return k.t
}

// Value aliases
var (
	Decimal   = Numeric
	Int       = Integer
	Int2      = SmallInt
	Int4      = Integer
	Int8      = BigInt
	Serial    = Integer
	BigSerial = BigInt
)
