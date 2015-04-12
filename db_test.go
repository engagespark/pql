package pqutil

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/lib/pq"
)

var pdb *DB

type dbcase struct {
	sqltype string
}

type tc struct {
	typ string      // PSQL type
	val interface{} // data to init Value
	s   string      // expected string returned
}

// list of col types to create, init vals and expected string representations
var testcols = [...]*tc{
	&tc{`serial primary key`, nil, ""},
	&tc{`char(1)`, "A", "A"},
	&tc{`varchar(5)`, "ABCDE", "ABCDE"},
	&tc{`text`, "multi\nline\ntext", "multi\nline\ntext"},
	&tc{`smallint`, 9, "9"},
	&tc{`integer`, 99, "99"},
	&tc{`bigint`, 999, "999"},
	&tc{`real`, 3.14, "3.14"},
	&tc{`double precision`, 3.1459, "3.1459"},
	&tc{`numeric(15,3)`, 0.120, "0.120"},
	&tc{`bytea`, []byte("xyz"), "xyz"},
	&tc{`timestamp`, "2011-01-01 23:01", "2011-01-01T23:01:00Z"},
	&tc{`timestamptz`, "2011-01-01 23:02:00", "2011-01-01T23:02:00Z"},
	&tc{`boolean`, true, "t"},
	&tc{`gender`, "male", "male"},
	&tc{`hstore`,
		[]byte(`"k1" => "v1", "k2" => "v2"`),
		`"k1"=>"v1","k2"=>"v2"`},
	&tc{`char(1)[]`,
		[]interface{}{"a", "b"},
		`{"a","b"}`},
	&tc{`varchar(5)[]`,
		[]interface{}{"abc", "def"},
		`{"abc","def"}`},
	&tc{`text[]`,
		[]interface{}{"xxx", "yyy"},
		`{"xxx","yyy"}`},
	&tc{`smallint[]`,
		[]interface{}{1, 2},
		`{1,2}`},
	&tc{`integer[]`,
		[]interface{}{100, 200},
		`{100,200}`},
	&tc{`bigint[]`,
		[]interface{}{1000, 2000},
		`{1000,2000}`},
	&tc{`real[]`,
		[]interface{}{1.1, 2.2},
		`{1.1,2.2}`},
	&tc{`double precision[]`,
		[]interface{}{1.11, 2.22},
		`{1.11,2.22}`},
	&tc{`numeric(15,3)[]`,
		[]interface{}{0.123, 1.123},
		`{0.123,1.123}`},
	&tc{`bytea[]`,
		[]interface{}{[]byte("x"), []byte("y")},
		`{"\\x78","\\x79"}`},
	&tc{`timestamp[]`,
		[]interface{}{"2009-01-01", "2010-01-01"},
		`{2009-01-01T00:00:00Z,2010-01-01T00:00:00Z}`},
	&tc{`timestamptz[]`,
		[]interface{}{"2011-01-01", "2012-01-01"},
		`{2011-01-01T00:00:00Z,2012-01-01T00:00:00Z}`},
	&tc{`boolean[]`,
		[]interface{}{true, false},
		`{t,f}`},
	&tc{`gender[]`,
		[]interface{}{"female", "male"},
		`{"female","male"}`},
	&tc{`hstore[]`,
		[]interface{}{[]byte(`"k1" => "v1"`), []byte(`"kx" => "vx"`)},
		`{"\"k1\" => \"v1\"","\"kx\" => \"vx\""}`},
	&tc{`thing`,
		[]interface{}{
			[]interface{}{8, 8, 8},
			[]interface{}{`"`, `\"`, `\\}`},
			[]interface{}{"2011-01-01", "2012-01-01"},
		},
		`("{8,8,8}","{""\\"""",""\\\\\\"""",""\\\\\\\\}""}","{2011-01-01T00:00:00Z,2012-01-01T00:00:00Z}")`,
	},
}

var setup = []string{
	// reset
	`DROP SCHEMA public CASCADE`,
	`CREATE SCHEMA public`,
	`CREATE EXTENSION hstore`,
	// create an ENUM
	`CREATE TYPE gender AS ENUM (
		'male', 'female'
	)`,
	// create a composite type
	`CREATE TYPE thing AS (
		t0 integer[],
		t1 text[],
		t2 timestamptz[]
	)`,
	`CREATE TABLE location (
		id serial primary key,
		name text
	)`,
	`CREATE TABLE person (
		id serial primary key,
		name text,
		age integer,
		location_id integer REFERENCES location
	)`,
	`INSERT INTO location VALUES (100,'g1')`,
	`INSERT INTO location VALUES (200,'g2')`,
	`INSERT INTO person VALUES (1,'bob',19, 100)`,
	`INSERT INTO person VALUES (2,'jeff',20, 100)`,
	`INSERT INTO person VALUES (3,'alice',17, 200)`,
}

func open(t *testing.T) *DB {
	if pdb == nil {
		var err error
		pdb, err = Open("dbname=pql_test sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range setup {
			_, err = pdb.DB.Exec(q)
			if err != nil {
				switch e := err.(type) {
				case pq.PGError: // just print PG errors from db setup
					fmt.Println("psql", e.Get('M'))
				default:
					t.Fatal(err)
				}
			}
		}
		// make the table
		cols := make([]string, len(testcols))
		for i, tc := range testcols {
			cols[i] = fmt.Sprintf("c%d %s", i, tc.typ)
		}
		q := fmt.Sprintf(`CREATE TABLE test (%s)`, strings.Join(cols, ","))
		_, err = pdb.DB.Exec(q)
		if err != nil {
			switch e := err.(type) {
			case pq.PGError: // just print PG errors from db setup
				t.Fatal("psql:", e.Get('M'))
			default:
				t.Fatal(err)
			}
		}
	}
	return pdb
}

func TestColumnNames(t *testing.T) {
	db := open(t)
	r, err := db.Relation("test")
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range r.Cols() {
		cn := fmt.Sprintf("c%d", i)
		if c.name != cn {
			t.Errorf(`expected col #0 to be %s got: %s`, cn, c.name)
		}
	}
}

func getEq(t *testing.T, v MapValue, col string, match string, msg string) {
	vx := v.ValueBy(col)
	if vx == nil {
		t.Fatalf("%s: could not find value with name %s", msg, col)
	}
	switch vxx := vx.(type) {
	case *pgHStore:
		vk := vxx.Get("k1")
		if vk == nil {
			t.Errorf(`expected to find an hstore key called "k1"`)
		} else {
			if vk.(string) != "v1" {
				t.Errorf("expected k1 => v1")
			}
		}
	default:
		s := vx.String()
		if s != match {
			t.Errorf("%s: expected %s to be %s got: %v", msg, col, match, s)
		}
	}
}

func chkRecord(t *testing.T, v MapValue, msg string) {
	for i, tc := range testcols {
		getEq(t, v, fmt.Sprintf("c%d", i), tc.s, msg)
	}
}

func TestInsert(t *testing.T) {
	db := open(t)
	// build mega arg from testcols
	args := make([]interface{}, 0)
	for _, tc := range testcols {
		args = append(args, tc.val)
	}
	// create a new (unsaved) record
	v, err := db.New("test", args)
	if err != nil {
		t.Fatal(err)
	}
	// check state
	chkRecord(t, v, "before INSERT")
	// try insert
	err = db.Insert(v)
	if err != nil {
		t.Fatal(err)
	}
	// check pk got set
	pk := v.Get("c0").(int64)
	if pk == 0 {
		t.Errorf("expected pk (col #0) to be set got: %v", pk)
	}
	testcols[0].s = strconv.FormatInt(pk, 10)
	// check again
	chkRecord(t, v, "after INSERT")
	// read the record from the db by pk
	v, err = db.From("test").Get(pk)
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Fatalf("could not Get(%d) record", pk)
	}
	// check again
	chkRecord(t, v, "after SELECT")
	// update something
	testcols[1].s = "B"
	err = v.Set("c1", testcols[1].s)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(v)
	if err != nil {
		t.Fatal(err)
	}
	// read it back again
	v2, err := db.From("test").Get(pk)
	if err != nil {
		t.Fatal(err)
	}
	// check again
	chkRecord(t, v2, "after UPDATE")
}

func TestRelations(t *testing.T) {
	db := open(t)
	rels, err := db.Relations()
	if err != nil {
		t.Fatal(err)
	}
	cnt := 0
	for _, rel := range rels {
		switch rel.Name {
		case "test", "thing", "person", "location":
			cnt++
		default:
			t.Fatal("unexpected relation %s", rel.Name)
		}
	}
	if cnt != 4 {
		t.Errorf("expected to find 2 relations got: %d", cnt)
	}
}

func TestFetchRecord(t *testing.T) {
	db := open(t)
	v, err := db.From("person").
		Where("age = $1 AND age = $1", 17).
		And("name = $1", "alice").
		FetchOne()
	if err != nil {
		t.Fatal(err)
	} else if v == nil {
		t.Error("no record found")
	}

}
func TestFetchRecords(t *testing.T) {
	db := open(t)
	vs, err := db.From("person").Where("name = $1", "bob").And("age = $1", 19).Fetch()
	if err != nil {
		t.Error(err)
	} else if len(vs) == 0 {
		t.Error("no records found")
	}
}

func TestFetchRecordsInTransaction(t *testing.T) {
	db := open(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	vs, err := tx.From("person").Where("age = $1", 17).And("name = $1", "alice").Fetch()
	if err != nil {
		t.Error(err)
	} else if len(vs) == 0 {
		t.Error("no records found")
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
}

func TestUpdateSave(t *testing.T) {
	db := open(t)
	//
	people := db.From("person")
	// store count
	originalCount, err := people.Count()
	if err != nil {
		t.Fatal(err)
	}
	// get by pk
	v, err := people.Get(1)
	if err != nil {
		t.Fatal(err)
	} else if v == nil {
		t.Fatal("no record found")
	}
	// change it
	err = v.Set("age", 20)
	if err != nil {
		t.Fatal(err)
	}
	// save it
	err = db.Upsert(v)
	if err != nil {
		t.Error(err)
	}
	// check it
	v, err = people.Get(1)
	if err != nil {
		t.Error(err)
	} else if v == nil {
		t.Error("no record found")
	} else if v.Get("age").(int64) != 20 {
		t.Errorf("expected age to be 20 got: %v", v.Get("age"))
	}
	// check count didn't change
	afterCount, err := people.Count()
	if err != nil {
		t.Error(err)
	}
	if afterCount != originalCount {
		t.Errorf("expected count to be %d got: %v", originalCount, afterCount)
	}
}

func TestHasOneReference(t *testing.T) {
	db := open(t)
	// get by pk
	person, err := db.From("person").Get(1)
	if err != nil {
		t.Fatal(err)
	} else if person == nil {
		t.Fatal("no record found")
	}
	// get locations that a person belongs to
	rs, err := db.From("location").For(person).Fetch()
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	for _, v := range rs {
		id := v.Get("id").(int64)
		switch id {
		case 100:
			i++
		default:
			t.Fatal("unexpected location id %d for person 1", id)
		}
	}
	if i != 1 {
		t.Fatal("expected 1 location that belongs to person 1")
	}
}

func TestHasManyReference(t *testing.T) {
	db := open(t)
	// get by pk
	location, err := db.From("location").Get(100)
	if err != nil {
		t.Fatal(err)
	} else if location == nil {
		t.Fatal("no record found")
	}
	// people who belongs to a location
	rs, err := db.From("person").For(location).Fetch()
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	for _, v := range rs {
		id := v.Get("id").(int64)
		switch id {
		case 1, 2:
			i++
		default:
			t.Fatal("unexpected person id %d for location 1", id)
		}
	}
	if i != 2 {
		t.Fatal("expected 2 person records that belong to location 1")
	}
}

func TestQueryMin(t *testing.T) {
	db := open(t)
	v, err := db.From("person").Min("age")
	if err != nil {
		t.Fatal(err)
	}
	if v.Val().(int64) != 17 {
		t.Fatalf("expected min age to be 17 got: %v", v.Val())
	}
}

func TestQueryMax(t *testing.T) {
	db := open(t)
	v, err := db.From("person").Max("age")
	if err != nil {
		t.Fatal(err)
	}
	if v.Val().(int64) != 20 {
		t.Fatalf("expected max age to be 20 got: %v", v.Val())
	}
}

func TestQueryAvg(t *testing.T) {
	db := open(t)
	v, err := db.From("person").Avg("age")
	if err != nil {
		t.Fatal(err)
	}
	if v.Val().(float64) != 19 {
		t.Fatalf("expected avg age to be 19 got: %v", v.Val())
	}
}

func TestQuerySum(t *testing.T) {
	db := open(t)
	v, err := db.From("person").Sum("age")
	if err != nil {
		t.Fatal(err)
	}
	if v.Val().(int64) != 57 {
		t.Fatalf("expected sum age to be 57 got: %v", v.Val())
	}
}
