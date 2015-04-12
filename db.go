package pqutil

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	// SQL to list relations with oid
	selectRelsSql = `
		SELECT
			pgc.oid,
			pgc.relname
		FROM pg_class pgc, pg_namespace pgn
		WHERE pgc.relnamespace = pgn.oid
		AND pg_table_is_visible(pgc.oid)
		AND pgc.relkind IN ('r','v','c')
		AND pgc.relpersistence != 't'
		AND pgn.nspname = 'public'
	`
	// SQL to fetch col info for a relation
	// along with foreign key data notnull, primary key info
	selectColsSql = `
		SELECT DISTINCT
			a.attnum as num,
			a.attname as name,
			COALESCE(
				format_type(a.atttypid, a.atttypmod),
				''
			) as typ,
			a.atttypid as toid,
			a.attnotnull as notnull,
			COALESCE(i.indisprimary,false) as pk,
			COALESCE(fks.fktable, ''),
			COALESCE(fks.fkfield, ''),
			COALESCE(regexp_replace(
				regexp_replace(
					format_type(a.atttypid, a.atttypmod),
					E'^(.*?\\(|[^\\(]+$)',
					''
				),
				E'\\).*',
				''
			),'') as args
		FROM pg_attribute a JOIN pg_class pgc ON pgc.oid = a.attrelid
		LEFT JOIN pg_index i ON pgc.oid = i.indrelid AND i.indkey[0] = a.attnum
		LEFT JOIN (
			select
				att2.attname as name,
				cl.relname as fktable,
				att.attname as fkfield,
				con.relname as relname
			from
				(select
					unnest(con1.conkey) as "parent",
					unnest(con1.confkey) as "child",
					con1.confrelid,
					con1.conrelid,
					cl.relname as relname
				from
					pg_class cl
					join pg_namespace ns on cl.relnamespace = ns.oid
					join pg_constraint con1 on con1.conrelid = cl.oid
				where
					con1.contype = 'f'
				) con
			join pg_attribute att on
				att.attrelid = con.confrelid and att.attnum = con.child
			join pg_class cl on
				cl.oid = con.confrelid
			join pg_attribute att2 on
				att2.attrelid = con.conrelid and att2.attnum = con.parent
		) fks ON fks.name = a.attname AND fks.relname = pgc.relname
		WHERE a.attnum > 0 AND pgc.oid = a.attrelid
		AND pgc.oid = $1
		AND pg_table_is_visible(pgc.oid)
		AND NOT a.attisdropped
		ORDER BY a.attnum
	`
	// SQL to list pg_type info
	selectTypeSql = `
		SELECT
			typname,
			typtype,
			typdelim,
			typrelid,
			typelem,
			typarray,
			typnotnull,
			typbasetype,
			typtypmod,
			typndims
		FROM pg_type
		WHERE oid = $1
		AND typisdefined = true
	`

	// SQL to fetch list of enum labels
	selectEnumSql = `
		SELECT enumlabel
		FROM pg_enum
		WHERE enumtypid = $1
		ORDER BY enumsortorder
	`
)

type queryer interface {
	Query(string, ...interface{}) (*Rows, error)
	Relations() (map[string]*Relation, error)
}

type col struct {
	k       Valstructor // the Value kind
	typ     string      // the pg_type name for casting
	oid     uint32      // the pg_type oid (if available)
	name    string      // name of this col
	reft    string      // name of referenced relation (if any)
	reff    string      // name of field in referenced relation (if any)
	pk      bool        // is col a primary key
	notNull bool        // is col marked as notNull
}

type refkind uint

const (
	r_hasOne = iota
	r_hasMany
)

// struct to hold foreign reference info on *Relation
type ref struct {
	name string    // relationship name
	kind refkind   //relationship type
	rel  *Relation // relation
	col  *col      // column with foreign key details
}

// Relation holds column and reference info about a relation.
// Usually inferred from the database. See Relation methods on DB
type Relation struct {
	Name string
	k    Valstructor
	cols []*col
	refs []*ref
}

// return a new RecordValue that represents a row
// from this relation
func (r *Relation) New(data interface{}) (RecordValue, error) {
	v, err := r.k(data)
	if err != nil {
		return nil, err
	}
	k := v.(RecordValue)
	k.SetRelation(r)
	return k, nil
}

// csv list of column names for this relation.
// If pk is false then the primary key will not appear in the list.
func (r *Relation) fields(pk bool) string {
	if r.cols == nil {
		panic("Cols not defined?")
	}
	n := len(r.cols)
	if !pk {
		n--
	}
	cols := make([]string, n)
	i := 0
	for _, c := range r.cols {
		if c.pk && !pk {
			continue
		}
		cols[i] = c.name
		i++
	}
	return strings.Join(cols, ",")
}

// Return csv list of $1,$2 etc bindings suitable for use with fields(),
// and an int representing the largest $X value in the returned list.
// If pk is false then it will not appear in the list.
// If set is true then the list will be field = $1,field = $2 etc.
func (r *Relation) bindings(pk bool, set bool) (string, int) {
	n := len(r.cols)
	if !pk {
		n--
	}
	ss := make([]string, n)
	i := 0
	for _, c := range r.cols {
		if c.pk && !pk {
			continue
		}
		bnd := fmt.Sprintf("$%d", i+1)
		if c.typ != "" {
			bnd = fmt.Sprintf("cast(%s as %s)\n", bnd, c.typ)
		}
		if set {
			bnd = fmt.Sprintf("%s = %s", c.name, bnd)
		}
		ss[i] = bnd
		i++
	}
	return strings.Join(ss, ","), i
}

// return the primary key col or nil if none
func (r *Relation) pk() *col {
	if r.cols == nil {
		return nil
	}
	for _, c := range r.cols {
		if c.pk {
			return c
		}
	}
	return nil
}

// Return a slice of all values from v.
// If update is true then move pk to end of list.
// If update is false then skip pk.
func (r *Relation) valArgs(v RecordValue, update bool) []interface{} {
	n := len(r.cols)
	if !update {
		n--
	}
	infs := make([]interface{}, n)
	i := 0
	var pk *col
	for _, c := range r.cols {
		if c.pk {
			pk = c
			continue
		}
		infs[i] = v.ValueBy(c.name)
		i++
	}
	if update {
		infs[i] = v.ValueBy(pk.name)
		i++
	}
	return infs
}

// return list of column data in the order postgresql expects them
func (r *Relation) Cols() []*col {
	return r.cols
}

// wrapper type around sql.Rows
// adds the ScanRecord method to make it easier to Scan Row Values
type Rows struct {
	*sql.Rows
}

// Similar to sql.Rows#Scan but scans all values into a RecordValue
func (rs *Rows) ScanRecord(v RecordValue) error {
	// get list of vals as interface
	vals := make([]interface{}, len(v.Values()))
	for i, v := range v.Values() {
		vals[i] = v
	}
	err := rs.Scan(vals...)
	if err != nil {
		return err
	}
	return nil
}

// the Query type is used to build simple/common queries
// most methods return a new Query so they can be chained
// with any errors being defered until a call that causes a db.Query
type Query struct {
	tx          queryer
	from        *Relation
	where       []string
	whereParams []interface{}
	order       string
	limit       int
	offset      int
	err         error // some errors are defered until a call the Fetch(), Update() etc
}

func (q *Query) cp() *Query {
	// if we are defering an error
	// might as well not bother with the copy
	if q.err != nil {
		panic("cp should not be called when there is a pending error")
	}
	return &Query{
		q.tx,
		q.from,
		q.where,
		q.whereParams,
		q.order,
		q.limit,
		q.offset,
		q.err,
	}
}

// Return a new Query based on this query with an additional
// (WHERE) filter.
func (q *Query) Where(w string, params ...interface{}) *Query {
	if q.err != nil {
		return q
	}
	q2 := q.cp()
	q2.where = append(q2.where, w)
	q2.whereParams = append(q2.whereParams, params...)
	return q2
}

// syntantic sugar alias for Where
func (q *Query) And(w string, params ...interface{}) *Query {
	return q.Where(w, params...)
}

// Return a new Query with
func (q *Query) For(v RecordValue) *Query {
	if q.err != nil {
		return q
	}
	q2 := q.cp()
	// get the pk for v
	vrel := v.Relation()
	if vrel == nil {
		q2.err = fmt.Errorf("RecordValue given to For() does not belong to a relation")
		return q2
	}
	// check for a ref on this query's rel to use (has one)
	// select * from x where id = v.fk
	if ref := q.refFor(r_hasOne, q.from, vrel); ref != nil {
		fkv := v.ValueBy(ref.col.name)
		if fkv == nil {
			q2.err = fmt.Errorf("No column %s for %s", ref.col.name, vrel.Name)
			return q2
		}
		if fkv.IsNull() {
			q2.err = fmt.Errorf("RecordValue for %s has a NULL foreign key", vrel.Name)
			return q2
		}
		pk := q.from.pk()
		if pk == nil {
			q2.err = fmt.Errorf("%s must have a primary key to use in For query",
				q.from.Name)
			return q2
		}
		return q2.Where(fmt.Sprintf(`%s = $1`, pk.name), fkv)
	}
	// check for a ref on v that can be used (has many)
	// select * from x where fk = v.id
	if ref := q.refFor(r_hasMany, q.from, vrel); ref != nil {
		pk := vrel.pk()
		if pk == nil {
			q2.err = fmt.Errorf("RecordValue for %s must have a primary key to use in For query",
				vrel.Name)
			return q2
		}
		pkv := v.ValueBy(pk.name)
		if pkv.IsNull() {
			q2.err = fmt.Errorf("RecordValue for %s has a NULL primary key", vrel.Name)
			return q2
		}
		return q2.Where(fmt.Sprintf(`%s = $1`, ref.col.name), pkv)
	}
	q2.err = fmt.Errorf("No reference columns between %s and %s", q.from.Name, vrel.Name)
	return q2
}

// find a column
func (q *Query) refFor(kind refkind, target *Relation, within *Relation) *ref {
	if within.refs == nil {
		return nil
	}
	for _, ref := range within.refs {
		if ref.rel == target && ref.kind == kind {
			return ref
		}
	}
	return nil
}

// Return a new Query with a LIMIT set
func (q *Query) Limit(n int) *Query {
	if q.err != nil {
		return q
	}
	q2 := q.cp()
	q2.limit = n
	return q2
}

// Return a new Query with an OFFSET set
func (q *Query) Offset(n int) *Query {
	if q.err != nil {
		return q
	}
	q2 := q.cp()
	q2.offset = n
	return q2
}

// perform a query and return *Rows
// ensure that deferred err is checked
func (q *Query) rows(s string, params ...interface{}) (*Rows, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.tx.Query(s, params...)
}

// perform a query that returns RecordValues
func (q *Query) query(s string, params ...interface{}) ([]RecordValue, error) {
	rs, err := q.rows(s, params...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	all := make([]RecordValue, 0)
	for rs.Next() {
		vx, err := q.from.k(nil)
		if err != nil {
			return nil, err
		}
		v, ok := vx.(RecordValue)
		if !ok {
			return nil, fmt.Errorf("%T is not a RecordValue", vx)
		}
		v.SetRelation(q.from)
		err = rs.ScanRecord(v)
		if err != nil {
			return nil, err
		}
		all = append(all, v)
	}
	return all, nil
}

// perform a SELECT for the current query and
// return a slice of RecordValues
func (q *Query) Fetch() ([]RecordValue, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.query(q.selectSql(), q.selectArgs()...)
}

// perform a SELECT and return a single RecordValue for this query
// will return nil if no rows where returned
func (q *Query) FetchOne() (RecordValue, error) {
	rs, err := q.Limit(1).Fetch()
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}
	return rs[0], nil
}

// create a new Query with a WHERE filter for the relation's
// primary key and the call FetchOne
func (q *Query) Get(pk interface{}) (RecordValue, error) {
	if q.err != nil {
		return nil, q.err
	}
	pkcol := q.from.pk()
	if pkcol == nil {
		return nil, fmt.Errorf("No primary key found for relation %s", q.from.Name)
	}
	s := fmt.Sprintf(`%s = $1`, pkcol.name)
	return q.Where(s, pk).FetchOne()
}

func (q *Query) agg(sel string, v Value, vals ...interface{}) error {
	if q.err != nil {
		return q.err
	}
	rs, err := q.rows(q.selectSql(sel), q.selectArgs()...)
	if err != nil {
		return err
	}
	defer rs.Close()
	for rs.Next() {
		err = rs.Scan(v)
		if err != nil {
			return err
		}
	}
	err = rs.Err()
	if err != nil {
		return err
	}
	return rs.Close()
}

// perform a "SELECT count(*)" query for this Query
func (q *Query) Count() (int64, error) {
	v, _ := BigInt(0)
	err := q.agg("count(*)", v)
	if err != nil {
		return 0, err
	}
	return v.Val().(int64), nil
}

// perform a "SELECT sum(x)" query
func (q *Query) Sum(name string) (Value, error) {
	if q.err != nil {
		return nil, q.err
	}
	for _, c := range q.from.cols {
		if c.name == name {
			v, err := c.k(nil)
			if err != nil {
				return nil, err
			}
			err = q.agg(fmt.Sprintf("sum(%s)", name), v)
			return v, err
		}
	}
	return nil, fmt.Errorf("could not use sum(%s) unknown column name: %s", name, name)
}

// perform a "SELECT avg(x)" query
func (q *Query) Avg(name string) (Value, error) {
	if q.err != nil {
		return nil, q.err
	}
	for _, c := range q.from.cols {
		if c.name == name {
			v, err := Double(nil)
			if err != nil {
				return nil, err
			}
			err = q.agg(fmt.Sprintf("avg(%s)", name), v)
			return v, err
		}
	}
	return nil, fmt.Errorf("could not use avg(%s) unknown column name: %s", name, name)
}

// perform a "SELECT avg(x)" query
func (q *Query) Min(name string) (Value, error) {
	if q.err != nil {
		return nil, q.err
	}
	for _, c := range q.from.cols {
		if c.name == name {
			v, err := c.k(nil)
			if err != nil {
				return nil, err
			}
			err = q.agg(fmt.Sprintf("min(%s)", name), v)
			return v, err
		}
	}
	return nil, fmt.Errorf("could not use min(%s) unknown column name: %s", name, name)
}

// perform a "SELECT max(x)" query
func (q *Query) Max(name string) (Value, error) {
	if q.err != nil {
		return nil, q.err
	}
	for _, c := range q.from.cols {
		if c.name == name {
			v, err := c.k(nil)
			if err != nil {
				return nil, err
			}
			err = q.agg(fmt.Sprintf("max(%s)", name), v)
			return v, err
		}
	}
	return nil, fmt.Errorf("could not use max(%s) unknown column name: %s", name, name)
}

// perform a "SELECT array_agg(x)" query. Returns an array value
func (q *Query) ArrayAgg(name string) (Value, error) {
	if q.err != nil {
		return nil, q.err
	}
	for _, c := range q.from.cols {
		if c.name == name {
			v, err := Array(c.k)(nil)
			if err != nil {
				return nil, err
			}
			err = q.agg(fmt.Sprintf("array_agg(%s)", name), v)
			return v, err
		}
	}
	return nil, fmt.Errorf("could not use array_agg(%s) unknown column name: %s", name, name)
}

// generate SQL string for a SELECT
// optionally pass in a list of column names to
// override the SELECT args
func (q *Query) selectSql(names ...string) string {
	cols := strings.Join(names, ",")
	if cols == "" {
		cols = q.from.fields(true)
	}
	return fmt.Sprintf(`SELECT %s FROM %s %s %s %s`,
		cols,
		q.from.Name,
		q.whereExpr(),
		q.limitExpr(),
		q.offsetExpr())
}

// regexp to match the $X placeholders in queries
var placePat = regexp.MustCompile(`(?:[^\\]\$)(\d+)`)

// convert all the where expressions into a single one
func (q *Query) whereExpr() string {
	if len(q.where) == 0 {
		return ""
	}
	// since we restart the $X count for the params each time we call
	// Where we now have to rejig the $1 statements so that they line up correctly
	sts := make([]string, len(q.where))
	var i int64
	for idx, st := range q.where {
		if i == 0 { // find the bigest $X in this string
			matches := placePat.FindAllStringSubmatch(st, -1)
			if len(matches) == 0 {
				continue
			}
			for _, m := range matches {
				n, err := strconv.ParseInt(m[1], 10, 64)
				if err != nil {
					panic(fmt.Sprintf("could not convert %s to int", m[1]))
				}
				if n > i {
					i = n
				}
			}
		} else { // update each $X we find by adding i to it
			st = placePat.ReplaceAllStringFunc(st, func(m string) string {
				n, err := strconv.ParseInt(m[2:], 10, 64)
				if err != nil {
					panic(fmt.Sprintf("could not convert %s to int", m[2:]))
				}
				return fmt.Sprintf(`%s%d`, m[0:2], n+1)
			})
		}
		sts[idx] = st
	}
	return fmt.Sprintf(`WHERE %s`, strings.Join(sts, " AND "))
}

func (q *Query) limitExpr() string {
	if q.limit == 0 {
		return ""
	}
	return fmt.Sprintf(`LIMIT %d`, q.limit)
}

func (q *Query) offsetExpr() string {
	if q.offset == 0 {
		return ""
	}
	return fmt.Sprintf(`OFFSET %d`, q.offset)
}

// return the vals to bind to placholders for selectSql
func (q *Query) selectArgs() []interface{} {
	vals := make([]interface{}, 0)
	vals = append(vals, q.whereParams...)
	return vals
}

// wrapper type around sql.Tx
// Adds methods for INSERTing, UPDATEing and DELETEing
// RecordValues
type Tx struct {
	*sql.Tx
	db *DB
}

func (tx *Tx) Relations() (rels map[string]*Relation, err error) {
	return tx.db.Relations()
}

// Create a Query for a named relation
// any errors are defered until an actual query is performed
func (tx *Tx) From(name string) *Query {
	// TODO: stop loading ALL relations just to get one
	q := new(Query)
	rel, err := tx.db.Relation(name)
	if err != nil {
		q.err = err
		return q
	}
	q.from = rel
	q.tx = tx
	return q
}

// perform query q and update values in v from the first RETURNING result
func (tx *Tx) queryAndUpdate(q string, v RecordValue, update bool) error {
	rs, err := tx.Query(q, v.Relation().valArgs(v, update)...)
	if err != nil {
		return err
	}
	defer rs.Close()
	for rs.Next() {
		err := rs.ScanRecord(v)
		if err != nil {
			return err
		}
	}
	return rs.Close()
}

// INSERT RecordValue(s)
func (tx *Tx) Insert(vs ...RecordValue) error {
	for _, v := range vs {
		rel := v.Relation()
		if rel == nil {
			return fmt.Errorf("RecordValue does not have a relation set")
		}
		bnds, _ := rel.bindings(false, false)
		s := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s) RETURNING %s`,
			rel.Name,
			rel.fields(false),
			bnds,
			rel.fields(true))
		err := tx.queryAndUpdate(s, v, false)
		if err != nil {
			return err
		}
	}
	return nil
}

// UPDATE RecordValue(s)
func (tx *Tx) Update(vs ...RecordValue) error {
	for _, v := range vs {
		rel := v.Relation()
		if rel == nil {
			return fmt.Errorf("RecordValue does not have a relation set")
		}
		pk := rel.pk()
		if pk == nil {
			return fmt.Errorf("Relation must have a primary key to use Update")
		}
		bnds, n := rel.bindings(false, true)
		s := fmt.Sprintf(`UPDATE %s SET %s WHERE %s = $%d RETURNING %s`,
			rel.Name,
			bnds,
			pk.name,
			n+1,
			rel.fields(true))
		err := tx.queryAndUpdate(s, v, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// UPDATE or INSERT RecordValue(s)
func (tx *Tx) Upsert(vs ...RecordValue) (err error) {
	for _, v := range vs {
		rel := v.Relation()
		if rel == nil {
			return fmt.Errorf("RecordValue does not have a relation set")
		}
		pk := rel.pk()
		if pk == nil {
			return fmt.Errorf("Relation has no primary key")
		}
		pkv := v.ValueBy(pk.name)
		if pkv == nil || pkv.IsNull() {
			err = tx.Insert(v)
		} else {
			err = tx.Update(v)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// DELETE RecordValue(s)
func (tx *Tx) Delete(vs ...RecordValue) error {
	for _, v := range vs {
		rel := v.Relation()
		if rel == nil {
			return fmt.Errorf("RecordValue does not have a relation set")
		}
		pk := rel.pk()
		if pk == nil {
			return fmt.Errorf("Relation has no primary key")
		}
		pkv := v.ValueBy(pk.name)
		if pkv == nil {
			return fmt.Errorf("Value must have a primary key set")
		}
		s := fmt.Sprintf(`DELETE FROM %s WHERE %s = $1`,
			rel.Name,
			pk.name)
		rs, err := tx.Tx.Query(s, pkv)
		if err != nil {
			return err
		}
		rs.Close()
	}
	return nil
}

// like sql.Tx.Query only returns a *Rows rather than *sql.Rows
func (tx *Tx) Query(q string, vals ...interface{}) (*Rows, error) {
	rows, err := tx.Tx.Query(q, vals...)
	if err != nil {
		return nil, err
	}
	rs := new(Rows)
	rs.Rows = rows
	return rs, nil
}

// wrapper type around sql.DB
// adds methods for getting meta infomation from the db (via Relations),
// automatically building Value types and convience functions for dealing
// with RecordValues (via *Rowss)
type DB struct {
	*sql.DB
	rels      map[string]*Relation
	getRels   *sql.Stmt
	getCols   *sql.Stmt
	getType   *sql.Stmt
	getLabels *sql.Stmt
}

// Analog of sql.Open that returns a *DB
// requires a "postgres" driver (lib/pq) is registered
func Open(connstr string) (*DB, error) {
	rawdb, err := sql.Open("postgres", connstr)
	if err != nil {
		return nil, err
	}
	return newDB(rawdb)
}

// init *DB by preparing any stmts we might need
func newDB(rawdb *sql.DB) (db *DB, err error) {
	db = new(DB)
	db.DB = rawdb
	db.getRels, err = db.DB.Prepare(selectRelsSql)
	if err != nil {
		return
	}
	db.getCols, err = db.DB.Prepare(selectColsSql)
	if err != nil {
		return
	}
	db.getType, err = db.DB.Prepare(selectTypeSql)
	if err != nil {
		return
	}
	db.getLabels, err = db.DB.Prepare(selectEnumSql)
	if err != nil {
		return
	}
	return
}

// Create a new RecordValue for the named relation
func (db *DB) New(name string, args interface{}) (RecordValue, error) {
	rel, err := db.Relation(name)
	if err != nil {
		return nil, err
	}
	return rel.New(args)
}

// Return all the Relations from the database
func (db *DB) Relations() (rels map[string]*Relation, err error) {
	if db.rels == nil {
		rels, err = db.relations()
		if err != nil {
			return nil, err
		}
		db.rels = rels
	}
	return db.rels, err
}

// Create a Query for a named relation
// any errors are defered until an actual query is performed
func (db *DB) From(name string) *Query {
	// TODO: stop loading ALL relations just to get one
	q := new(Query)
	rel, err := db.Relation(name)
	if err != nil {
		q.err = err
		return q
	}
	q.from = rel
	q.tx = db
	return q
}

// Get Relation info by name
func (db *DB) Relation(name string) (*Relation, error) {
	// TODO: stop loading ALL relations just to get one
	rels, err := db.Relations()
	if err != nil {
		return nil, err
	}
	rel, ok := rels[name]
	if !ok {
		return nil, fmt.Errorf("No relation found: %s", name)
	}
	return rel, nil
}

// like sql.DB.Query only returns a *Rows rather than sql.Rows
func (db *DB) Query(q string, vals ...interface{}) (*Rows, error) {
	rows, err := db.DB.Query(q, vals...)
	if err != nil {
		return nil, err
	}
	rs := new(Rows)
	rs.Rows = rows
	return rs, nil
}

// same as sql.DB.Begin() only returns our *Tx not *sql.Tx
func (db *DB) Begin() (*Tx, error) {
	rawtx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{rawtx, db}, nil
}

// INSERT the given RecordValue(s) into the db
// runs multiple INSERTs within a transaction
func (db *DB) Insert(vs ...RecordValue) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = tx.Insert(vs...)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// UPDATE the given RecordValue(s) into the db
// runs multiple INSERTs within a transaction
func (db *DB) Update(vs ...RecordValue) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = tx.Update(vs...)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// INSERT OR UPDATE the given RecordValue(s) into the db
// runs multiple INSERTs within a transaction
func (db *DB) Upsert(vs ...RecordValue) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = tx.Upsert(vs...)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// DELETE the given RecordValue(s) into the db
// runs multiple INSERTs within a transaction
func (db *DB) Delete(vs ...RecordValue) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = tx.Delete(vs...)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// create a new map of all Relations in the db
func (db *DB) relations() (map[string]*Relation, error) {
	rels := make(map[string]*Relation)
	rows, err := db.getRels.Query()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var (
			oid  uint32
			name string
		)
		err = rows.Scan(&oid, &name)
		if err != nil {
			return nil, err
		}
		rel, err := db.relation(name, oid)
		if err != nil {
			return nil, err
		}
		rels[name] = rel
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	// now we have all the relation info we can extend it with
	// the reference info
	for _, rel := range rels {
		for _, c := range rel.cols {
			// skip if col does not reference another rel
			if c.reft == "" {
				continue
			}
			// get the foreign referenced rel
			frel, ok := rels[c.reft]
			if !ok {
				return nil, fmt.Errorf("expected to find referenced relation: %s", c.reft)
			}
			// add has_one to this rel
			// strip _id suffix and camelize field name. branch_location_id -> BranchLocation
			rx := regexp.MustCompile(`_(id|sku|key)$`)
			hasOneName := rx.ReplaceAllString(c.name, "")
			rel.refs = append(rel.refs, &ref{hasOneName, r_hasOne, frel, c})
			// add has_many to the foreign rel
			// NOTE:
			// if there are multiple local keys pointing to the foreign model
			// then the relation will be setup to look at ALL of the keys
			// ie if you have a table (person) with two foreign keys (locate_a_id, locate_b_id)
			// then the has_many side of that relationship will lookup like:
			// SELECT * FROM locate WHERE id = locate_a_id OR id = locate_b_id
			hasManyName := rel.Name
			frel.refs = append(frel.refs, &ref{hasManyName, r_hasMany, rel, c})
		}
	}
	return rels, rows.Close()
}

// return list of cols for a pg_class oid
func (db *DB) cols(reloid uint32) ([]*col, error) {
	rows, err := db.getCols.Query(reloid)
	if err != nil {
		return nil, err
	}
	cols := make([]*col, 0)
	for rows.Next() {
		c := new(col)
		var argstr string
		var num int
		err = rows.Scan(&num, &c.name, &c.typ, &c.oid, &c.notNull,
			&c.pk, &c.reft, &c.reff, &argstr)
		if err != nil {
			return nil, err
		}
		var args []string
		if argstr != "" {
			args = strings.Split(argstr, ",")
		}
		// build the Valstructor for this col
		c.k, err = db.kind(c.oid, args...)
		if err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return cols, rows.Close()
}

// create a new Relation from the db
func (db *DB) relation(name string, oid uint32) (r *Relation, err error) {
	r = new(Relation)
	r.Name = name
	r.cols, err = db.cols(oid)
	r.k = Record(r.cols...)
	return r, err
}

// lookup a Valstructor for the pg_type of the column
// if nothing is found in the typs map, then it will
// try to construct an array or composite type from the
// info in the pg_type system table
func (db *DB) kind(oid uint32, args ...string) (Valstructor, error) {
	if f, ok := typs[oid]; ok {
		return f(args...)
	}
	return db.complexKind(oid, args...)
}

// construct an array or composite Valstructor by getting type
// details from pg_type
func (db *DB) complexKind(oid uint32, args ...string) (Valstructor, error) {
	rows, err := db.getType.Query(oid)
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, fmt.Errorf("No pg_type with oid %d", oid)
	}
	var (
		name     string // the string representation
		typ      string // b=base c=composite d=domain e=enum p=pseudo
		delim    string // delimeter when array=0
		relid    uint32 // pg_class oid when typ=c
		elem     uint32 // the pg_type oid of the element (or 0 if not array)
		array    uint32 // the pg_type oid of the array version of type (or 0 if is an array)
		notnull  bool   // rejects nulls
		basetype uint32 // pg_type oid of base type when typ=d
		typmod   int32  // type-specific data supplied at table creation time
		ndims    int32  // num of array dimension when typ=d
	)
	err = rows.Scan(
		&name, &typ, &delim, &relid, &elem, &array,
		&notnull, &basetype, &typmod, &ndims,
	)
	if err != nil {
		return nil, err
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	err = rows.Close()
	if err != nil {
		return nil, err
	}
	switch typ {
	// base types
	case "b":
		switch array {
		// handle array
		case 0:
			elk, err := db.kind(elem, args...)
			if err != nil {
				return nil, err
			}
			return Array(elk), nil
		// handle other base types
		default:
			switch name {
			// auto-register hstore oid
			case "hstore":
				typs[oid] = func(args ...string) (Valstructor, error) {
					return HStore, nil
				}
				return HStore, nil
			// other (unknown) base types
			default:
				return nil, fmt.Errorf("base type %s with oid %d is not implimented", name, oid)
			}
		}
	// composite types
	case "c":
		cols, err := db.cols(relid)
		if err != nil {
			return nil, err
		}
		return Record(cols...), nil
	// domain types
	case "d":
		return nil, fmt.Errorf("domain types not implimented yet")
	// enum types
	case "e":
		labels, err := db.enumLabelsFor(oid)
		if err != nil {
			return nil, err
		}
		if len(labels) == 0 {
			return nil, fmt.Errorf("No labels found for Enum type %s", name)
		}
		return Enum(labels...), nil
	// psuedo types
	default:
		return nil, fmt.Errorf("psuedo pg_types cannot be supported")
	}
	panic("unreachable")
}

// fetch all the possible labels for enum type with the given oid
func (db *DB) enumLabelsFor(oid uint32) ([]string, error) {
	rows, err := db.getLabels.Query(oid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	labels := make([]string, 0)
	for rows.Next() {
		var label string
		err = rows.Scan(&label)
		if err != nil {
			return nil, err
		}
		labels = append(labels, label)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return labels, rows.Close()
}
