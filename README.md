NOTE
====

This is a fork of the original [PQL](https://bitbucket.org/pkg/pql/) from bitbucket.

What is pql?
============

`pql` is a go ([golang](http://www.golang.org)) package to compliment
the standard [database/sql](http://www.golang.org/pkg/database/sql) package
when working with [PostgreSQL](http://www.postgresql.org).

Key Features:

* `Value` types for handling all pg types from simple `integer` columns to complex composite/array constructions.
* Keep your schema defined in one place (in your database) and automatically "model" your rows as RecordValues
* Thin wrapper types around the standard `database/sql` types. (with some convinience functions for working with `RecordValue`s
* A simple ORMish API for querying the db and returning `RecordValue`s


What is isn't
==============

* `pql` is not a driver for postgres. Use [lib/pq](http://github.com/lib/pq) for that.
* `pql` is not a full-on ORM-style library. It provides helpers for very common cases (The Query builder for SELECT and INSERT, UPDATE, DELETE methods for working with RecordValues). It is expected that you ultize PostgreSQL itself for building complex queries as Views, Functions etc, then use `pql` to access those relations with simple queries. Let postgres and go each do what they are best at.

Why use this?
=============

The main reason this library was created was a need to work with Arrays, HStore and Composite types. With just the standard database/sql package you quickly find you need help when you need to read something like:

```SQL
	SELECT ARRAY[
			ROW(1, '"key" => "value1"'::hstore, 'jeff'),
			ROW(2, '"key" => "value2"'::hstore, 'bob')
		] as complexthing;
```

With `pql` you can define a `Value` to handle such a column like:

```Go
	MyComplexValue := Array(Record(
		Col("id",   BigInt),
		Col("tags", HStore),
		Col("name", Text),
	)
```

Then use either the standard database/sql's, or pql's `DB`/`Rows` to `Scan` into the value:

```Go
	...
	v := MyComplexValue(nil) // create a NULL MyComplexValue
	rows.Scan(v)             // use Scan to read data into it

	for _, el := range v.Values() {
		fmt.Println("name", el.Get("name"))  // prints jeff/bob
		fmt.Println("tags", el.Map())        // prints out representation of map of tags
	}
```

Since most of the time you are probably not building complex anonymous types, but using types defined within relations you can skip the step of defining `Values` upfront and let `pql` do it for you. See "Working with Query & RecordValues" example below.


Documentation
=============

See [godoc](http://godoc.org/bitbucket.org/pkg/pql)

Examples
========

Using Values for base types
----------------------------

(TODO)

Using Values for array types
----------------------------

(TODO)

Using Values for complex composite types
----------------------------------------

(TODO)


Working with Query & RecordValues
---------------------------------

`RecordValue` is an interface type for working with a database row (record). Think of it like a lightweight model. Various *DB and other methods return (or work with) RecordValues.

For a simple example we'll assume we have a database with the following setup:

```sql
    CREATE TABLE person (
		name text,
		age integer
    );
	INSERT INTO person (name,age) VALUES ('bob', 25);
	INSERT INTO person (name,age) VALUES ('jeff', 35);
	INSERT INTO person (name,age) VALUES ('alice', 26);
```

Now if we wanted a little go cmd to print out all the names of people over 25. Our
minimal (sans error checking) code might look something like:

```Go
	package main

	import(
		_ "github.com/lib/pq"
		"bitbucket.org/pkg/pql"
		"fmt"
	)

	func main(){
		db, _ := pql.Open("")
		rs, _ := db.From("person").Where("age > $1", 25).Fetch()
		for _, r := range rs {
			fmt.Println("%s is over 25", r.Get("name"))
		}
	}
```





Development
===========

Testing
-------

* You will need to create a db called `pqgotest`
* You will need to install the *HStore* extension (part of the contrib package)
* run `CREATE EXTENSION hstore;` on your db

To run tests use the ENV variables like PGHOST. (see [lib/pq](https://github.com/lib/pq) for options).

`cd` to the package directory and run:

```Bash
	PGHOST=/var/run/postgresql go test
```

Authors
=======

Chris Farmiloe


License
=======

MIT, See LICENSE.md file
