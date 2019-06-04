# gokvstore

This is the implementation of a KV store developed for the book **Go The Other Way**. 

**Not meant for production use** 

License
=======

Apache License 2.0

Example - Basic Usage
=======

	dir := "path/to/some/dir"
    opts := Options{
    		ReadOnly:       false,
    		UseCompression: true,
    		SyncWrite:      false,
    	}// or use the default options. 
	
	//open the database
	db, err := Open(dir, opts)
	
	//work with the database
	err = db.Put([]byte("somekey"), []byte("somevalue"))
	
	
	val,err := db.Get([]byte("somekey"))
	
	ok, err = db.Delete([]byte("somekey"))
	
	cursor,err := db.Range([]byte("startkey"),[]byte("endkey"))
	//iterate over keys and values
	for cursor.Next() {
	    k := cursor.Key()
	    v:=  cursor.Value()
	}
	//close the cursor
	cursor.Close()
	//close the database
	err = db.Close()
	
    
=======



Example - Compacting the Database
=======
dir := "path/to/database/dir"

c := NewCompactor(dir)
c.Compact()


=======


Features
=======
* Keys and values are arbitrary byte arrays. 
* Data is stored sorted by keys. 
* The basic operations a client can perform are *Put(key,value), Get(key), Delete(Key), Range(startKey,endKey)*
* The data store corresponds to a directory on the file system. All the contents of a database are stored in this directory
* A client can open a database by passing the path of the directory to the *Open* function.The client can specify additional options like opening the database in read only format, whether to compress data while storing, whether writes are synchronous or asynchronous etc.
* When a client is done using a database, it can make a call to *Close* the database. 
* A database can only be opened by one process for writes. However multiple readers can read concurrently from the database.
* The database supports range queries by specifying a start and an end key. A range query returns a cursor which can be used to iterate over the range of key-value pairs. 
* The database stores each block as a compressed block using the *Snappy Compression* library. Data is always compressed by default. However, it can be switched off, but thats not recommended. 
* Data is filtered on reads by using a *Bloom Filter*. This ensures that we don't need to read multiple files for *Get*. 
* In order to keep the number of files manageable, as well as remove redundant keys, compaction is periodically performed to merge files together. The default compaction supported by the database is *Size Tiered Compaction*. 
* In order to avoid data loss in the event of a crash, the writes are appended to a write ahead log(WAL). If the client chooses to write data synchronously, the writes are written to the WAL immediately, else they are deferred before being written. Each SSTable has its own write ahead log. If the database crashes, the WAL is used to restore the most current SSTable. Older WAL's are periodically discarded.


Limitations
=======

* This is not a relational database. There is no support for SQL, joins or user defined indexes. The database internally maintains an index per SSTable to speed up reads. 
* Only a single process can write to the database at a point in time. Reads can be performed concurrently by multiple processes.
* There is no client server support for the database.  
* Range is only supported if the start and end key reside in the same segment. 
* Compaction needs to be triggered manually. There is no automatic compaction process provided. 
