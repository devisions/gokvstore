# gokvstore

This is the implementation of a KV store developed for the book **Go The Other Way**. 

**Not meant for production use** 

License
=======

BSD 3-Clause License 

Example
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
	cursor.Close()
	//close the database
	err = db.Close()
	
    
=======

Design
=======



