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
    	}
	// or use the default options. 
	
	db, err := Open(dir, opts)
	
	err = db.Put([]byte("somekey"), []byte("somevalue"))
	
	
	val,err := db.Get([]byte("somekey"))
	
	ok, err = db.Delete([]byte("somekey"))
	
	c,err := db.Range([]byte("startkey"),[]byte("endkey"))
	
	err = db.Close()
	
    
=======

Design
=======



