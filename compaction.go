/*
 * //  Licensed under the Apache License, Version 2.0 (the "License");
 * //  you may not use this file except in compliance with the
 * //  License. You may obtain a copy of the License at
 * //    http://www.apache.org/licenses/LICENSE-2.0
 * //  Unless required by applicable law or agreed to in writing,
 * //  software distributed under the License is distributed on an "AS
 * //  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * //  express or implied. See the License for the specific language
 * //  governing permissions and limitations under the License.
 */

package gokvstore

import (
	"fmt"
	"sort"
	"path"
	"io/ioutil"
	"log"
	"github.com/tylertreat/BoomFilters"
	"time"
)

const (
	minBucketSize = 2
	maxBucketSize = 8
)

type Compactor struct {
	fs      *FileSystem
	filter  *boom.ScalableBloomFilter
	files   []string
	buckets []*bucket
}

type bucket struct {
	files     []string
	processed bool
}

type compactionStats struct {
	numFilesBeforeCompaction int
	numFilesAfterCompaction  int
	numKeysBeforeCompaction  uint64
	numKeysAfterCompaction   uint64
	timeToCompactBucket      string
	err                      error
}

func (c *Compactor) shouldCompact() bool {
	if c.files != nil && len(c.files) > minBucketSize {
		return true
	}
	return false
}

func (c *Compactor) Compact() {
	if !c.shouldCompact() {
		fmt.Println("Not Enough SSTables to start compaction")
		return
	}
	c.makeBuckets(c.files)
	done := make(chan interface{})
	defer close(done)
	for result := range c.compactBuckets(done, c.buckets) {
		fmt.Printf("filesBeforeCompaction : %d\n", result.numFilesBeforeCompaction)
		fmt.Printf("filesAfterCompaction : %d\n", result.numFilesAfterCompaction)
		fmt.Printf("numKeysBeforeCompaction : %d\n", result.numKeysBeforeCompaction)
		fmt.Printf("numKeysAfterCompaction : %d\n", result.numKeysAfterCompaction)
		fmt.Printf("timeTakenToCompactBucket : %s\n", result.timeToCompactBucket)
		fmt.Printf("error : %v\n", result.err)

	}

	c.deleteProcessedFiles()

}
func (c *Compactor) makeBuckets(files []string) {
	if len(files) >= minBucketSize && len(files) <= maxBucketSize {
		b := bucket{c.files, false}
		c.buckets = append(c.buckets, &b)
	} else {
		c.makeBuckets(files[0 : len(files)/2])
		c.makeBuckets(files[len(files)/2:])
	}
}

func (c *Compactor) compactBuckets(done <-chan interface{}, buckets []*bucket) (<-chan compactionStats) {
	results := make(chan compactionStats)
	go func() {
		defer close(results)
		for _, bucket := range buckets {
			var cs compactionStats
			cs = c.compactBucket(bucket)
			select {
			case <-done:
				return
			case results <- cs:
			}
		}
	}()
	return results
}

var cerr error

func (c *Compactor) compactBucket(b *bucket) (cs compactionStats) {

	startTime := time.Now()
	iters := make([]*chunkIterator, 0)
	sort.Sort(ByTime{b.files, DefaultNameFormat})
	for _, f := range b.files {
		fName := path.Join(c.fs.path, f) + dataFileExt
		data, err := ioutil.ReadFile(fName)
		if err != nil {
			cerr = err
			log.Fatal("failed to compact bucket")
		}
		iter := NewChunkIterator(data)
		iters = append(iters, iter)
	}
	sst, err := c.fs.NewSSTable()
	if err != nil {
		cerr = err
		fmt.Errorf("unable to create new sstable %v\n", err)
	}
	w := NewWriter(sst, c.fs.options.UseCompression)

	mergingIter := NewMergingIterator(iters)
	for mergingIter.Next() {
		c.filter.Add(mergingIter.Key())
		w.Set(mergingIter.Key(), mergingIter.Value())
	}
	_, err = c.filter.WriteTo(sst.filterfile)
	defer sst.filterfile.Close()
	if err != nil {
		cerr = err
		fmt.Errorf("unable to write filter %v\n", err)
	}
	err = w.Close()
	if err != nil {
		cerr = err
		fmt.Errorf("unable to write sstable %v\n", err)
	}
	elapsed := time.Since(startTime)
	timeTaken := fmt.Sprintf("%s", elapsed)
	b.processed = true
	stats := compactionStats{
		numFilesAfterCompaction:  1,
		numFilesBeforeCompaction: len(b.files),
		numKeysBeforeCompaction:  mergingIter.numKeysBeforeCompaction,
		numKeysAfterCompaction:   mergingIter.numKeysAfterCompaction,
		timeToCompactBucket:      timeTaken,
		err:                      cerr,
	}
	return stats

}

func (c *Compactor) deleteProcessedFiles() {
	for _, b := range c.buckets {
		if b.processed {
			for _, f := range b.files {
				c.fs.DeleteSSTable(f)
			}
		}
	}

}

func NewCompactor(path string) *Compactor {

	opts := Options{
		ReadOnly:       false,
		UseCompression: true,
		SyncWrite:      false,
	}
	fs := NewFS(path, &opts)
	files := GetDataFiles(path)
	return &Compactor{
		fs:     fs,
		files:  files,
		filter: boom.NewDefaultScalableBloomFilter(0.01),
	}
}
