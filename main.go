package main

import (
	"github.com/etcd-io/bbolt"

	"bytes"
	"encoding/binary"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

var (
	flagDB   = flag.String("db", "./db.db", "-db ./db.db")
	flagBind = flag.String("bind", "127.0.0.1:1251", "-bind 127.0.0.1:1251")

	kvdb *bbolt.DB

	bucketValues = []byte("values")
	bucketTTL    = []byte("ttl")
	bucketKTTL   = []byte("vttl")
)

func main() {
	flag.Parse()

	var err error
	kvdb, err = bbolt.Open(*flagDB, 0755, nil)
	if err != nil {
		panic(err)
	}

	kvdb.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(bucketValues)
		tx.CreateBucketIfNotExists(bucketTTL)
		tx.CreateBucketIfNotExists(bucketKTTL)
		return nil
	})

	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			kvdb.Update(func(tx *bbolt.Tx) error {
				bv := tx.Bucket(bucketValues)
				bttl := tx.Bucket(bucketTTL)
				bkttl := tx.Bucket(bucketKTTL)

				cbttl := tx.Bucket(bucketTTL).Cursor()

				now := time.Now().UnixNano()
				n := 0
				for k, v := cbttl.First(); k != nil && bytes.Compare(k, itob(int(now))) < 0; k, v = cbttl.Next() {
					if n >= 50 {
						break
					}
					bttl.Delete(k)
					bv.Delete(v)
					bkttl.Delete(v)
					n++
				}
				return nil
			})
		}
	}()

	http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		ttl, err := strconv.Atoi(r.URL.Query().Get("ttl"))
		key := r.URL.Query().Get("key")
		if err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		value, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Read boty error", http.StatusInternalServerError)
			return
		}

		kvdb.Update(func(tx *bbolt.Tx) error {
			bv := tx.Bucket(bucketValues)
			bttl := tx.Bucket(bucketTTL)
			bkttl := tx.Bucket(bucketKTTL)

			expires := time.Now().UnixNano() + int64(ttl)*time.Second.Nanoseconds()

			v := bkttl.Get([]byte(key))

			if v == nil {
				bv.Put([]byte(key), []byte(value))
			} else {
				bttl.Delete(v)
			}

			bttl.Put([]byte(itob(int(expires))), []byte(key))
			bkttl.Put([]byte(key), []byte(itob(int(expires))))

			return nil
		})
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		var v []byte
		kvdb.View(func(tx *bbolt.Tx) error {
			bv := tx.Bucket(bucketValues).Get([]byte(key))
			if bv != nil {
				v = bcopy(bv)
			}
			return nil
		})
		if v == nil {
			http.NotFound(w, r)
			return
		}
		w.Write(v)
	})

	log.Fatal(http.ListenAndServe(*flagBind, nil))
}

func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func btoi(v []byte) uint64 {
	return binary.BigEndian.Uint64(v)
}

func bcopy(s []byte) []byte {
	b := make([]byte, len(s))
	copy(b, s)
	return b
}
