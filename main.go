package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/kricen/shstorage/storage"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Define a main function to test the peformance about the storage of simhash
// in order to ensure high availability of the simhash'storage,i use the real
// data from our remote test db (the backup from the busniess Databas).
var (
	connUsername string //db name
	connPassord  string //db password
	connAddr     string //connect addr : "ip:port/collection"
	lastID       int64  //last read id from mongodb
	s            *storage.Store8
	session      *mgo.Session
	collection   *mgo.Collection
	count        int64 = 1
)

// Mondodb Entity
type MondodbEntity struct {
	ID      int64 `bson:"id" json:"id"`
	Simhash int64 `bson:"simhash" json:"simhash"`
}

func main() {

	// init the db's session
	err := connDB(os.Args)
	if err != nil {
		fmt.Printf("Oops,encounter an error when connect the mongodb :%s ", err.Error())
	}
	defer closeMgo()

	// init a storage for hamming distance 8
	s = storage.New8(100000000, storage.NewU64Slice)
	fmt.Println(s == nil)
	err = readSimhashsFromDB(s)
	if err != nil {
		fmt.Printf("Oops,encounter an error :%s ", err.Error())
		return
	}
	s.Finish()

	start := time.Now().UnixNano()
	table := s.Search(4886085145408545231)
	end := time.Now().UnixNano()

	fmt.Printf("query duration : %d ms", end-start) // empirical query time : 0 ms
	fmt.Printf("query results:%+v", table)

}

// read simhashs from mongodb,about 7 million datas in mongodb
func readSimhashsFromDB(store *storage.Store8) (err error) {

	cols := make([]MondodbEntity, 0)
	err = collection.Find(bson.M{"id": bson.M{"$gt": lastID}}).Limit(50000).Sort("id").All(&cols)
	if err != nil {
		return
	}
	if len(cols) == 0 {
		return
	}
	fmt.Println(len(cols))
	for _, v := range cols {
		lastID = v.ID
		count++
		fmt.Println(count)
		s.Add(uint64(v.Simhash), uint64(lastID))
	}
	readSimhashsFromDB(store)
	return
}

// combinate the connection param and connect the mongodb
func connDB(params []string) error {

	if len(params) < 4 {
		return errors.New("Param Error")
	}
	connUsername = params[1]
	connPassord = params[2]
	connAddr = params[3]

	url := fmt.Sprintf("mongodb://%s:%s@%s", connUsername, connPassord, connAddr)
	fmt.Printf("conn url :%s \n", url)
	ms, err := mgo.Dial(url)

	if err != nil {
		return err
	}

	ms.SetMode(mgo.Monotonic, true)
	session = ms

	err = session.Ping()

	if err != nil {

		return err
	}
	collection = ms.DB("fingerprintDB").C("docFingerprintCol")

	return nil
}

//CloseMgo : close the connect from mongodb
func closeMgo() {
	if session != nil {
		session.Close()
	}
}
