package main

// Define a main function to test the peformance about the storage of simhash
// in order to ensure high availability of the simhash'storage,i use the real
// data from our remote test db (the backup from the busniess Databas).
var (
	connName       string //db name
	connPassord    string //db password
	connPort       string //db port
	connCollection string //db collection (mongodb)
	endID          int64
)

func main() {

}

// read simhashs from mongodb,about 7 million datas in mongodb
func readSimhashsFromDB(startID int64) (simHashs []int64, err error) {

	return
}
