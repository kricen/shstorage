package storage

import (
	"math/rand"
	"testing"
	"time"
)

// func TestAdd3(t *testing.T) {
// 	s := New3(size, NewU64Slice)
// 	testAdd(t, s, size, queries, 3)
// }

// func TestAdd3Small(t *testing.T) {
// 	s := New3Small(size)
// 	testAdd(t, s, size, queries, 3)
// }

func TestAdd8(t *testing.T) {
	s := New8(size, NewU64Slice)
	testRealAdd8(t, s, size, queries, 6)
}
func testRealAdd8(t *testing.T, s Storage, size, queries, d int) {
	s.Add(4886085145408545231, 100)
	s.Add(1189710714089908775, 200)
	var start, end int64
	start = time.Now().Unix()
	t.Logf("start add ,time := %d", start)
	for i := 0; i < size; i++ {
		s.Add(uint64(rand.Int63()), uint64(i))
	}
	end = time.Now().Unix()
	t.Logf("end add ,time := %d ,add duration time :%d", end, (end-start)/1000)

	s.Finish()
	start = time.Now().Unix()
	t.Logf("start search ,time := %d", start)
	// 100
	table := s.Search(4780250519815037391) //8
	end = time.Now().Unix()
	t.Logf("end search ,time := %d,search duration time :%d", end, (end-start)/1000)
	t.Logf("result %+v", table)
	// 200
	table = s.Search(1189710709794957863) //2
	t.Logf("result 2 2 %+v", table)

	table = s.Search(1189710714089908775) //0
	t.Logf("result 2 0  %+v", table)

	table = s.Search(1191118088973331247) //5
	t.Logf("result 2 5  %+v", table)
	table = s.Search(1191118084678364719) //6
	t.Logf("result 2 6  %+v", table)

	table = s.Search(5802804107484604963) //7
	t.Logf("result 2 7  %+v", table)

	table = s.Search(8108647116632238627) //8
	t.Logf("result 2 8  %+v", table)

	table = s.Search(8180704710736226851) //9
	t.Logf("result 2 9  %+v", table)

	table = s.Search(5874861701523581474) //10
	t.Logf("result 2 10  %+v", table)

	table = s.Search(1265392300650668583) //11
	t.Logf("result 2 11 %+v", table)

}

func testAdd8(t *testing.T, s Storage, size, queries, d int) {

	rand.Seed(0)

	for i := 0; i < size; i++ {
		s.Add(uint64(rand.Int63()), uint64(i))
	}

	sig := uint64(0x001122334455667788)
	s.Add(sig, 0xdeadbeef)

	s.Finish()

	var fails int

	for j := 0; j < queries; j++ {

		q := sig

		// bits := rand.Intn(7)
		bits := d

		for i := 0; i < bits; i++ {
			q ^= 1 << uint(rand.Intn(64))
		}

		found := s.Find(q)
		var foundbeef bool
		for _, v := range found {
			if v == 0xdeadbeef {
				foundbeef = true
				break
			}

		}
		if !foundbeef {
			t.Errorf("sig = %016x (%064b) (found=%v)\n", sig, sig^q, found)
			fails++
		}
	}

	if fails != 0 {
		t.Logf("fails = %f", 100*float64(fails)/float64(queries))
	}
}
