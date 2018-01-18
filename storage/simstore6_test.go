package storage

import (
	"math/rand"
	"testing"
)

const size = 10000000
const queries = 10000000

// func TestAdd3(t *testing.T) {
// 	s := New3(size, NewU64Slice)
// 	testAdd(t, s, size, queries, 3)
// }

// func TestAdd3Small(t *testing.T) {
// 	s := New3Small(size)
// 	testAdd(t, s, size, queries, 3)
// }

func TestAdd6(t *testing.T) {
	s := New6(size, NewU64Slice)
	testRealAdd(t, s, size, queries)
}
func testRealAdd(t *testing.T, s Storage, size, queries int) {
	s.Add(16917059514421184623, 111)
	for i := 0; i < size; i++ {
		s.Add(uint64(rand.Int63()), uint64(i))
	}

	s.Finish()
	table := s.Search(16754859350651242553)
	t.Logf("result %+v", table)
}

func testAdd(t *testing.T, s Storage, size, queries, d int) {

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
