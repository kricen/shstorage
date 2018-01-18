package storage

import (
	"testing"
	"testing/quick"
)

func TestUnshuffle(t *testing.T) {

	f := func(hash uint64) bool {
		s := New3(1, NewU64Slice)
		s.Add(hash, 0)

		for i := range s.rhashes {
			if got := s.unshuffle((*s.rhashes[i].(*u64slice))[0], i); got != hash {
				t.Errorf("unshuffle(rhashes[%d])=%016x, want %016x\n", i, got, hash)
				return false
			}
		}
		return true
	}

	quick.Check(f, nil)
}

func TestUnshuffle6(t *testing.T) {

	f := func(hash uint64) bool {
		s := New6(1, NewU64Slice)
		s.Add(hash, 0)

		for i := range s.rhashes {
			if got := s.unshuffle((*s.rhashes[i].(*u64slice))[0], i); got != hash {
				t.Errorf("unshuffle(rhashes[%d])=%016x, want %016x\n", i, got, hash)
				return false
			}
		}
		return true
	}

	quick.Check(f, nil)
}

func TestShift(t *testing.T) {
	var sig uint64 = 0xffff000fff000fff
	p := (sig & 0xff80007fffffffff) | (sig & 0x007f800000000000 >> 8) | (sig & 0x00007f8000000000 << 8)
	t.Logf("%0x", sig)
	t.Logf("%0x", p)
	t.Logf("%0x", 100830208800000)

	sig = 0xff80007fffffffff
	t.Logf("%0x", sig&sig>>16)
	t.Logf("%0x", 1<<16)

}
