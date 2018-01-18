package storage

type Store8 struct {
	Store
}

func New8(hashes int, newStore func(hashes int) u64store) *Store8 {
	var s Store8
	s.rhashes = make([]u64store, 90)

	if hashes != 0 {
		s.docids = make(table, 0, hashes)
		for i := range s.rhashes {
			s.rhashes[i] = newStore(hashes)
		}
	}

	return &s
}

// Add inserts a signature and document id into the store
func (s *Store8) Add(sig uint64, docid uint64) {
	t := 0

	var p uint64

	s.docids = append(s.docids, entry{hash: sig, docid: docid})
	s.Count++

	for i := 0; i < 8; i++ {
		p = sig
		s.rhashes[t].add(p)
		t++
		p = (sig & 0xfe0001ffffffffff) | (sig & 0x01f8000000000000 >> 6) | (sig & 0x0007e00000000000 << 6)
		s.rhashes[t].add(p)
		t++
		p = (sig & 0xfe07e07fffffffff) | (sig & 0x01f8000000000000 >> 12) | (sig & 0x00001f8000000000 << 12)
		s.rhashes[t].add(p)
		t++
		p = (sig & 0xfe07ff81ffffffff) | (sig & 0x001f800000000000 >> 18) | (sig & 0x0000007e00000000 << 18)
		s.rhashes[t].add(p)
		t++
		p = (sig & 0xfe07fffe07ffffff) | (sig & 0x001f800000000000 >> 24) | (sig & 0x00000001f8000000 << 24)
		s.rhashes[t].add(p)
		t++
		p = (sig & 0xfe07fffff81fffff) | (sig & 0x001f800000000000 >> 30) | (sig & 0x0000000007e00000 << 30)
		s.rhashes[t].add(p)
		t++
		p = (sig & 0xfe07ffffffe07fff) | (sig & 0x001f800000000000 >> 36) | (sig & 0x00000000001f8000 << 36)
		s.rhashes[t].add(p)
		t++
		p = (sig & 0xfe0fffffffff83ff) | (sig & 0x001f000000000000 >> 42) | (sig & 0x0000000000007c00 << 42)
		s.rhashes[t].add(p)
		t++
		p = (sig & 0xfe0ffffffffffc1f) | (sig & 0x001f000000000000 >> 47) | (sig & 0x00000000000003e0 << 47)
		s.rhashes[t].add(p)
		t++
		p = (sig & 0xfe0fffffffffffe0) | (sig & 0x001f000000000000 >> 52) | (sig & 0x000000000000001f << 52)
		s.rhashes[t].add(p)
		t++
		sig = (sig << 7) | (sig >> (64 - 7))
	}

	p = sig
	s.rhashes[t].add(p)
	t++
	p = (sig & 0xff000fffffffffff) | (sig & 0x00fc000000000000 >> 6) | (sig & 0x0003f00000000000 << 6)
	s.rhashes[t].add(p)
	t++
	p = (sig & 0xff03f03fffffffff) | (sig & 0x00fc000000000000 >> 12) | (sig & 0x00000fc000000000 << 12)
	s.rhashes[t].add(p)
	t++
	p = (sig & 0xf03ffc0fffffffff) | (sig & 0x00fc000000000000 >> 18) | (sig & 0x0000003f00000000 << 18)
	s.rhashes[t].add(p)
	t++
	p = (sig & 0xff03ffff03ffffff) | (sig & 0x00fc000000000000 >> 24) | (sig & 0x00000000fc000000 << 24)
	s.rhashes[t].add(p)
	t++
	p = (sig & 0xff03fffffc0fffff) | (sig & 0x00fc000000000000 >> 30) | (sig & 0x0000000003f00000 << 30)
	s.rhashes[t].add(p)
	t++
	p = (sig & 0xff07ffffff07fff) | (sig & 0x00f8000000000000 >> 36) | (sig & 0x00000000000f8000 << 36)
	s.rhashes[t].add(p)
	t++
	p = (sig & 0xff07fffffff83ff) | (sig & 0x00f8000000000000 >> 41) | (sig & 0x0000000000007c00 << 41)
	s.rhashes[t].add(p)
	t++
	p = (sig & 0xff07ffffffffc0f) | (sig & 0x00f8000000000000 >> 46) | (sig & 0x00000000000003e0 << 46)
	s.rhashes[t].add(p)
	t++
	p = (sig & 0xff07fffffffffe0) | (sig & 0x00f8000000000000 >> 51) | (sig & 0x000000000000001f << 51)
	s.rhashes[t].add(p)
}

func (*Store8) unshuffle(sig uint64, t int) uint64 {

	t7 := t % 10
	shift := 6 * uint64(t7)

	var m2 uint64

	if t < 81 {
		m2 = 0x01f8000000000000

		if t7 >= 7 {
			m2 = 0x001f000000000000
		}
		if t7 >= 8 {
			shift--
		}
	} else {
		m2 = 0x00fc000000000000

		if t7 >= 6 {
			m2 = 0x003f800000000000

			if t7 >= 7 {
				shift--
			}
		}
	}

	m3 := uint64(m2 >> shift)
	m1 := ^uint64(0) &^ (m2 | m3)

	sig = (sig & m1) | (sig & m2 >> shift) | (sig & m3 << shift)
	sig = (sig >> (7 * (uint64(t) / 10))) | (sig << (64 - (7 * (uint64(t) / 10))))
	return sig
}

func (s *Store8) unshuffleList(sigs []uint64, t int) []uint64 {
	for i := range sigs {
		sigs[i] = s.unshuffle(sigs[i], t)
	}

	return sigs
}

const (
	mask8_7_6 = 0xfff8000000000000
	mask8_7_5 = 0xfff0000000000000
	mask8_8_6 = 0xfffc000000000000
	mask8_8_5 = 0xfff8000000000000
)

// Find searches the store for all hashes hamming distance 6 or less from the
// query signature.  It returns the associated list of document ids.
func (s *Store8) Find(sig uint64) []uint64 {

	// empty store
	if s.Count == 0 {
		return nil
	}

	var ids []uint64

	// TODO(dgryski): search in parallel

	t := 0

	var p uint64

	for i := 0; i < 8; i++ {
		p = sig
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe0001ffffffffff) | (sig & 0x01f8000000000000 >> 6) | (sig & 0x0007e00000000000 << 6)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe07e07fffffffff) | (sig & 0x01f8000000000000 >> 12) | (sig & 0x00001f8000000000 << 12)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe07ff81ffffffff) | (sig & 0x001f800000000000 >> 18) | (sig & 0x0000007e00000000 << 18)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe07fffe07ffffff) | (sig & 0x001f800000000000 >> 24) | (sig & 0x00000001f8000000 << 24)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe07fffff81fffff) | (sig & 0x001f800000000000 >> 30) | (sig & 0x0000000007e00000 << 30)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe07ffffffe07fff) | (sig & 0x001f800000000000 >> 36) | (sig & 0x00000000001f8000 << 36)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe0fffffffff83ff) | (sig & 0x001f000000000000 >> 42) | (sig & 0x0000000000007c00 << 42)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_5, 8), t)...)
		t++
		p = (sig & 0xfe0ffffffffffc1f) | (sig & 0x001f000000000000 >> 47) | (sig & 0x00000000000003e0 << 47)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_5, 8), t)...)
		t++
		p = (sig & 0xfe0fffffffffffe0) | (sig & 0x001f000000000000 >> 52) | (sig & 0x000000000000001f << 52)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_5, 8), t)...)
		t++
		sig = (sig << 7) | (sig >> (64 - 7))
	}

	p = sig
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_6, 8), t)...)
	t++
	p = (sig & 0xff000fffffffffff) | (sig & 0x00fc000000000000 >> 6) | (sig & 0x0003f00000000000 << 6)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_6, 8), t)...)
	t++
	p = (sig & 0xff03f03fffffffff) | (sig & 0x00fc000000000000 >> 12) | (sig & 0x00000fc000000000 << 12)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_6, 8), t)...)
	t++
	p = (sig & 0xf03ffc0fffffffff) | (sig & 0x00fc000000000000 >> 18) | (sig & 0x0000003f00000000 << 18)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_6, 8), t)...)
	t++
	p = (sig & 0xff03ffff03ffffff) | (sig & 0x00fc000000000000 >> 24) | (sig & 0x00000000fc000000 << 24)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_6, 8), t)...)
	t++
	p = (sig & 0xff03fffffc0fffff) | (sig & 0x00fc000000000000 >> 30) | (sig & 0x0000000003f00000 << 30)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_6, 8), t)...)
	t++
	p = (sig & 0xff07ffffff07fff) | (sig & 0x00f8000000000000 >> 36) | (sig & 0x00000000000f8000 << 36)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_5, 8), t)...)
	t++
	p = (sig & 0xff07fffffff83ff) | (sig & 0x00f8000000000000 >> 41) | (sig & 0x0000000000007c00 << 41)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_5, 8), t)...)
	t++
	p = (sig & 0xff07ffffffffc0f) | (sig & 0x00f8000000000000 >> 46) | (sig & 0x00000000000003e0 << 46)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_5, 8), t)...)
	t++
	p = (sig & 0xff07fffffffffe0) | (sig & 0x00f8000000000000 >> 51) | (sig & 0x000000000000001f << 51)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_5, 8), t)...)

	ids = unique(ids)

	var docids []uint64
	for _, v := range ids {
		docids = append(docids, s.docids.find(v)...)
	}

	return docids
}

// Find searches the store for all hashes hamming distance 6 or less from the
// query signature.  It returns the associated list of document ids.
func (s *Store8) Search(sig uint64) table {

	// empty store
	if s.Count == 0 {
		return nil
	}

	var ids []uint64

	// TODO(dgryski): search in parallel

	t := 0

	var p uint64

	for i := 0; i < 8; i++ {
		p = sig
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe0001ffffffffff) | (sig & 0x01f8000000000000 >> 6) | (sig & 0x0007e00000000000 << 6)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe07e07fffffffff) | (sig & 0x01f8000000000000 >> 12) | (sig & 0x00001f8000000000 << 12)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe07ff81ffffffff) | (sig & 0x001f800000000000 >> 18) | (sig & 0x0000007e00000000 << 18)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe07fffe07ffffff) | (sig & 0x001f800000000000 >> 24) | (sig & 0x00000001f8000000 << 24)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe07fffff81fffff) | (sig & 0x001f800000000000 >> 30) | (sig & 0x0000000007e00000 << 30)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe07ffffffe07fff) | (sig & 0x001f800000000000 >> 36) | (sig & 0x00000000001f8000 << 36)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_6, 8), t)...)
		t++
		p = (sig & 0xfe0fffffffff83ff) | (sig & 0x001f000000000000 >> 42) | (sig & 0x0000000000007c00 << 42)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_5, 8), t)...)
		t++
		p = (sig & 0xfe0ffffffffffc1f) | (sig & 0x001f000000000000 >> 47) | (sig & 0x00000000000003e0 << 47)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_5, 8), t)...)
		t++
		p = (sig & 0xfe0fffffffffffe0) | (sig & 0x001f000000000000 >> 52) | (sig & 0x000000000000001f << 52)
		ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_7_5, 8), t)...)
		t++
		sig = (sig << 7) | (sig >> (64 - 7))
	}

	p = sig
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_6, 8), t)...)
	t++
	p = (sig & 0xff000fffffffffff) | (sig & 0x00fc000000000000 >> 6) | (sig & 0x0003f00000000000 << 6)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_6, 8), t)...)
	t++
	p = (sig & 0xff03f03fffffffff) | (sig & 0x00fc000000000000 >> 12) | (sig & 0x00000fc000000000 << 12)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_6, 8), t)...)
	t++
	p = (sig & 0xf03ffc0fffffffff) | (sig & 0x00fc000000000000 >> 18) | (sig & 0x0000003f00000000 << 18)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_6, 8), t)...)
	t++
	p = (sig & 0xff03ffff03ffffff) | (sig & 0x00fc000000000000 >> 24) | (sig & 0x00000000fc000000 << 24)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_6, 8), t)...)
	t++
	p = (sig & 0xff03fffffc0fffff) | (sig & 0x00fc000000000000 >> 30) | (sig & 0x0000000003f00000 << 30)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_6, 8), t)...)
	t++
	p = (sig & 0xff07ffffff07fff) | (sig & 0x00f8000000000000 >> 36) | (sig & 0x00000000000f8000 << 36)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_5, 8), t)...)
	t++
	p = (sig & 0xff07fffffff83ff) | (sig & 0x00f8000000000000 >> 41) | (sig & 0x0000000000007c00 << 41)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_5, 8), t)...)
	t++
	p = (sig & 0xff07ffffffffc0f) | (sig & 0x00f8000000000000 >> 46) | (sig & 0x00000000000003e0 << 46)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_5, 8), t)...)
	t++
	p = (sig & 0xff07fffffffffe0) | (sig & 0x00f8000000000000 >> 51) | (sig & 0x000000000000001f << 51)
	ids = append(ids, s.unshuffleList(s.rhashes[t].find(p, mask8_8_5, 8), t)...)

	ids = unique(ids)

	var ts table
	for _, v := range ids {
		ts = append(ts, s.docids.search(v)...)
	}
	return ts
}
