// https://en.wikipedia.org/wiki/Rendezvous_hashing
// https://randorithms.com/2020/12/26/rendezvous-hashing.html
// https://www.snia.org/sites/default/files/SDC15_presentations/dist_sys/Jason_Resch_New_Consistent_Hashings_Rev.pdf
package main

import (
	"fmt"
	"hash/maphash"
	"math"
	"math/rand"
	"os"
	"sort"
)

var siteCounter int

type site struct {
	id         int
	capacity   int
	knownKeys  map[int]struct{}
	readHits   int
	readMisses int
}

func newSite(capacity int) *site {
	siteCounter++
	return &site{id: siteCounter, capacity: capacity, knownKeys: make(map[int]struct{})}
}

func (s *site) full() bool {
	return len(s.knownKeys) >= s.capacity
}

func (s *site) handleWrite(key int) {
	s.knownKeys[key] = struct{}{}
}

func (s *site) handleRead(key int) bool {
	if _, ok := s.knownKeys[key]; ok {
		s.readHits++
		return true
	}
	s.readMisses++
	return false
}

var sites = []*site{newSite(20000), newSite(10000), newSite(10000), newSite(10000)}

const numWrites = 1000
const numReads = 100000 // Number of reads, uniformly random in the write set.
const replicationFactor = 2

func main() {
	if replicationFactor > len(sites) {
		fmt.Printf("replication factor %d is greater than num sites (%d)", replicationFactor, len(sites))
		os.Exit(1)
	}

	unableToWrite := make(map[int]struct{})
	for key := 0; key < numWrites; key++ {
		sites := hashOrderedSites(key)
		allAvail := true
		for i := 0; i < replicationFactor; i++ {
			allAvail = allAvail && !sites[i].full()
		}
		if !allAvail {
			unableToWrite[key] = struct{}{}
			continue
		}
		for i := 0; i < replicationFactor; i++ {
			sites[i].handleWrite(key)
		}
	}

	for i := 0; i < numReads; i++ {
		key := rand.Intn(numWrites)
		if _, ok := unableToWrite[key]; ok {
			continue
		}
		for _, s := range hashOrderedSites(key) {
			if s.handleRead(key) {
				break
			}
		}
	}

	for _, s := range sites {
		fmt.Printf("site %d: %d/%d (%.2f%% full). received reads: %d hits (%.2f%% of total), %d misses\n", s.id, len(s.knownKeys), s.capacity, float64(len(s.knownKeys))/float64(s.capacity)*100, s.readHits, float64(s.readHits)/float64(numReads)*100, s.readMisses)
	}
	fmt.Printf("unable to write: %d (%.2f%%)\n", len(unableToWrite), float64(len(unableToWrite))/float64(numWrites)*100)
}

var seed = maphash.MakeSeed()

func hashOrderedSites(key int) []*site {
	type indexedSite struct {
		*site
		num float64
	}
	var indexedSites []*indexedSite
	for _, s := range sites {
		hashKey := fmt.Sprintf("%d-%d", s.id, key)
		c := maphash.String(seed, hashKey)
		checksum := float64(s.capacity) / -1 * math.Log(float64(c)/math.MaxUint64)
		indexedSites = append(indexedSites, &indexedSite{site: s, num: checksum})
	}
	sort.Slice(indexedSites, func(i, j int) bool {
		return indexedSites[i].num > indexedSites[j].num
	})
	var ordered []*site
	for _, s := range indexedSites {
		ordered = append(ordered, s.site)
	}
	return ordered
}
