// https://en.wikipedia.org/wiki/Rendezvous_hashing
// https://randorithms.com/2020/12/26/rendezvous-hashing.html
// https://www.snia.org/sites/default/files/SDC15_presentations/dist_sys/Jason_Resch_New_Consistent_Hashings_Rev.pdf
package main

import (
	"flag"
	"fmt"
	"hash/maphash"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
)

var replicationFactor = flag.Int("rf", 1, "replication factor")
var numWrites = flag.Int("numWrites", 1000, "number of writes")
var numReads = flag.Int("numReads", 10000, "number of reads, uniformly random to the site set")
var siteCaps = flag.String("siteCaps", "", "comma separated list of integers, each of which represents a site and its capacity")

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

func main() {
	flag.Parse()

	if *siteCaps == "" {
		fmt.Println("please supply --siteCaps")
		os.Exit(1)
	}

	var sites []*site
	for _, ss := range strings.Split(*siteCaps, ",") {
		c, err := strconv.Atoi(ss)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		sites = append(sites, newSite(c))
	}

	if *replicationFactor > len(sites) {
		fmt.Printf("replication factor %d is greater than num sites (%d)", replicationFactor, len(sites))
		os.Exit(1)
	}

	// Writes.
	unableToWrite := make(map[int]struct{})
	for key := 0; key < *numWrites; key++ {
		sites := hashOrderedSites(sites, key)
		allAvail := true
		for i := 0; i < *replicationFactor; i++ {
			allAvail = allAvail && !sites[i].full()
		}
		if !allAvail {
			unableToWrite[key] = struct{}{}
			continue
		}
		for i := 0; i < *replicationFactor; i++ {
			sites[i].handleWrite(key)
		}
	}

	// Reads.
	for i := 0; i < *numReads; i++ {
		key := rand.Intn(*numWrites)
		if _, ok := unableToWrite[key]; ok {
			continue
		}
		for _, s := range hashOrderedSites(sites, key) {
			if s.handleRead(key) {
				break
			}
		}
	}

	// Print stats.
	for _, s := range sites {
		fmt.Printf("site %d: %d/%d (%.2f%% full)", s.id, len(s.knownKeys), s.capacity, float64(len(s.knownKeys))/float64(s.capacity)*100)
		if *numReads == 0 {
			fmt.Println()
		} else {
			fmt.Printf(". received reads: %d hits (%.2f%% of total), %d misses\n", s.readHits, float64(s.readHits)/float64(*numReads)*100, s.readMisses)
		}
	}
	fmt.Printf("unable to write: %d (%.2f%%)\n", len(unableToWrite), float64(len(unableToWrite))/float64(*numWrites)*100)
}

var seed = maphash.MakeSeed()

func hashOrderedSites(sites []*site, key int) []*site {
	type indexedSite struct {
		*site
		num float64
	}
	var indexedSites []*indexedSite
	for _, s := range sites {
		hashKey := fmt.Sprintf("%d-%d", s.id, key)
		c := float64(maphash.String(seed, hashKey)) / float64(math.MaxUint64)
		checksum := -1 * float64(s.capacity) / math.Log(c)
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
