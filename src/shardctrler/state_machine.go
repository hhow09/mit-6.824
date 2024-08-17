package shardctrler

import (
	"sort"
	"sync"
)

type InMemoryStateMachine struct {
	// Each configuration describes a set of replica groups and an assignment of shards to replica groups.
	configs []Config
	mu      sync.RWMutex
}

const NilGID = 0

func NewInMemoryStateMachine() *InMemoryStateMachine {
	return &InMemoryStateMachine{
		// dummy: Shars has all 0 group id
		configs: []Config{{
			Num:    0,
			Shards: [NShards]int{},
			Groups: map[int][]string{},
		}},
	}
}

// Join(servers) -- add a set of groups (gid -> server-list mapping).
func (sm *InMemoryStateMachine) Join(servers map[int][]string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	prevCfg := sm.configs[len(sm.configs)-1]
	// The shardctrler should react by creating a new configuration that includes the new replica groups.
	// interhit the previous configuration first
	newCfg := dcopy(prevCfg)
	newCfg.Num = len(sm.configs)

	for gid, servers := range servers {
		// new gid
		if gid == NilGID {
			panic("Invalid group id")
		}
		if _, ok := newCfg.Groups[gid]; !ok {
			newServs := make([]string, len(servers))
			copy(newServs, servers)
			newCfg.Groups[gid] = newServs
		}
	}

	// new configuration should divide the shards as evenly as possible among the full set of groups,
	// and should move as few shards as possible to achieve that goal.

	// groupID -> []shardID
	groupShardsMap := groupsShardsMap(newCfg)
	// fmt.Printf("Join.groupShardsMap before balance: %+v\n", groupShardsMap)
	// iteratively move one shard from the group with the most shards to the group with the fewest shards
	for {
		maxGroupID, maxShards := maxShardsGroup(groupShardsMap)
		minGroupID, minShards := minShardsGroup(groupShardsMap)
		if maxGroupID != NilGID && maxShards-minShards <= 1 { // already balanced
			break
		}
		// move one shard from maxGroupID to minGroupID
		groupShardsMap[minGroupID] = append(groupShardsMap[minGroupID], groupShardsMap[maxGroupID][0])
		groupShardsMap[maxGroupID] = groupShardsMap[maxGroupID][1:]
	}
	// fmt.Printf("Join.groupShardsMap after balance: %+v\n", groupShardsMap)
	// shards -> groupID
	var newShards [NShards]int
	for gid, shards := range groupShardsMap {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newCfg.Shards = newShards
	sm.configs = append(sm.configs, newCfg)
}

// Leave(gids) -- delete a set of groups.
func (sm *InMemoryStateMachine) Leave(gids []int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	prevCfg := sm.configs[len(sm.configs)-1]
	newCfg := dcopy(prevCfg)
	newCfg.Num = len(sm.configs)

	// groupID -> []shardID
	groupShardsMap := groupsShardsMap(newCfg)
	// fmt.Printf("Leave.groupShardsMap before balance: %+v\n", groupShardsMap)
	shardsNoGroup := make([]int, 0)
	for _, gid := range gids {
		delete(newCfg.Groups, gid)
		if _, ok := groupShardsMap[gid]; ok {
			shardsNoGroup = append(shardsNoGroup, groupShardsMap[gid]...)
			delete(groupShardsMap, gid)
		}
	}
	// move the shards from the deleted groups to the group with the fewest shards
	var newShards [NShards]int
	// when no group left, just assign empty group and empty shards
	if len(newCfg.Groups) > 0 {
		for _, shard := range shardsNoGroup {
			minGroupID, _ := minShardsGroup(groupShardsMap)
			groupShardsMap[minGroupID] = append(groupShardsMap[minGroupID], shard)
		}
		// fmt.Printf("Leave.groupShardsMap after balance: %+v\n", groupShardsMap)
		for gid, shards := range groupShardsMap {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newCfg.Shards = newShards
	sm.configs = append(sm.configs, newCfg)
}

// Move hand off one shard from current owner to gid.
func (sm *InMemoryStateMachine) Move(gid, shard int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	prevCfg := sm.configs[len(sm.configs)-1]
	newCfg := dcopy(prevCfg)
	newCfg.Num = len(sm.configs)

	newCfg.Shards[shard] = gid
	sm.configs = append(sm.configs, newCfg)
}

// Query fetch Config # num, or latest config if num==-1.
func (sm *InMemoryStateMachine) Query(num int) Config {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if num == -1 || num >= len(sm.configs) {
		return sm.configs[len(sm.configs)-1]
	}
	return sm.configs[num]
}

func dcopy(config Config) Config {
	newCfg := Config{
		Num:    config.Num,
		Shards: config.Shards,
	}
	copied := make(map[int][]string)
	for gid, servers := range config.Groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		copied[gid] = newServers
	}
	newCfg.Groups = copied
	return newCfg
}

func groupsShardsMap(config Config) map[int][]int {
	groupsShardsMap := make(map[int][]int)
	// add new group
	for groupID := range config.Groups {
		groupsShardsMap[groupID] = make([]int, 0)
	}
	for shardID, groupID := range config.Shards {
		groupsShardsMap[groupID] = append(groupsShardsMap[groupID], shardID)
	}
	return groupsShardsMap
}

func minShardsGroup(groupsShardsMap map[int][]int) (int, int) {
	minGroupID, minShards := -1, NShards

	// The code in your state machine that performs the shard rebalancing needs to be deterministic.
	// In Go, map iteration order is not deterministic.
	ids := make([]int, 0, len(groupsShardsMap))
	for groupID := range groupsShardsMap {
		ids = append(ids, groupID)
	}
	sort.Ints(ids)

	for _, groupID := range ids {
		if groupID != NilGID && len(groupsShardsMap[groupID]) < minShards {
			minShards = len(groupsShardsMap[groupID])
			minGroupID = groupID
		}
	}
	return minGroupID, minShards
}

func maxShardsGroup(groupsShardsMap map[int][]int) (int, int) {
	// important, allocate from nil group first
	if len(groupsShardsMap[NilGID]) > 0 {
		return NilGID, len(groupsShardsMap[NilGID])
	}
	// here len(groupsShardsMap[NilGID]) == 0
	// The code in your state machine that performs the shard rebalancing needs to be deterministic.
	// In Go, map iteration order is not deterministic.
	ids := make([]int, 0, len(groupsShardsMap))
	for groupID := range groupsShardsMap {
		ids = append(ids, groupID)
	}
	sort.Ints(ids)

	maxShards, maxGroupID := -1, -1
	for _, groupID := range ids {
		if len(groupsShardsMap[groupID]) > maxShards {
			maxShards = len(groupsShardsMap[groupID])
			maxGroupID = groupID
		}
	}
	return maxGroupID, maxShards
}
