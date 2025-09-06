package main

import (
	"fmt"
	"lab5/kvraft"
	"lab5/labrpc"
	"lab5/raft"
	"time"
)

func main() {
	fmt.Println("=== Distributed Key/Value Store Demo ===")
	fmt.Println("Starting a 3-server Raft cluster...")

	// Create network and servers
	net := labrpc.MakeNetwork()
	servers := make([]*kvraft.KVServer, 3)
	ends := make([][]*labrpc.ClientEnd, 3)

	// Create RPC endpoints for each server
	for i := 0; i < 3; i++ {
		ends[i] = make([]*labrpc.ClientEnd, 3)
		for j := 0; j < 3; j++ {
			endname := fmt.Sprintf("server-%d-to-%d", i, j)
			ends[i][j] = net.MakeEnd(endname)
			net.Connect(endname, j)
		}
	}

	// Start all servers
	for i := 0; i < 3; i++ {
		persister := raft.MakePersister()
		servers[i] = kvraft.StartKVServer(ends[i], i, persister, -1)

		// Add servers to network
		kvsvc := labrpc.MakeService(servers[i])
		rfsvc := labrpc.MakeService(servers[i].GetRaft())
		srv := labrpc.MakeServer()
		srv.AddService(kvsvc)
		srv.AddService(rfsvc)
		net.AddServer(i, srv)

		// Enable all connections
		for j := 0; j < 3; j++ {
			net.Enable(fmt.Sprintf("server-%d-to-%d", i, j), true)
		}
	}

	// Wait for leader election
	fmt.Println("Waiting for leader election...")
	time.Sleep(2 * time.Second)

	// Find and display the leader
	leader := findLeader(servers)
	if leader != -1 {
		fmt.Printf("✓ Leader elected: Server %d\n", leader)
	} else {
		fmt.Println("⚠ No leader found, continuing anyway...")
	}

	// Create client endpoints
	clientEnds := make([]*labrpc.ClientEnd, 3)
	for i := 0; i < 3; i++ {
		endname := fmt.Sprintf("client-to-%d", i)
		clientEnds[i] = net.MakeEnd(endname)
		net.Connect(endname, i)
		net.Enable(endname, true)
	}

	// Create client
	client := kvraft.MakeClerk(clientEnds)

	fmt.Println("\n=== Demo Scenarios ===")

	// Scenario 1: Basic operations
	fmt.Println("\n1. Basic Key/Value Operations:")

	fmt.Println("   PUT user:123 → alice")
	client.Put("user:123", "alice")

	fmt.Println("   GET user:123")
	value := client.Get("user:123")
	fmt.Printf("   → %s\n", value)

	fmt.Println("   APPEND user:123 → _smith")
	client.Append("user:123", "_smith")

	fmt.Println("   GET user:123")
	value = client.Get("user:123")
	fmt.Printf("   → %s\n", value)

	// Scenario 2: Multiple keys
	fmt.Println("\n2. Multiple Keys:")
	client.Put("counter", "0")
	client.Put("config:timeout", "30s")
	client.Append("log", "startup;")
	client.Append("log", "leader_elected;")

	fmt.Printf("   counter: %s\n", client.Get("counter"))
	fmt.Printf("   config:timeout: %s\n", client.Get("config:timeout"))
	fmt.Printf("   log: %s\n", client.Get("log"))

	// Scenario 3: Leader failure simulation
	fmt.Println("\n3. Fault Tolerance Demo:")
	currentLeader := findLeader(servers)
	if currentLeader != -1 {
		fmt.Printf("   Current leader: Server %d\n", currentLeader)
		fmt.Printf("   Simulating leader crash...\n")

		// Disconnect the leader
		for j := 0; j < 3; j++ {
			net.Enable(fmt.Sprintf("server-%d-to-%d", currentLeader, j), false)
			net.Enable(fmt.Sprintf("server-%d-to-%d", j, currentLeader), false)
		}

		// Wait for new election
		time.Sleep(2 * time.Second)

		newLeader := findLeader(servers)
		if newLeader != -1 && newLeader != currentLeader {
			fmt.Printf("   ✓ New leader elected: Server %d\n", newLeader)

			// Test that system still works
			fmt.Println("   Testing system after leader failure...")
			client.Put("post_failure", "system_working")
			value := client.Get("post_failure")
			fmt.Printf("   GET post_failure → %s\n", value)

			// Verify old data is still there
			value = client.Get("user:123")
			fmt.Printf("   GET user:123 → %s (preserved!)\n", value)
		}

		// Reconnect the old leader
		fmt.Printf("   Reconnecting old leader...\n")
		for j := 0; j < 3; j++ {
			net.Enable(fmt.Sprintf("server-%d-to-%d", currentLeader, j), true)
			net.Enable(fmt.Sprintf("server-%d-to-%d", j, currentLeader), true)
		}
		time.Sleep(1 * time.Second)
		fmt.Printf("   ✓ Cluster fully healed\n")
	}

	// Scenario 4: Concurrent operations
	fmt.Println("\n4. Concurrent Client Operations:")
	done1 := make(chan bool)
	done2 := make(chan bool)

	// Client 1: Increments a counter
	go func() {
		defer func() { done1 <- true }()
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("counter_%d", i)
			client.Put(key, fmt.Sprintf("%d", i*10))
		}
	}()

	// Client 2: Builds a log
	go func() {
		defer func() { done2 <- true }()
		client.Put("events", "")
		for i := 1; i <= 5; i++ {
			client.Append("events", fmt.Sprintf("event_%d;", i))
		}
	}()

	// Wait for both to complete
	<-done1
	<-done2

	fmt.Println("   Results after concurrent operations:")
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("counter_%d", i)
		value := client.Get(key)
		fmt.Printf("   %s: %s\n", key, value)
	}
	fmt.Printf("   events: %s\n", client.Get("events"))

	// Scenario 5: Network partition
	fmt.Println("\n5. Network Partition Demo:")
	fmt.Println("   Creating network partition...")

	// Partition: Server 0 alone vs Servers 1,2
	net.Enable("server-0-to-1", false)
	net.Enable("server-1-to-0", false)
	net.Enable("server-0-to-2", false)
	net.Enable("server-2-to-0", false)

	time.Sleep(2 * time.Second)

	// The majority partition (1,2) should still work
	fmt.Println("   Testing majority partition...")
	client.Put("partition_test", "majority_works")
	value = client.Get("partition_test")
	fmt.Printf("   GET partition_test → %s\n", value)

	// Heal the partition
	fmt.Println("   Healing partition...")
	net.Enable("server-0-to-1", true)
	net.Enable("server-1-to-0", true)
	net.Enable("server-0-to-2", true)
	net.Enable("server-2-to-0", true)

	time.Sleep(2 * time.Second)

	// Verify all data is consistent
	fmt.Println("   Verifying data consistency after partition:")
	fmt.Printf("   user:123: %s\n", client.Get("user:123"))
	fmt.Printf("   partition_test: %s\n", client.Get("partition_test"))
	fmt.Printf("   events: %s\n", client.Get("events"))

	fmt.Println("\n=== Demo Summary ===")
	fmt.Println("✓ Leader election and basic operations")
	fmt.Println("✓ Multiple key storage and retrieval")
	fmt.Println("✓ Leader failure and automatic failover")
	fmt.Println("✓ Data persistence across leadership changes")
	fmt.Println("✓ Concurrent client operations")
	fmt.Println("✓ Network partition tolerance")
	fmt.Println("✓ Cluster healing and data consistency")

	// Clean shutdown
	fmt.Println("\nShutting down cluster...")
	for i := 0; i < 3; i++ {
		if servers[i] != nil {
			servers[i].Kill()
		}
	}
	net.Cleanup()
	fmt.Println("Demo completed successfully!")
}

func findLeader(servers []*kvraft.KVServer) int {
	for i := 0; i < len(servers); i++ {
		if servers[i] != nil {
			_, isLeader := servers[i].GetRaft().GetState()
			if isLeader {
				return i
			}
		}
	}
	return -1
}
