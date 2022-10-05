package viewservice

import (
	"container/list"
	"net"
)
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	isStartForPrimary bool
	isStartForBackup  bool
	currentViewNumber int
	ackedViewNumber   int

	// Parameters for Primary
	timeWhenPrimaryReceived int64
	currentPrimary          string

	// Parameters for Backup
	timeWhenBackupReceived int64
	currentBackup          string

	// ViewService Tracking Parameters
	pingsMap     map[string]int64
	stateReached bool
	volunteers   list.List
	DEBUG        bool
}

type viewSummary struct {
	deadFlag bool
	server   string
}

func noBackupPresent(vs *ViewServer) bool {
	if vs.currentBackup == "" {
		return true
	}
	return false
}

func noPrimaryPresent(vs *ViewServer) bool {
	if vs.currentPrimary == "" {
		return true
	}
	return false
}

func ackedByPrimary(vs *ViewServer, receivedPingArgs *PingArgs) {
	if vs.currentPrimary == receivedPingArgs.Me {
		vs.ackedViewNumber = int(receivedPingArgs.Viewnum)
	}
	//return false
}
func isPrimary(vs *ViewServer, entity string) bool {
	if vs.currentPrimary == entity {
		return true
	} else {
		return false
	}
}

func isBackup(vs *ViewServer, entity string) bool {
	if vs.currentBackup == entity {
		return true
	} else {
		return false
	}
}

func checkAcknowledgement(vs *ViewServer) bool {
	if vs.currentViewNumber == vs.ackedViewNumber {
		return true
	} else {
		return false
	}
}

func deadPing(timestamp int64, currentTime int64) bool {
	if (timestamp) <= currentTime-DeadPings*int64(PingInterval) {
		return true
	} else {
		return false
	}
}

func assignFirstPrimary(vs *ViewServer, receivedPingArgs *PingArgs) {
	if vs.isStartForPrimary && noPrimaryPresent(vs) {
		if vs.DEBUG {
			fmt.Println("Assigning First Primary")
		}
		vs.isStartForPrimary = false
		vs.timeWhenPrimaryReceived = time.Now().UnixNano()
		vs.currentPrimary = receivedPingArgs.Me
		vs.currentViewNumber = vs.currentViewNumber + 1
	}
}

func assignFirstBackup(vs *ViewServer, receivedPingArgs *PingArgs) {
	if vs.isStartForBackup && noBackupPresent(vs) && vs.currentPrimary != receivedPingArgs.Me {
		if vs.DEBUG {
			fmt.Println("Assigning First Backup")
		}
		vs.isStartForBackup = false
		vs.stateReached = true
		vs.timeWhenBackupReceived = time.Now().UnixNano()
		vs.currentBackup = receivedPingArgs.Me
		vs.currentViewNumber = vs.currentViewNumber + 1
	}
}

func assignPiningServer(vs *ViewServer, receivedPingArgs *PingArgs) {
	if noBackupPresent(vs) {
		if vs.currentPrimary != receivedPingArgs.Me {
			vs.timeWhenBackupReceived = time.Now().UnixNano()
			vs.currentBackup = receivedPingArgs.Me
			vs.currentViewNumber = vs.currentViewNumber + 1
		}
	}
}

func createResponse(vs *ViewServer, reply *PingReply) {
	reply.View.Primary = vs.currentPrimary
	reply.View.Backup = vs.currentBackup
	reply.View.Viewnum = uint(vs.currentViewNumber)

}

func updateBackupIfPossible(vs *ViewServer) {
	if noBackupPresent(vs) {
		for e := vs.volunteers.Front(); e != nil; e = e.Next() {
			volunteerServer := e.Value.(string)
			if volunteerServer != vs.currentPrimary {
				vs.currentBackup = volunteerServer
				if vs.currentViewNumber == vs.ackedViewNumber {
					vs.currentViewNumber = vs.currentViewNumber + 1
				}
			}
		}
	}
}

func handlePrimaryRestart(vs *ViewServer, receivedPingArgs *PingArgs) {
	if vs.currentPrimary == receivedPingArgs.Me && receivedPingArgs.Viewnum == 0 && vs.stateReached {
		if vs.DEBUG {
			fmt.Println("handlePrimaryRestart")
		}
		failedPrimary := vs.currentPrimary
		vs.currentPrimary = vs.currentBackup
		vs.currentBackup = failedPrimary
		vs.currentViewNumber = vs.currentViewNumber + 1
	}
}

func handlePrimaryFailure(vs *ViewServer) {
	vs.currentPrimary = vs.currentBackup
	vs.currentBackup = ""
	vs.currentViewNumber = vs.currentViewNumber + 1
}

func beginningStage(vs *ViewServer, receivedPingArgs *PingArgs) {
	if vs.stateReached == false {
		if vs.isStartForPrimary {
			assignFirstPrimary(vs, receivedPingArgs)
		} else if vs.isStartForBackup {
			assignFirstBackup(vs, receivedPingArgs)
		}
	}
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	currentServer := args.Me
	currentServerPingTime := time.Now().UnixNano()
	vs.pingsMap[currentServer] = currentServerPingTime
	beginningStage(vs, args)
	handlePrimaryRestart(vs, args)
	assignPiningServer(vs, args)
	ackedByPrimary(vs, args)
	createResponse(vs, reply)
	if vs.DEBUG {
		fmt.Printf("ME: %s\n", args.Me)
		fmt.Printf("VIEWNUM: %d\n", args.Viewnum)
		fmt.Printf("CURRENT VIEWNUM: %d\n", vs.currentViewNumber)
		fmt.Printf("ACKED VIEWNUM: %d\n", vs.ackedViewNumber)
		fmt.Printf("REPLY PRIMARY: %s\n", reply.View.Primary)
		fmt.Printf("REPLY BACKUP: %s\n", reply.View.Backup)
		fmt.Printf("REPLY VIEWNUM: %d\n", reply.View.Viewnum)
	}
	defer vs.mu.Unlock()
	return nil
}

// Get server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	reply.View.Primary = vs.currentPrimary
	reply.View.Backup = vs.currentBackup
	reply.View.Viewnum = uint(vs.currentViewNumber)
	if vs.DEBUG {
		fmt.Printf("REPLY PRIMARY: %s\n", reply.View.Primary)
		fmt.Printf("REPLY BACKUP: %s\n", reply.View.Backup)
		fmt.Printf("REPLY VIEWNUM: %d\n", reply.View.Viewnum)
	}
	defer vs.mu.Unlock()
	return nil
}

func lostServer(servername string, timestamp int64, currentTime int64, vs *ViewServer) bool {
	if deadPing(timestamp, currentTime) && checkAcknowledgement(vs) &&
		(servername == vs.currentBackup || servername == vs.currentPrimary) && servername != "" {
		return true
	} else {
		return false
	}
}

func liveAdditionalServer(servername string, timestamp int64, currentTime int64, vs *ViewServer) bool {
	if servername != vs.currentPrimary && servername != vs.currentBackup && deadPing(timestamp, currentTime) == false {
		return true
	}
	return false
}

func getSnapShot(vs *ViewServer) *list.List {
	currentTime := time.Now().UnixNano()
	snapshot := list.New()
	for servername, timestamp := range vs.pingsMap {
		if lostServer(servername, timestamp, currentTime, vs) {
			snapshot.PushFront(viewSummary{deadFlag: true, server: servername})
		}
	}
	return snapshot
}

func getVolunteers(vs *ViewServer) {
	currentTime := time.Now().UnixNano()
	for servername, timestamp := range vs.pingsMap {
		if liveAdditionalServer(servername, timestamp, currentTime, vs) {
			vs.volunteers.PushFront(servername)
		}
	}
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.

func (vs *ViewServer) tick() {
	vs.mu.Lock()
	snapshot := getSnapShot(vs)
	for e := snapshot.Front(); e != nil; e = e.Next() {
		_, entity := e.Value.(viewSummary).deadFlag, e.Value.(viewSummary).server
		if isPrimary(vs, entity) {
			delete(vs.pingsMap, entity)
			handlePrimaryFailure(vs)
		} else if isBackup(vs, entity) {
			delete(vs.pingsMap, entity)
			vs.currentBackup = ""
			vs.currentViewNumber = vs.currentViewNumber + 1
		}
		snapshot.Remove(e)
	}
	getVolunteers(vs)
	updateBackupIfPossible(vs)
	defer vs.mu.Unlock()
}

// tell the server to shut itself down.
// for testing.
// please don't change this function.

func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	//vs.pingsMap = make(map[string]int64)
	vs.isStartForPrimary = true
	vs.isStartForBackup = true
	vs.currentViewNumber = 0
	vs.ackedViewNumber = 0

	// Parameters for Primary
	vs.timeWhenPrimaryReceived = int64(0)
	vs.currentPrimary = ""

	// Parameters for Backup
	vs.timeWhenBackupReceived = int64(0)
	vs.currentBackup = ""

	// Map to update ping timestamps
	vs.pingsMap = make(map[string]int64)
	vs.DEBUG = false
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
