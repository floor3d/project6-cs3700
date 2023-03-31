package main

import (
    "fmt"
    "net"
    "os"
    "strconv"
    "encoding/json"
    "encoding/hex"
    "time"
    "crypto/rand"
    mrand "math/rand"
//    "strings"
)


//type Message struct {
//    src int
//    dst string
//    leader string
//    type string
//}
type Message map[string]string

type Replica struct {
  port     int
  id       string
  others []string
  leader   string
  state    string // one of: 'Leader', 'Candidate', 'Follower'
  term     int    // designates which term it is. starts at 1
  dict     map[string]string // the key-value store
}

var rng *mrand.Rand

func initReplica(r Replica) {

  if len(os.Args) < 4 {
    fmt.Println("Usage: ./3700kvstore <UDP port> <your ID> <ID of second replica> [<ID of third replica> ...]")
    os.Exit(1)
  }

//  fmt.Println(os.Args)

  port, err := strconv.Atoi(os.Args[1])
  if err != nil {
    fmt.Println("Invalid port number:", os.Args[1])
    os.Exit(1)
  }

  id := os.Args[2]

  r.others = make([]string, len(os.Args)-3)
  for i := 3; i < len(os.Args); i++ {
    other_id := os.Args[i]
    r.others[i-3] = other_id
  }
  
  r.port = port
  r.id = id

//  fmt.Println("Port: " + strconv.Itoa(r.port) + " || Id: " + r.id + " || Others: " + strings.Join(r.others, ","))

  // Create a hello message
  message := make(Message)
  message["src"] = r.id 
  message["dst"] = "FFFF"
  message["leader"] = r.leader
  message["type"] = "hello"

  // Open a UDP socket to listen for incoming messages on port
  addr, err := net.ResolveUDPAddr("udp", "0.0.0.0:0") 
  if err != nil {
    fmt.Printf("Error resolving UDP address: %v\n", err)
    return
  }

  listen, err := net.ListenUDP("udp", addr)
  if err != nil {
    fmt.Printf("Error listening on UDP socket: %v\n", err) 
    return
  }
  defer listen.Close()


  send(r, listen, message)

  //TODO -- ensure that this happens in a race with a timeout. if it doesnt happen in x seconds then retry that bitch
  
  electLeader(&r, listen)

  heartbeat := make(chan Message)

  go ensureLeaderHeartbeat(&r, listen, heartbeat)

  fmt.Println("Listening for incoming messages...")

  // Wait for incoming messages and print them to the console
  buf := make([]byte, 2048)
  for {
    n, _, err := listen.ReadFromUDP(buf)
    if err != nil {
        fmt.Printf("Error reading UDP packet: %v\n", err)
        continue
    }
    fmt.Printf("Received message: %s\n", string(buf[:n]))
    str := make(map[string]string)
    json.Unmarshal(buf[:n], &str)
    if str["type"] == "put" {
      put(&r, listen, str)
    } else if str["type"] == "get" {
      get(r, listen, str)
    } else if str["type"] == "AppendEntries" {
      heartbeat<-str
    }
  }

}


func main() {
  r := Replica{}
  r.term = 0
  r.leader = "FFFF"
  initReplica(r)
}


func get(r Replica, conn *net.UDPConn, req map[string]string) {
  message := make(Message)
  message["src"] = r.id 
  message["dst"] = req["src"]
  message["leader"] = r.leader
  message["type"] = "ok"
  message["MID"] = req["MID"]
  if(r.leader != "FFFF" && r.state != "Leader") {
    // send redirect
    message["type"] = "redirect"
  } else {
    message["value"] = r.dict[req["key"]]
  }
  send(r, conn, message)
}


func put(r *Replica, conn *net.UDPConn, req map[string]string) {
  if(r.state == "Leader") {
    if(r.dict == nil) {
      r.dict = make(map[string]string)
    }
    r.dict[req["key"]] = req["value"]
    sendPutToFollowers(*r, conn, req)
  }
  message := make(Message)
  message["src"] = r.id 
  message["dst"] = req["src"]
  message["leader"] = r.leader
  message["type"] = "ok"
  message["MID"] = req["MID"]
  if(r.leader != "FFFF" && r.state != "Leader") {
    // send redirect
    message["type"] = "redirect"
  }
  send(*r, conn, message)
}


func send(r Replica, conn *net.UDPConn, message map[string]string) {
  // Marshal the JSON object into a byte array
  data, err := json.Marshal(message)
  if err != nil {
      panic(err)
  }
  addr, err := net.ResolveUDPAddr("udp", "localhost:" + strconv.Itoa(r.port))
  // Write the byte array to the UDP connection
  n, err := conn.WriteToUDP(data, addr)
  if err != nil {
      panic(err)
  }
  fmt.Printf("Sent %d bytes to %s at %s\n", n, addr.String(), message["dst"])
//  fmt.Println("***********************************")
//  fmt.Println(message)
//  fmt.Println("***********************************")
}


func electLeader(r *Replica, conn *net.UDPConn/*, done chan bool, stop chan bool*/) {
  r.state = "Follower"
  ch := make(chan string)
  stop := make(chan bool)
  timeout := electionTimeout()
  go listenForLeader(ch, conn, stop)
  select {
    case candidate_id := <-ch:
      fmt.Println("Received request for vote from candidate " + candidate_id)
      voteForCandidate(r, conn, candidate_id)
    case <-time.After(time.Duration(timeout) * time.Millisecond):
      r.state = "Candidate"
      sendRequestForVote(r, conn)
//      stop<-true
      close(stop)
  }

}


func listenForLeader(ch chan string, conn *net.UDPConn, stop chan bool) {
  // wait for someone to ask for vote
  buf := make([]byte, 2048)
  for {
    select {
			default:
        n, _, err := conn.ReadFromUDP(buf)
        if err != nil {
            fmt.Printf("Error reading UDP packet: %v\n", err)
            continue
        }
        fmt.Printf("Received message in listen for leader: %s\n", string(buf[:n]))
        str := make(map[string]string)
        json.Unmarshal(buf[:n], &str)
        if (str["type"] == "RFV") {
          ch <- str["src"]
          close(ch)
          return
        }
        if (str["type"] == "VOTE") {
          return
        }
			case <-stop:
				return
		}

  }
}



func sendRequestForVote(r *Replica, conn *net.UDPConn) {
  r.term += 1
  message := make(Message)
//  for index, value := range r.others {
  message["src"] = r.id 
  message["dst"] = "FFFF"
  message["leader"] = r.leader
  message["type"] = "RFV"
  message["MID"] = generateRandomID()
  message["term"] = strconv.Itoa(r.term)
  send(*r, conn, message)
//  }

  numVotes := 1 // vote for self

  // wait for others to send votes back

  buf := make([]byte, 2048)
  for {
    n, _, err := conn.ReadFromUDP(buf)
    if err != nil {
        fmt.Printf("Error reading UDP packet: %v\n", err)
        continue
    }
    fmt.Printf("Received message in send request for vote: %s\n", string(buf[:n]))
    str := make(map[string]string)
    json.Unmarshal(buf[:n], &str)
    if str["type"] == "VOTE" {
      numVotes = numVotes + 1
      if(numVotes > len(r.others) / 2) {
        r.state = "Leader"
        r.leader = r.id
        sendNewLeaderMessage(*r, conn)
        return
      }
    }

  }

}


func generateRandomID() string {
    // Generate 16 random bytes
    bytes := make([]byte, 16)
    _, err := rand.Read(bytes)
    if err != nil {
        panic(err)
    }

    // Convert the bytes to a hexadecimal string
    id := hex.EncodeToString(bytes)

    return id
}

func sendNewLeaderMessage(r Replica, conn *net.UDPConn) {
  message := make(Message)
  message["src"] = r.id 
  message["dst"] = "FFFF"
  message["leader"] = r.id
  message["type"] = "AppendEntries"
  message["MID"] = generateRandomID()
  send(r, conn, message)
  go sendHeartbeat(r, conn, message)

}

func voteForCandidate(r *Replica, conn *net.UDPConn, candidate_id string) {
  message := make(Message)
  message["src"] = r.id 
  message["dst"] = candidate_id
  message["leader"] = r.leader
  message["type"] = "VOTE"
  message["MID"] = generateRandomID()
  send(*r, conn, message)

  buf := make([]byte, 2048)
  for {
    n, _, err := conn.ReadFromUDP(buf)
    if err != nil {
        fmt.Printf("Error reading UDP packet: %v\n", err)
        continue
    }
    fmt.Printf("Received message in vote for candidate: %s\n", string(buf[:n]))
    str := make(map[string]string)
    json.Unmarshal(buf[:n], &str)
    if str["type"] == "AppendEntries" {
      r.leader = str["src"]
      r.state = "Follower"
      return
    }
  }

}

//return random num from 200 to 500
func electionTimeout() int {
  rng = mrand.New(mrand.NewSource(time.Now().Unix()))
  return rng.Intn(301) + 200
}


// as leader, send heartbeat every 300 ms so that the followers know the leader is still alive
func sendHeartbeat(r Replica, conn *net.UDPConn, message Message, necessary chan bool) {
  for {
    time.Sleep(300 * time.Millisecond)
    message["MID"] = generateRandomID()
    send(r, conn, message)
  }
}


// as follower, ensure that you hear from leader every x ms or else leader is dead
func ensureLeaderHeartbeat(r *Replica, conn *net.UDPConn, heartbeat chan Message) {
  if(r.state == "Leader") {
    return
  }
  timeout := electionTimeout()
  for {
    select {
      case <-time.After(time.Duration(timeout + 250) * time.Millisecond):
        // uh oh. leader has died. begin new election by incrementing term and requesting vote
        sendRequestForVote(r, conn)
        return
      case msg := <-heartbeat:
        appendEntriesToLog(r, msg)
    }
  }
}

func appendEntriesToLog(r *Replica, msg Message) {
  if(r.dict == nil) {
    r.dict = make(map[string]string)
  }
  r.dict[msg["key"]] = msg["value"]
}

func sendPutToFollowers(r Replica, conn *net.UDPConn, msg Message) {
  message := make(Message)
  message["src"] = r.id 
  message["dst"] = "FFFF"
  message["leader"] = r.id
  message["type"] = "AppendEntries"
  message["MID"] = generateRandomID()
  message["key"] = msg["key"]
  message["value"] = msg["value"]
  send(r, conn, message)
}

