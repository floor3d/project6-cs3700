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


// message will just be a hashmap of strings to strings.
type Message map[string]string

// the data structure for a replica. 
type Replica struct {
  port     int
  id       string
  others []string
  leader   string
  state    string // one of: 'Leader', 'Candidate', 'Follower'
  term     int    // designates which term it is. starts at 1
  dict     map[string]string // the key-value store
  newLeaderMsg chan Message // for tracking message when a new leader is elected
  votes    int // how many votes does this replica have
}

var rng *mrand.Rand // declare random var for ease of use later


// initialize all the necessary properties for the replica, i.e. id, port, etc
func initReplica(r Replica) {

  if len(os.Args) < 4 {
    fmt.Println("Usage: ./3700kvstore <UDP port> <your ID> <ID of second replica> [<ID of third replica> ...]")
    os.Exit(1)
  }


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

  // say hello to the other replicas
  send(r, listen, message)

  electLeader(&r, listen)
  //piazza said you were allowed to start with a certain replica as a leader so that's what we are doing
  

  // will pass heartbeat (appendentries) from leader to replicas
  heartbeat := make(chan Message)
  newLeaderMsg := make(chan Message)
  r.newLeaderMsg = newLeaderMsg
  go ensureLeaderHeartbeat(&r, listen, heartbeat) // check every x ms to ensure leader is sending appendentries

//  fmt.Println("Listening for incoming messages...")

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
      // pass to put handler
      put(&r, listen, str)
    } else if str["type"] == "get" {
      // pass to get handler
      get(r, listen, str)
    } else if str["type"] == "RFV" {
      // is term above ours? if so, vote for them
      num, err := strconv.Atoi(str["term"])
      if num > r.term && err == nil {
        fmt.Println("Received request for vote from candidate " + str["src"])
        voteForNewLeader(&r, listen, newLeaderMsg, str)
      }
    } else if str["type"] == "AppendEntries" {
      // if we are the leader, check if we must become a follower instead.
      // otherwise send this as heartbeat
      if(r.state == "Leader") {
        num, err := strconv.Atoi(str["term"])
        if num > r.term && err == nil {
          r.state = "Follower"
          r.leader = str["src"]
        }

      }
      heartbeat<-str
    } else if str["type"] == "VOTE" {
      // we have been voted for. increment votes and see if we can become leader
      r.votes += 1
      if(r.votes > len(r.others) / 2) {
        r.votes = 1
        r.state = "Leader"
        r.leader = r.id
        sendNewLeaderMessage(r, listen)
      }
    }
  }

}


// start of program. create replica and initialize
func main() {
  r := Replica{}
  r.term = 0
  r.leader = "FFFF"
  r.votes = 1
  initReplica(r)
}


// get handler. if not leader, redirect. if leader, return proper value
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


// put handler. if not leader, redirect. if leader, add to map
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


//sends a message to the necessary destination
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
}


// elects the leader by voting for it or asking to be voted for
func electLeader(r *Replica, conn *net.UDPConn) {
  r.state = "Follower"
  if(r.id == "0000") {
    time.Sleep(750 * time.Millisecond)
    sendRequestForVote(r, conn)
  } else {
    time.Sleep(850 * time.Millisecond)
    voteForCandidate(r, conn, "0000")
  }
//  ch := make(chan string)
//  stop := make(chan bool)
//  timeout := electionTimeout()
//  go listenForLeader(ch, conn, stop)
//  select {
//    case candidate_id := <-ch:
//      fmt.Println("Received request for vote from candidate " + candidate_id)
//      voteForCandidate(r, conn, candidate_id)
//    case <-time.After(time.Duration(timeout) * time.Millisecond):
//      sendRequestForVote(r, conn)
//      stop<-true
//      close(stop)
//  }

}


// listens for someone to ask for a vote then votes for it otherwise becomes candidate 
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
          // received request for vote. vote for it
          ch <- str["src"]
          close(ch)
          return
        }
        if (str["type"] == "VOTE") {
          // hmmm... this is not right. get out of there
          return
        }
			case <-stop:
				return
		}

  }
}


// send request for vote to the other replicas and try to become leader
func sendRequestForVote(r *Replica, conn *net.UDPConn) {
  r.state = "Candidate"
  r.term += 1
  message := make(Message)
  message["src"] = r.id 
  message["dst"] = "FFFF"
  message["leader"] = r.leader
  message["type"] = "RFV"
  message["MID"] = generateRandomID()
  message["term"] = strconv.Itoa(r.term)
  send(*r, conn, message)

  
  // wait for others to send votes back
  timeout := 2 
  start := time.Now()
  elapsed := 0

  buf := make([]byte, 2048)
  for {
    elapsed = int(time.Since(start).Seconds())
    if elapsed >= timeout {
        return
    }
    if(r.votes > len(r.others) / 2) {
      // yay we have enough votes. become leader
      r.votes = 1
      r.state = "Leader"
      r.leader = r.id
      sendNewLeaderMessage(*r, conn)
      return
    }
    n, _, err := conn.ReadFromUDP(buf)
    if err != nil {
        fmt.Printf("Error reading UDP packet: %v\n", err)
        continue
    }
    fmt.Printf("Received message in send request for vote: %s\n", string(buf[:n]))
    str := make(map[string]string)
    json.Unmarshal(buf[:n], &str)
    if str["type"] == "RFV" {
      // is their term higher? if so vote for them
      num, err := strconv.Atoi(str["term"])
      if num > r.term && err == nil {
        fmt.Println("Received request for vote from candidate " + str["src"])
        voteForNewLeader(r, conn, r.newLeaderMsg, str)
        return
      }
    }
    if str["type"] == "VOTE" {
      // yay we got a vote. increment votes and check if we can become leader
      r.votes = r.votes + 1
      if(r.votes > len(r.others) / 2) {
        r.votes = 1
        r.state = "Leader"
        r.leader = r.id
        sendNewLeaderMessage(*r, conn)
        return
      }
    }
    if str["type"] == "AppendEntries" {
      // is their term higher than us? if so become follower immediately
      term, err := strconv.Atoi(str["term"])
      if err == nil {
        r.term = term
      }
      r.state = "Follower"
      r.leader = str["src"]
      return
    }
  }
}


// maker of MID
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


// send msg once elected as leader. tells other replicas to be followers
func sendNewLeaderMessage(r Replica, conn *net.UDPConn) {
  message := make(Message)
  message["src"] = r.id 
  message["dst"] = "FFFF"
  message["leader"] = r.id
  message["type"] = "AppendEntries"
  message["term"] = strconv.Itoa(r.term)
  message["MID"] = generateRandomID()
  send(r, conn, message)
  // send heartbeat as well, ensures that other replicas know we are not dead
  go sendHeartbeat(r, conn, message)

}


// send vote message
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
      // found the leader. become follower mark them as a leader and get back to business
      term, err := strconv.Atoi(str["term"])
      if err == nil {
        r.term = term
      }
      r.leader = str["src"]
      r.state = "Follower"
      return
    }
  }
}


//return random num from 600 to 900
func electionTimeout() int {
  rng = mrand.New(mrand.NewSource(time.Now().Unix()))
  return rng.Intn(301) + 500
}


// as leader, send heartbeat every 300 ms so that the followers know the leader is still alive
func sendHeartbeat(r Replica, conn *net.UDPConn, message Message) {
  for {
    if(r.state != "Leader") {
      return
    }
    time.Sleep(300 * time.Millisecond)
    message["MID"] = generateRandomID()
    send(r, conn, message)
  }
}


// as follower, ensure that you hear from leader every x ms or else leader is dead
func ensureLeaderHeartbeat(r *Replica, conn *net.UDPConn, heartbeat chan Message) {
  timeout := electionTimeout()
  for {
    if(r.state == "Leader") {
      return
    }
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


// leader has given us entries. add it to our log too
func appendEntriesToLog(r *Replica, msg Message) {
  if(r.dict == nil) {
    r.dict = make(map[string]string)
  }
  r.dict[msg["key"]] = msg["value"]
}


//just got some data to put in the db. send the data to our followers too
func sendPutToFollowers(r Replica, conn *net.UDPConn, msg Message) {
  message := make(Message)
  message["src"] = r.id 
  message["dst"] = "FFFF"
  message["leader"] = r.id
  message["type"] = "AppendEntries"
  message["term"] = strconv.Itoa(r.term)
  message["MID"] = generateRandomID()
  message["key"] = msg["key"]
  message["value"] = msg["value"]
  send(r, conn, message)
}


// send out vote for a new leader
func voteForNewLeader(r *Replica, conn *net.UDPConn, newLeaderMsg chan Message, str Message) {
  r.state = "Follower"
  term, err := strconv.Atoi(str["term"])
  if(err != nil) {
    panic("error in atoi")
  }
  r.term = term
  message := make(Message)
  message["src"] = r.id 
  message["dst"] = str["src"]
  message["leader"] = r.leader
  message["type"] = "VOTE"
  message["MID"] = generateRandomID()
  send(*r, conn, message)
  
  timeout := electionTimeout()

  select {
  case <-time.After(time.Duration(timeout) * time.Millisecond):
    // failed. try to become next leader
    sendRequestForVote(r, conn)
    return
  case msg := <-newLeaderMsg:
    // yay, leader said hi
    r.leader = msg["src"]
    return
  }

}
