### High-level approach
  Step one: leader election
  --> as a performance boost we opted to make replica 0 become leader automatically
  Step two: heartbeat
  --> leader sends heartbeat, replicas ensure heartbeat is happening or else they attempt to become
      leader
  Step three: handle
  --> for any put or get request, ensure that only the leader actually handles them and the followers
      only redirect
  Step three point five: disperse
  --> for "put", get the value inside of your own db and then send it to the followers. the followers
      will then copy that value in to their own database.
  Step four: stay on the lookout
  --> Nearly all of the difficult functionality in raft centers directly around leader election.
      while listening for gets and puts etc, be sure to also be on the lookout for RFVs because 
      there could be another replica vying for leadership. 

  In reality, the most difficult part of RAFT is to be able to handle the requests as well as leadership
  issues / elections at the same time. That's why we chose Go: it has wonderful concurrency functionality
  built in, unlike python.

### Challenges we faced
  It was difficult to get started with this project because we decided to use Go instead of python.
  thus, we had no starter code, and had to make it from scratch. Also it was annoying to find how
  to do stuff on Go because less people use it than python, so even reading from a socket took us 
  a while to figure out. Another issue we had was with understanding RAFT in the first place. We got
  started early, so we had to make sure we understood stuff like leader election quite well before
  we tried to make it. It took us a while to get our bearings for that. Further, we had trouble
  with timeouts. Sometimes we weren't sure if our timeouts were too long / short so we had to play
  with them a lot to figure out what worked best.

### Features / Design Choices we thought were good
  We tried to keep the code simple and easy to read, and not have too much code per method, so that
  it is clear to any onlookers what each function does. 
  A major aspect / design choice that was very good was simply using Go. It is a language with some
  great concurrency features of which Python cannot hold a candle to. We used some of Go's features
  such as channels and select statements to determine when and how to respond to certain actions,
  like if a certain timer came up before a channel received a value that replica would try to become
  the next leader, etc.
  Putting some of the variables as Replica struct properties. Instead of passing it all around in 
  methods, we occasionally set certain variables to be part of the replica struct, ex. a certain
  channel and the number of votes it had, etc. It made it easier to check and update these across
  methods.

### How we tested the code
  For testing code, we employed two main strategies.
  1. Running it through the simulator. There is no better way to see if the code works than to run
  it in a simulation that checks for correctness. Plus it would tell us how good or bad it did.
  2. print checking. When we realized there was an error in our logic or the simulator was returning
  errors, we always made sure to print the events that were going on, for example printing code before and after
  an if statement to see if one of the replica struct's properties had changed properly. This helped
  tremendously because it identified exactly where we went wrong so we could check over the code 
  that was causing errors.
