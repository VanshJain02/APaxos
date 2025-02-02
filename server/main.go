package main

// Code From Vansh Jain
// DS Lab 1
// SBU ID - 116713519

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
	pb "project1/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transaction struct {
	TimeStamp int64
	Sender    string
	Receiver  string
	Amount    float32
}
type Block struct {
	block_id    int
	Transaction []Transaction
}
type server struct {
	pb.UnimplementedBankingServiceServer
	id                       int
	name                     string
	addr                     string
	transactions             []Transaction
	log                      []Transaction
	datastore                []Block
	ballot_num               int64
	last_commited_ballot_num int64
	acceptNum                int64
	acceptVal                []Transaction
	Clientbalance            float32
	servers                  [5]grpc.ClientConnInterface
	serverStatus             [5]int64
	totalLatency             time.Duration
	totalTxns                int
}

func toProtoTransaction(txn Transaction) *pb.Transaction {
	return &pb.Transaction{
		Timestamp: txn.TimeStamp,
		Sender:    txn.Sender,
		Receiver:  txn.Receiver,
		Amount:    float32(txn.Amount),
	}
}

func toProtoTransactions(txns []Transaction) []*pb.Transaction {
	var protoTxns []*pb.Transaction
	for _, txn := range txns {
		protoTxns = append(protoTxns, toProtoTransaction(txn))
	}
	return protoTxns
}
func fromProtoStarTransactions(protoTxns []*pb.Transaction) []Transaction {
	var txns []Transaction
	for _, protoTxn := range protoTxns {
		txns = append(txns, Transaction{
			TimeStamp: protoTxn.Timestamp,
			Sender:    protoTxn.Sender,
			Receiver:  protoTxn.Receiver,
			Amount:    protoTxn.Amount,
		})
	}
	return txns
}



func (s *server) Catchup(ctx context.Context, in *pb.CatchupMessage) (*pb.SyncedResponse, error) {

	log.Print("Contacted by Server to Catchup: U r Leader")

	log.Print("Sync Success")

	var datastore []*pb.Block
	for _, block := range s.datastore {

		pbBlock := &pb.Block{
			BlockId: int64(block.block_id),
			Mb:      toProtoTransactions(block.Transaction),
		}
		datastore = append(datastore, pbBlock)
	}
	return &pb.SyncedResponse{IsSynced: "yes", Datastore: datastore}, nil
}
 
func (s *server) StartSyncing(highest_commit int, highest_commit_id int) int64 {

	if highest_commit != 0 {
		ctx1, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		data, _ := pb.NewBankingServiceClient(s.servers[highest_commit_id]).Catchup(ctx1, &pb.CatchupMessage{})
		convertedDatastore := make([]Block, len(data.GetDatastore()))
		for i, block := range data.GetDatastore() {
			convertedDatastore[i] = Block{
				block_id:    int(block.BlockId),
				Transaction: fromProtoStarTransactions(block.Mb),
			}
			if i >= int(s.last_commited_ballot_num) {
				s.addTransactionToDatastore(fromProtoStarTransactions(block.Mb), false)
			}
		}
		s.datastore = convertedDatastore
		s.last_commited_ballot_num = int64(highest_commit)

		return 1
	}
	return 0
}
func (s *server) Prepare(ctx context.Context, in *pb.PrepareMessage) (*pb.PromiseMessage, error) {
	// log.Print("Server Prepare Started")
	if in.ServerId == int64(s.id) {
		return &pb.PromiseMessage{}, nil
	}
	// var choice string
	syncCheck := 0
	// log.Print("Send Promise Message (yes/no)")
	// fmt.Scan(&choice)
	// if choice == "yes" {
	if in.LastCommitedBallotNum != s.last_commited_ballot_num {
		if in.LastCommitedBallotNum > s.last_commited_ballot_num {
			log.Print("Inconsistency Found in Commit")
			syncCheck = int(s.StartSyncing(int(in.LastCommitedBallotNum),int(in.ServerId)))
		}
		if in.LastCommitedBallotNum < s.last_commited_ballot_num {
			promise := pb.PromiseMessage{
				N:                 s.ballot_num,
				AcceptNum:         s.acceptNum,
				LeaderSyncBallotNum:     s.last_commited_ballot_num,
				AcceptVal:         toProtoTransactions(s.acceptVal),
				LocalTransactions: toProtoTransactions(s.log),
			}
			log.Printf("Server %s: Promise Sent-Leader Synchronisation Required",s.name)
			return &promise, nil
		}
	} else {
		syncCheck = 1
	}
	if syncCheck != 0 {

		if in.BallotNum > int64(s.ballot_num) {
			s.ballot_num = in.BallotNum
			promise := pb.PromiseMessage{
				N:                 s.ballot_num,
				AcceptNum:         s.acceptNum,
				LeaderSyncBallotNum:    0,
				AcceptVal:         toProtoTransactions(s.acceptVal),
				LocalTransactions: toProtoTransactions(s.log),
			}
			log.Printf("Server %s: Promise Sent",s.name)
			return &promise, nil
		} else{
			log.Printf("Server %s: Promise Rejected",s.name)
		}
	}
	// }
	return &pb.PromiseMessage{}, nil
}

func (s *server) Accept(ctx context.Context, in *pb.AcceptMessage) (*pb.AcceptedMessage, error) {

	// var choice string
	// log.Print("Take Accept Message (yes/no)")
	// fmt.Scan(&choice)
	// if choice == "yes" {
		if in.N > s.acceptNum {
			// var choice string
			s.acceptNum = in.N
			s.acceptVal = fromProtoStarTransactions(in.MajorBlock)
			// log.Print("Send Accepted Message (yes/no)")
			// fmt.Scan(&choice)
			// if choice == "yes" {
			log.Printf("Server %s: Accepted Msg Sent",s.name)

			return &pb.AcceptedMessage{N: in.N, MajorBlock: in.MajorBlock}, nil
			// }
		}
	// }
	return &pb.AcceptedMessage{}, nil
}

func (s *server) Decide(ctx context.Context, in *pb.CommitMessage) (*pb.Empty, error) {
	log.Print("Commiting Transactions......")
	s.addTransactionToDatastore(s.acceptVal, true)
	s.last_commited_ballot_num = int64(len(s.datastore))
	s.acceptNum = 0
	s.acceptVal = []Transaction{}
	// log.Printf("Last Commited Ballot Num for Server %s: %d", s.name, s.last_commited_ballot_num)
		
	return &pb.Empty{}, nil
}

func (s *server) SyncServerStatus(ctx context.Context, in *pb.ServerStatus) (*pb.Empty, error) {
	s.serverStatus = [5]int64(in.ServerStatus)
	return &pb.Empty{}, nil
}

func (s *server) StartLeaderElection() (bool, float32) {
	start := time.Now() 
	timeForRpcContext:= 100*time.Millisecond

	for _, i := range(s.servers){
		if(i==nil){
			continue
		}
		ctx1, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond) 
		defer cancel()
		if(len(s.serverStatus)>0){
			s,_ :=pb.NewBankingServiceClient(i).SyncServerStatus(ctx1,&pb.ServerStatus{ServerStatus: s.serverStatus[:]})
			fmt.Print(s)		
		}
		
	}

	s.ballot_num++
	// log.Printf("Ballot Num Updated: %d", s.ballot_num)
	// log.Print("Log of Server: ", s.log)

	var promise_messages []*pb.PromiseMessage
	leaderSyncedCheck := false

	promiseMessagesChan := make(chan *pb.PromiseMessage, 5)
	isPromiseChannelOpen := true
	acceptMessagesChan := make(chan *pb.AcceptedMessage, 5)
	isAcceptChannelOpen := true
	var accepted_messages []*pb.AcceptedMessage
	acceptValCheck := 0

	majority := len(s.servers)/2 + 1 
	var wg_count sync.WaitGroup      
	var wg_count_Conter = majority
	count_promise := 0
	failed_promise := 0
	received_promisemsgs := 0
	highest_commit :=s.last_commited_ballot_num
	highest_commit_id := s.id
	sendPrepare := func(id int, srv pb.BankingServiceClient) {
		prepareMessage := pb.PrepareMessage{
			BallotNum:             s.ballot_num,
			LastCommitedBallotNum: s.last_commited_ballot_num,
			ServerId:              int64(s.id),
		}

		ctx1, cancel := context.WithTimeout(context.Background(), timeForRpcContext) // Timeout for gRPC call
		defer cancel()
		promiseMsg := &pb.PromiseMessage{}
		if s.serverStatus[id] == 1 {
			var err error
			promiseMsg, err = srv.Prepare(ctx1, &prepareMessage)
			if err != nil {
				log.Printf("Error sending prepare message to server %d: %v", id, err)
			}
		}

		if promiseMsg != nil {

			received_promisemsgs++
			if promiseMsg.N > 0 {
				if promiseMsg.LeaderSyncBallotNum>highest_commit{
					// received_promisemsgs--
					highest_commit = promiseMsg.LeaderSyncBallotNum
					highest_commit_id = id
					leaderSyncedCheck = true
					// remaining := wg_count_Conter
					// if remaining > 0 {
					// 	for i := 0; i < remaining; i++ {
					// 		wg_count.Done() // Decrement remaining counter to 0
					// 		wg_count_Conter--
					// 	}
					// }
					// return
				}
				count_promise += 1
				// log.Print("Valid Promise Message Receieved from Server: ", id)
				if count_promise < majority && wg_count_Conter > 0 {
					wg_count.Done()
					wg_count_Conter--
				}
			} else{
				// log.Print("Invalid Promise Message Receieved from Server: ", id)
			}
		} else {
			failed_promise++
			if (count_promise+failed_promise) < majority && (wg_count_Conter > 0) {
				wg_count.Done()
				wg_count_Conter--
			}
		}

		if received_promisemsgs == len(s.servers) && wg_count_Conter > 0 {
			for i := 0; i < wg_count_Conter; i++ {
				wg_count.Done()
			}
			wg_count_Conter=0
		}
		if isPromiseChannelOpen {
			promiseMessagesChan <- promiseMsg
		}
	}
	wg_count.Add(majority)
	wg_count_Conter = majority
	for id, srv := range s.servers {
		if srv == nil {
			promise := pb.PromiseMessage{
				N:                 s.ballot_num,
				AcceptNum:         s.acceptNum,
				AcceptVal:         toProtoTransactions(s.acceptVal),
				LocalTransactions: toProtoTransactions(s.log),
			}
			promise_messages = append(promise_messages, &promise)
			received_promisemsgs++
			wg_count.Done()
			wg_count_Conter--
		}
		if srv != nil {
			go sendPrepare(id, pb.NewBankingServiceClient(srv)) 
		}

	}
	log.Print("All Prepare Msgs Sent")

	wg_count.Wait()
	if leaderSyncedCheck {
		log.Print("ATTENTION: Leader Going Under Synchronisation")
		s.StartSyncing(int(highest_commit),highest_commit_id)
		return leaderSyncedCheck, float32(s.Clientbalance)
	}

	highest_accept_num := 0
	highest_accepted_Val := []Transaction{}

	if count_promise >= 2 {
		// log.Print("Majority Promise Messages Receieved")
		// log.Print("Timer Started: For Remaining Possible Messages")
		timer := time.NewTimer(timeForRpcContext/5)
		defer timer.Stop()

		<-timer.C
		// log.Print("Timer Expired. Proceeding with the major block.")

		isPromiseChannelOpen = false
		close(promiseMessagesChan)
		
		idx := 0

		for promiseMsg := range promiseMessagesChan {
			if promiseMsg != nil {
					if promiseMsg.AcceptNum != 0 {
						if(promiseMsg.AcceptNum>int64(highest_accept_num)){
							highest_accept_num = int(promiseMsg.AcceptNum)
							highest_accepted_Val = fromProtoStarTransactions(promiseMsg.AcceptVal)
							acceptValCheck = int(promiseMsg.GetAcceptNum())
							leaderSyncedCheck = true
						}
					}
					if promiseMsg.N == s.ballot_num {
						promise_messages = append(promise_messages, promiseMsg)
					}
			}
			idx++
		}
	}

	var majorBlock []Transaction
	if len(promise_messages) > len(s.servers)/2 {
		log.Print("REACHED CONSENSUS: Majority Promise Msgs Receieved")

		//Preparing Major Block
		if acceptValCheck != 0 {
			majorBlock = highest_accepted_Val
			log.Print("Proposing Previous AcceptedVal")
		} else {
			for _, promise := range promise_messages {
				if promise == nil {
					continue
				}
				for _, i := range promise.LocalTransactions {
					majorBlock = append(majorBlock, Transaction{TimeStamp: i.Timestamp, Sender: i.Sender, Receiver: i.Receiver, Amount: i.Amount})
				}
			}
		}

		count_accept := 0
		failed_accept := 0
		received_acceptmsgs :=1
		var wg_count sync.WaitGroup
		wg_count_Conter = majority

		sendAccept := func(id int, srv pb.BankingServiceClient) {
			AcceptMessage := pb.AcceptMessage{
				N:          s.ballot_num,
				MajorBlock: toProtoTransactions(majorBlock),
			}
			ctx1, cancel := context.WithTimeout(context.Background(), timeForRpcContext) 
			defer cancel()
			acceptMsg, err := srv.Accept(ctx1, &AcceptMessage)
			if err != nil {
				log.Printf("Error sending accept message to server %d: %v", id, err)
			}
			received_acceptmsgs++

			if acceptMsg != nil {
				if acceptMsg.N > 0 {
					// log.Print("Valid Accept Message Receieved from Server: ", id)
					count_accept += 1
					if count_accept < 3 && wg_count_Conter>0{
						wg_count.Done()
						wg_count_Conter--
					}
				}
			} else {
				failed_accept++
				if (count_accept + failed_accept) < 3 && (wg_count_Conter>0){
					wg_count.Done()
					wg_count_Conter--
				}
			}

			if(received_acceptmsgs>=len(s.servers) && (wg_count_Conter>0)){
				for i:=0;i<wg_count_Conter;i++{
					wg_count.Done()
				}
				wg_count_Conter=0
			}
			if(isAcceptChannelOpen){
				acceptMessagesChan <- acceptMsg
			}
		}
		wg_count.Add(majority)
		wg_count_Conter = majority
		for id, srv := range s.servers {
			if srv == nil {
				AcceptedMessage := pb.AcceptedMessage{
					N:          s.ballot_num,
					MajorBlock: toProtoTransactions(majorBlock),
				}
				s.acceptNum = s.ballot_num
				s.acceptVal = majorBlock
				accepted_messages = append(accepted_messages, &AcceptedMessage)
				wg_count.Done()
				wg_count_Conter--
			}
			if s.serverStatus[id] == 1 && srv != nil {
				go sendAccept(id, pb.NewBankingServiceClient(srv)) 
			}
		}
		log.Print("All Accept Msgs Sent")

		wg_count.Wait()
		if(received_acceptmsgs>=len(s.servers)){
			// log.Print("Closing Accept Channel")
			isAcceptChannelOpen=false
			close(acceptMessagesChan)
		}
		for acceptMsg := range acceptMessagesChan {
			if acceptMsg != nil {
				if acceptMsg.N != 0 {
					accepted_messages = append(accepted_messages, acceptMsg)
				}
			}
			if len(accepted_messages) >= majority {
				break
			}
		}
	} else {
		log.Printf("COULDNT ACHIEVE CONSENSUS! VOTING FAILED")
		return leaderSyncedCheck, float32(s.Clientbalance)
	}

	if len(accepted_messages) > len(s.servers)/2 {

		sendDecide := func(id int, srv grpc.ClientConnInterface) {
			commit := pb.CommitMessage{N: s.ballot_num}
			ctx1, cancel := context.WithTimeout(context.Background(), timeForRpcContext/2)
			defer cancel()
			pb.NewBankingServiceClient(srv).Decide(ctx1, &commit)
		}
		log.Print("Commiting Transactions......")
		s.addTransactionToDatastore(majorBlock, true)
		s.last_commited_ballot_num = int64(len(s.datastore))
		s.acceptVal = []Transaction{}
		s.acceptNum = 0
		// log.Printf("Last Commited Ballot Num for Server %s: %d", s.name, s.last_commited_ballot_num)

		for idx, srv := range s.servers {
			if idx != s.id && s.serverStatus[idx] == 1 {
				// log.Print("Sending Commit Message to Server: ", idx)
				go sendDecide(idx,srv)
			}
		}

	} else {
		log.Printf("COULDNT RECEIVE ACCEPTED ACKS!")
	}
	elapsed := time.Since(start) 
	log.Print("Time Taken for Entire Leader Election: ", elapsed)
	return leaderSyncedCheck, float32(s.Clientbalance)
}

func (s *server) PrintPerformance(ctx context.Context, in *pb.Empty) (*pb.PrintPerformanceResponse, error) {
	if(s.totalTxns>0){
	avgLatency := s.totalLatency / time.Duration(s.totalTxns) 
	fmt.Printf("Server %d Performance:\n", s.id)
	fmt.Printf("  Total Transactions: %d\n", s.totalTxns)
	fmt.Printf("  Total Latency: %s\n", s.totalLatency)
	fmt.Printf("  Average Latency per Transaction: %s\n", avgLatency)
	fmt.Printf("  Throughput (Txns/sec): %.2f\n", float64(s.totalTxns)/s.totalLatency.Seconds())
	}else{
	fmt.Printf("The server has 0 transactions, hence cannot show performance metrics\n")
	}
	return &pb.PrintPerformanceResponse{TotalTransactions: int64(s.totalTxns),TotalLatency: float32(s.totalLatency)}, nil
}


func (s *server) PrintBalance(ctx context.Context, in *pb.Empty) (*pb.PrintBalanceResponse, error) {
	fmt.Printf("Server %s balance: %f\n", s.name,s.Clientbalance)
	return &pb.PrintBalanceResponse{Balance: int64(s.Clientbalance)}, nil
}
func (s *server) PrintLog(ctx context.Context, in *pb.Empty) (*pb.PrintLogResponse, error) {
	fmt.Printf("Client %s log: %v\n", s.name,s.log)
	return &pb.PrintLogResponse{Logs: toProtoTransactions(s.log)}, nil
}
func (s *server) PrintDB(ctx context.Context, in *pb.Empty) (*pb.PrintDBResponse, error) {
	var datastore []*pb.Block
	for _, block := range s.datastore {

		pbBlock := &pb.Block{
			BlockId: int64(block.block_id),
			Mb:      toProtoTransactions(block.Transaction),
		}
		datastore = append(datastore, pbBlock)
	}
	fmt.Printf("Server %s datastore: %v\n", s.name,s.datastore)
	return &pb.PrintDBResponse{Datastore: datastore}, nil
}

func (s *server) CollectAllBalance(ctx context.Context, in *pb.Empty) (*pb.BalanceDetails, error) {
	return &pb.BalanceDetails{ClientId: int64(s.id),Balance: int64(s.Clientbalance),Log: toProtoTransactions(s.log)}, nil
}



func (s *server) addTransactionToDatastore(transactions []Transaction, delLog bool) {
	// log.Print("---Applying To Datastore---")
	block := &Block{
		block_id:    len(s.datastore),
		Transaction: transactions,
	}
	s.datastore = append(s.datastore, *block)

	fmt.Printf("Client %s Balance: %f\n", s.name, s.Clientbalance)

	// Update account balances
	for _, txn := range transactions {
		// log.Print(txn)
		if txn.Receiver == s.name {
			s.Clientbalance += (txn.Amount)
		}
	}
	if delLog {
		newLog := []Transaction{}
		for _, logEntry := range s.log {
			found := false
			for _, txn := range transactions {
				if logEntry == txn {
					found = true
					break
				}
			}
			if !found {
				newLog = append(newLog, logEntry)
			}
		}
		s.log = newLog
	}
	fmt.Printf("Updated Client Balance: %f\n", s.Clientbalance)
}

func (s *server) TransferMoney(ctx context.Context, in *pb.TransferRequest) (*pb.TransactionResponse, error) {
	start := time.Now()
	if s.Clientbalance < (in.GetAmount()) {
		fmt.Printf("Server %s: Insufficient balance\n---Starting Leader Election---\n", s.name)
		if(len(in.ServerStatus)>0){
		s.serverStatus = [5]int64(in.ServerStatus)
		}
		leaderSyncedCheck, x := s.StartLeaderElection()
		if leaderSyncedCheck && (x < in.GetAmount()) {
			fmt.Print("Leader Resync or Previously AcceptedVal Proposed in Last Round, Hence:\n")
			fmt.Print("---Starting Leader Election---\n")
			_, x = s.StartLeaderElection()
		}
		if x < (in.GetAmount()) {
			elapsed := time.Since(start) 
			s.totalLatency += elapsed  
			s.totalTxns++     
			return &pb.TransactionResponse{Message: "Insufficient Balance", Success: false}, nil
		}
		s.log = append(s.log, Transaction{TimeStamp: time.Now().UnixNano(), Sender: in.Sender, Receiver: in.Receiver, Amount: (in.GetAmount())})
		s.Clientbalance -= (in.GetAmount())
		fmt.Printf("Transferred %.2f from %s\n", in.GetAmount(), s.name)
		fmt.Printf("New balance for %s: %f\n", s.name, s.Clientbalance)
		elapsed := time.Since(start) 
		s.totalLatency += elapsed   
		s.totalTxns++               
		return &pb.TransactionResponse{Message: "Transfer successful", Success: true}, nil
	} else {
		s.log = append(s.log, Transaction{TimeStamp: time.Now().UnixNano(), Sender: in.Sender, Receiver: in.Receiver, Amount: (in.GetAmount())})
		s.Clientbalance -= (in.GetAmount())
		fmt.Printf("Transferred %.2f from %s\n", in.GetAmount(), s.name)
		fmt.Printf("New balance for %s: %f\n", s.name, s.Clientbalance)
		elapsed := time.Since(start) // Measure elapsed time
		s.totalLatency += elapsed    // Add to total latency
		s.totalTxns++                // Increment transaction count
		return &pb.TransactionResponse{Message: "Transfer successful", Success: true}, nil

	}

}

func main() {
	server_addr := []string{"50051", "50052", "50053", "50054", "50055"}
	port := flag.String("port", "50051", "The server port")
	server_id := flag.Int("id", -1, "The server id (eg. 0,1,2,3,4...)")
	server_name := flag.String("name", "S1", "The server id (eg. A, B, C...)")

	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatalf("failed to listen on port %v: %v", *port, err)
	}

	grpc_server := grpc.NewServer()

	s := &server{
		id:                       *server_id,
		addr:                     *port,
		name:                     *server_name,
		ballot_num:               0,
		acceptNum:                0,
		last_commited_ballot_num: 0,
		acceptVal:                []Transaction{},
		transactions:             []Transaction{},
		log:                      []Transaction{},
		datastore:                []Block{},
	}

	s.Clientbalance=100

	pb.RegisterBankingServiceServer(grpc_server, s)

	fmt.Println("Waiting for 2 seconds for other servers to join...")
	time.Sleep(2 * time.Second)
	for id, addr := range server_addr {

		if addr != *port {
			conn, _ := grpc.NewClient("localhost:"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			s.servers[id] = conn
		} else {
			s.servers[id] = nil
		}
	}

	log.Printf("gRPC server listening at %v", lis.Addr())
	if err := grpc_server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
