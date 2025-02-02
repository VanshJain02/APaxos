package main

// Code From Vansh Jain
// DS Lab 1
// SBU ID - 116713519
import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	pb "project1/proto"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	numClients = 5
	basePort   = 50051
)

type ClientInfo struct {
	name      string
	port      int
	client_id int
	client    pb.BankingServiceClient
}

type Transaction struct {
	sender       string
	receiver     string
	amount       float64
	serverStatus [5]int64
}

type Block struct {
	blockId  int
	TxnBlock []Transaction
}

var activeServerStatus []int64

func parseCSV(filePath string) ([]Block, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	r := csv.NewReader(file)
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	var TransactionOrder []Block
	var BlockTransaction []Transaction
	blockId := 1
	serverStatus_txn := [5]int64{0, 0, 0, 0, 0}
	for id, row := range records {
		x := strings.Split(strings.Trim(row[1], "()"), ",")
		amount, err := strconv.ParseFloat(strings.TrimSpace(x[2]), 64)
		if err != nil {
			log.Fatal(err)
		}

		if id == 0 {
			for _, i := range strings.Split(strings.Trim(row[2], "[]"), ",") {
				i = strings.TrimSpace(i)
				if string(i) == "S1" {
					serverStatus_txn[0] = 1
				} else if string(i) == "S2" {
					serverStatus_txn[1] = 1
				} else if string(i) == "S3" {
					serverStatus_txn[2] = 1
				} else if string(i) == "S4" {
					serverStatus_txn[3] = 1
				} else if string(i) == "S5" {
					serverStatus_txn[4] = 1
				}
			}
		}

		if len(row[2]) != 0 && id != 0 {
			row_block_id, err := strconv.Atoi(row[0])
			if err != nil {
				log.Fatal(err)
			}

			blockId = row_block_id
			TransactionOrder = append(TransactionOrder, Block{blockId: blockId - 1, TxnBlock: BlockTransaction})

			BlockTransaction = []Transaction{}
			serverStatus_txn = [5]int64{0, 0, 0, 0, 0}
			for _, i := range strings.Split(strings.Trim(row[2], "[]"), ",") {
				i = strings.TrimSpace(i)
				if string(i) == "S1" {
					serverStatus_txn[0] = 1
				} else if string(i) == "S2" {
					serverStatus_txn[1] = 1
				} else if string(i) == "S3" {
					serverStatus_txn[2] = 1
				} else if string(i) == "S4" {
					serverStatus_txn[3] = 1
				} else if string(i) == "S5" {
					serverStatus_txn[4] = 1
				}
			}

		}
		copyServerStat := make([]int64, len(serverStatus_txn))
		copy(copyServerStat, serverStatus_txn[:])

		abd := Transaction{sender: strings.TrimSpace(x[0]), receiver: strings.TrimSpace(x[1]), amount: float64(amount), serverStatus: [5]int64(copyServerStat)}
		BlockTransaction = append(BlockTransaction, abd)
	}
	TransactionOrder = append(TransactionOrder, Block{blockId: blockId, TxnBlock: BlockTransaction})

	return TransactionOrder, nil
}

func initializeClient(name string, port int) pb.BankingServiceClient {
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[%s] did not connect: %v", name, err)
	}

	return pb.NewBankingServiceClient(conn)
}

func startQueue(clientInfo []ClientInfo, queuedTxns map[int]Transaction) {

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if len(queuedTxns) == 0 {
			break
		}
		client_txn_id := -1

		for idx, txn := range queuedTxns {

			if txn.sender == "S1" {
				client_txn_id = 0
			}
			if txn.sender == "S2" {
				client_txn_id = 1
			}
			if txn.sender == "S3" {
				client_txn_id = 2
			}
			if txn.sender == "S4" {
				client_txn_id = 3
			}
			if txn.sender == "S5" {
				client_txn_id = 4
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			r, err := clientInfo[client_txn_id].client.TransferMoney(ctx, &pb.TransferRequest{
				Sender:       txn.sender,
				Receiver:     txn.receiver,
				Amount:       float32(txn.amount),
				ServerStatus: activeServerStatus,
			})
			if err == nil && r.GetSuccess() {
				fmt.Printf("Transaction from %s to %s succeeded on retry.\n", txn.sender, txn.receiver)
				delete(queuedTxns, idx)
			} else {
				fmt.Printf("Retry failed for transaction from %s to %s.\nTrying Again in 10 secs\n", txn.sender, txn.receiver)
			}
		}
	}
}

func executeTransactions(transaction_block []Transaction, clientInfo []ClientInfo, queuedTxns map[int]Transaction) {
	client_txn_id := -1
	for _, block := range transaction_block {

		if block.sender == "S1" {
			client_txn_id = 0
		}
		if block.sender == "S2" {
			client_txn_id = 1
		}
		if block.sender == "S3" {
			client_txn_id = 2
		}
		if block.sender == "S4" {
			client_txn_id = 3
		}
		if block.sender == "S5" {
			client_txn_id = 4
		}

		fmt.Printf("[%s] Initiating transfer from %s to %s of amount %.2f\n", clientInfo[client_txn_id].name, block.sender, block.receiver, block.amount)

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		activeServerStatus = block.serverStatus[:]
		r, err := clientInfo[client_txn_id].client.TransferMoney(ctx, &pb.TransferRequest{
			Sender:       block.sender,
			Receiver:     block.receiver,
			Amount:       float32(block.amount),
			ServerStatus: block.serverStatus[:],
		})
		if err != nil {
			log.Fatalf("[%s] could not transfer money: %v", clientInfo[client_txn_id].name, err)
		}

		if r.GetSuccess() {
			fmt.Printf("[%s] Transfer successful!\n", clientInfo[client_txn_id].name)
		} else {
			fmt.Printf("[%s] Transfer Queued: Waiting for Incoming Balance\n", clientInfo[client_txn_id].name)
			queuedTxns[len(queuedTxns)] = block
			if len(queuedTxns) == 1 {
				time.Sleep(200 * time.Millisecond)
				go startQueue(clientInfo, queuedTxns)
			}
		}
	}
}

func printFunctions(option int, clients []ClientInfo) {
	for _, i := range clients {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		switch option {
		case 1:
			b, _ := i.client.PrintBalance(ctx, &pb.Empty{})
			fmt.Printf("The Balance of Client %s: %d\n", i.name, b.Balance)
		case 2:
			log, _ := i.client.PrintLog(ctx, &pb.Empty{})
			fmt.Printf("The Log of Client %s: %v\n\n", i.name, log.Logs)

		case 3:
			db, _ := i.client.PrintDB(ctx, &pb.Empty{})
			fmt.Printf("The Database of Client %s: \n\n", i.name)
			for idx, i := range db.Datastore {
				fmt.Printf("Block Id: %d \n", idx)
				for _, j := range i.Mb {
					fmt.Printf("%v \n", j)
				}
				fmt.Print("\n\n")
			}

		case 4:
			performance, _ := i.client.PrintPerformance(ctx, &pb.Empty{})
			if performance.TotalTransactions > 0 {
				avgLatency := time.Duration(performance.TotalLatency) / time.Duration(performance.TotalTransactions)
				fmt.Printf("Server %s Performance:\n", i.name)
				fmt.Printf("  Total Transactions: %d\n", performance.TotalTransactions)
				fmt.Printf("  Total Latency: %s\n", time.Duration(performance.TotalLatency))
				fmt.Printf("  Average Latency per Transaction: %s\n", avgLatency)
				fmt.Printf("  Throughput (Txns/sec): %.2f\n\n", float64(performance.TotalTransactions)/time.Duration(performance.TotalLatency).Seconds())
			} else {
				fmt.Printf("The Server %s has 0 transactions, hence cannot show performance metrics\n\n", i.name)
			}
		}
	}
}

func printAllClientBalance(clients []ClientInfo) {
	client_bal := [5]int64{}
	for _, i := range clients {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		temp, _ := i.client.CollectAllBalance(ctx, &pb.Empty{})
		client_bal[i.client_id] += temp.Balance
		for _, j := range temp.Log {
			if j.Receiver == "S1" {
				client_bal[0] += int64(j.Amount)
			}
			if j.Receiver == "S2" {
				client_bal[1] += int64(j.Amount)
			}
			if j.Receiver == "S3" {
				client_bal[2] += int64(j.Amount)
			}
			if j.Receiver == "S4" {
				client_bal[3] += int64(j.Amount)
			}
			if j.Receiver == "S5" {
				client_bal[4] = client_bal[4] + int64(j.Amount)
			}
		}
	}

	for idx, i := range [5]string{"S1", "S2", "S3", "S4", "S5"} {
		fmt.Printf("The Overall Balance of Client %s: %d\n", i, client_bal[idx])
	}

}

func main() {

	csvFilePath := "lab1_Test.csv"

	// csvFilePath := "vansh_test.csv"
	flag.Parse()

	clientTransactions, err := parseCSV(csvFilePath)
	if err != nil {
		log.Fatalf("Failed to parse CSV: %v", err)
	}

	var wg sync.WaitGroup
	doneChan := make(chan bool, numClients)

	clientNames := []string{"S1", "S2", "S3", "S4", "S5"}
	clientId := []int{0, 1, 2, 3, 4}
	queuedTxns := make(map[int]Transaction)

	var clients []ClientInfo
	for i := 0; i < numClients; i++ {
		name := clientNames[i]
		port := basePort + i
		client := initializeClient(name, port)

		clientInfo := ClientInfo{
			name:      name,
			port:      port,
			client_id: clientId[i],
			client:    client,
		}
		clients = append(clients, clientInfo)
	}
	i := 0

	for i <= len(clientTransactions) {
		fmt.Println("\nChoose an option:")
		if i < len(clientTransactions) {
			fmt.Println("1. Proceed to a New Block of Transactions")
		}
		fmt.Println("2. Print Balance")
		fmt.Println("3. Print Log")
		fmt.Println("4. Print DB")
		fmt.Println("5. Print Performance")
		fmt.Println("6. Print Balance of all Clients (BONUS:3)")
		fmt.Println("7. Exit")

		var option int
		fmt.Scan(&option)

		switch option {
		case 1:
			if i < len(clientTransactions) {
				executeTransactions(clientTransactions[i].TxnBlock, clients, queuedTxns)
			}
			i++
		case 2:
			printFunctions(1, clients)
		case 3:
			printFunctions(2, clients)
		case 4:
			printFunctions(3, clients)
		case 5:
			printFunctions(4, clients)
		case 6:
			printAllClientBalance(clients)
		case 7:
			i++
			return
		default:
			fmt.Println("Invalid option. Please try again.")
		}

	}

	wg.Wait()
	close(doneChan)
}
