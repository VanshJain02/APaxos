package main

// Code From Vansh Jain
// DS Lab 1
// SBU ID - 116713519

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "project1/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	port := flag.String("port", "50051", "The server port to connect to")
	flag.Parse()

	conn, err := grpc.NewClient("localhost:"+*port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewBankingServiceClient(conn)

	var name string
	// fmt.Println("Enter your account name:")
	// fmt.Scan(&name)
	// name := "world"
	if len(flag.Args()) == 0 {
		log.Fatalf("client name must be provided as an argument")
	}
	name = flag.Args()[0]

	for {
		fmt.Println("\nChoose an option:")
		fmt.Println("1. Check Balance")
		fmt.Println("2. Transfer Money")
		fmt.Println("3. Exit")

		var option int
		fmt.Scan(&option)

		switch option {
		case 1:
			checkBalance(c, name)
		case 2:
			transferMoney(c, name)
		case 3:
			return
		default:
			fmt.Println("Invalid option. Please try again.")
		}
	}
	
}

func checkBalance(c pb.BankingServiceClient, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := c.PrintBalance(ctx, &pb.Empty{})
	if err != nil {
		log.Fatalf("could not retrieve balance: %v", err)
	}

	// fmt.Printf("Balance for %s: %.2f\n", name, r.GetBalance())
}

func transferMoney(c pb.BankingServiceClient, sender string) {
	var receiver string
	var amount float64

	fmt.Println("Enter receiver's account name:")
	fmt.Scan(&receiver)
	fmt.Println("Enter amount to transfer:")
	fmt.Scan(&amount)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	r, err := c.TransferMoney(ctx, &pb.TransferRequest{
		Sender:   sender,
		Receiver: receiver,
		Amount:   float32(amount),
		ServerStatus: []int64{1,1,1,1,1},
	})
	if err != nil {
		log.Fatalf("could not transfer money: %v", err)
	}

	if r.GetSuccess() {
		fmt.Println("Transfer successful!")
	} else {
		fmt.Println("Transfer Queued: Waiting for Incoming Balance")
	}
}
