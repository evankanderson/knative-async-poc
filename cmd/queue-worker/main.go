package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/evankanderson/knative-async-poc/protocol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	server := os.Getenv("SERVER")
	if server == "" {
		server = "queue-server:80"
	}
	conn, err := grpc.Dial(server, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatal("Could not connect:", err)
	}
	defer conn.Close()

	c := protocol.NewTaskManagerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	req := &protocol.GetRequest{QueueName: os.Getenv("QUEUE")}

	stream, err := c.Get(ctx, req)
	if err != nil {
		log.Fatal("Failed to fetch work from queue", err)
	}

	for {
		lease, err := stream.Recv()
		if err != nil {
			if status.Code(err) == codes.Unavailable {
				stream, err = c.Get(ctx, req)
				if err != nil {
					log.Fatal("Failed to reconnect to queue", err)
				}
			}
			log.Fatal("Lease error:", err)
		}
		payload := bytes.NewBuffer(lease.Payload)

		resp, err := http.Post("http://localhost:8080", "", payload)
		if err != nil {
			log.Fatal("Failed to post payload", err)
		}
		// TODO: start background lease renewal
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("failed to read for %q: %v", lease.Id, err)
		}
		log.Printf("Processed %q: %q", lease.Id, data)

		if resp.StatusCode < 300 {
			log.Print("Processed ", lease.Id)
			c.Finish(ctx, &protocol.FinishRequest{Id: lease.Id})
		} else {
			log.Printf("Failed %q: %d", lease.Id, resp.StatusCode)
			c.Nack(ctx, &protocol.NackRequest{Id: lease.Id})
		}
	}
}
