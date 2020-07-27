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
	ctx := context.Background()

	req := &protocol.GetRequest{QueueName: os.Getenv("QUEUE")}

	for {
		// TODO: move to function and set timeout.
		//		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		//		defer cancel()
		lease, err := c.Get(ctx, req)
		if err != nil {
			if status.Code(err) == codes.Unavailable {
				lease, err = c.Get(ctx, req)
				if err != nil {
					log.Fatal("Failed to reconnect to queue", err)
				}
			}
			log.Fatal("Lease error:", err)
		}
		if lease.Id == "" {
			time.Sleep(2 * time.Second)
			continue
		}

		req, err := http.NewRequest("POST", "http://localhost:8080/v1/", bytes.NewBuffer(lease.Payload))
		if err != nil {
			log.Print("Unable to create request: ", err)
			continue
		}
		for k, v := range lease.Headers {
			req.Header[k] = v.Value
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal("Failed to post payload", err)
		}
		defer resp.Body.Close()
		// TODO: start background lease renewal
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("failed to read for %q: %v", lease.Id, err)
		}
		log.Printf("Processed %q: %q", lease.Id, data)

		if resp.StatusCode < 300 {
			log.Print("Processed ", lease.Id)
			finish := &protocol.FinishRequest{
				Id:            lease.Id,
				Result:        data,
				ResultHeaders: make(map[string]*protocol.HeaderValue, len(resp.Header)),
			}
			for k, v := range resp.Header {
				finish.ResultHeaders[k] = &protocol.HeaderValue{Value: v}
			}
			c.Finish(ctx, finish)
		} else {
			log.Printf("Failed %q: %d", lease.Id, resp.StatusCode)
			c.Nack(ctx, &protocol.NackRequest{Id: lease.Id, Error: resp.Status})
		}
	}
}
