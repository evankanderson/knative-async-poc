package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/evankanderson/knative-async-poc/protocol"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/backends/result"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"

	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func main() {
	queueCfg, err := config.NewFromEnvironment()
	if err != nil {
		log.Fatal("Unable to read machinery config", err)
	}
	queueCfg.AMQP = nil
	queueCfg.TLSConfig = nil
	if queueCfg.Redis == nil {
		queueCfg.Redis = &config.RedisConfig{
			MasterName: "mymaster",
		}
		log.Print("Updated missing redis mastername: ", queueCfg)
	}
	log.Print("Using broker:", queueCfg.Broker)
	log.Print("Serving queue:", queueCfg.DefaultQueue)

	rpcServer := grpc.NewServer()
	tm := TaskManagerServer{srv: rpcServer, queueCfg: queueCfg, httpMux: http.NewServeMux()}

	protocol.RegisterTaskManagerServer(rpcServer, &tm)

	queue, err := machinery.NewServer(queueCfg)
	if err != nil {
		log.Fatal("Unable to connect to machinery", err)
	}
	queue.RegisterTask("do", func(payload string) error {
		return fmt.Errorf("Got unexpected payload: %q", payload)
	})
	api := &ExternalAPI{queue: queue}

	tm.httpMux.Handle("/v1/", api)

	h2s := &http2.Server{}

	log.Fatal(http.ListenAndServe(":80", h2c.NewHandler(&tm, h2s)))
}

// ExternalAPI provides a meechanism
type ExternalAPI struct {
	queue *machinery.Server
}

// ServeHTTP implements http.Handler
func (api *ExternalAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Got request for %q", r.URL)
	switch r.Method {
	case http.MethodGet:
		// TODO: fetch asyncResult based on query param
		id := r.URL.Path[len("/v1/"):]

		log.Printf("Fetching %q", id)
		task := result.NewAsyncResult(&tasks.Signature{UUID: id}, api.queue.GetBackend())
		if task == nil {
			w.WriteHeader(500)
			fmt.Fprint(w, "Unable to get result for ", id)
			return
		}
		log.Printf("Getting state for %q", id)
		state := task.GetState()
		if state != nil {
			w.WriteHeader(500)
			fmt.Fprint(w, "Unable to get status for ", id)
			return
		}

		fmt.Fprintf(w, `{"id": "%s", "state": "%s"}`, state.TaskUUID, state.State)
	case http.MethodPost:
		// TODO: enqueue payload in machinery
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprint(w, "Unable to read body:", err)
			return
		}
		log.Printf("Handling %q", data)
		task := tasks.Signature{
			Name: "do",
			Args: []tasks.Arg{
				{
					Type:  "string",
					Value: base64.StdEncoding.EncodeToString(data),
				},
			},
		}
		ctx, cancel := context.WithTimeout(r.Context(), 200*time.Millisecond)
		defer cancel()
		asyncResult, err := api.queue.SendTaskWithContext(ctx, &task)
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprint(w, "Unable to store task", err)
			return
		}
		fmt.Fprintf(w, `{"id": "%s"`, asyncResult.Signature.UUID)
		log.Printf("Created task %q", asyncResult.Signature.UUID)
	default:
		w.WriteHeader(400)
		fmt.Fprintf(w, "Unimplemented!")
	}
}

// TaskManagerServer implements protocol.TaskManagerServer
type TaskManagerServer struct {
	srv      *grpc.Server
	queueCfg *config.Config
	httpMux  *http.ServeMux
}

// ServeHTTP implements http.Handler
func (tms *TaskManagerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
		tms.srv.ServeHTTP(w, r)
	} else {
		h, p := tms.httpMux.Handler(r)
		log.Printf("Sending %q to %v for %q", r.URL, h, p)
		tms.httpMux.ServeHTTP(w, r)
	}
}

// Get implements protocol.TaskManagerServer
func (tms *TaskManagerServer) Get(req *protocol.GetRequest, stream protocol.TaskManager_GetServer) error {
	remoteAddr := "unknown"
	if clientPeer, ok := peer.FromContext(stream.Context()); ok {
		remoteAddr = clientPeer.Addr.String()
	}
	log.Printf("Handling connection from %q with config %+v", remoteAddr, tms.queueCfg)
	queue, err := machinery.NewServer(tms.queueCfg)
	if err != nil {
		log.Fatal("Failed to connect to machinery", err)
	}
	worker := queue.NewWorker(req.QueueName, 1)
	queue.RegisterTask("do", func(ctx context.Context, payload string) error {
		data, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return err
		}
		sig := tasks.SignatureFromContext(ctx)
		err = stream.Send(&protocol.Work{
			Id:      sig.UUID,
			Payload: data,
			// TODO: timestamps from additional args
		})
		if status.Code(err) == codes.Canceled || status.Code(err) == codes.Unavailable {
			log.Printf("Shutting down stream to %q: %v", remoteAddr, err)
			worker.Quit()
		}
		return err
	})
	if err := worker.Launch(); err != nil {
		log.Fatal("Failed to launch machinery worker", err)
	}
	return nil
}

// Renew implements protocol.TaskManagerServer
func (tms *TaskManagerServer) Renew(ctx context.Context, req *protocol.RenewRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

// Finish implements protocol.TaskManagerServer
func (tms *TaskManagerServer) Finish(ctx context.Context, req *protocol.FinishRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

// Nack implements protocol.TaskManagerServer
func (tms *TaskManagerServer) Nack(ctx context.Context, req *protocol.NackRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}
