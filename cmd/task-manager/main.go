package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/evankanderson/knative-async-poc/protocol"
	"github.com/go-redis/redis/v8"

	"github.com/bradleypeabody/gouuidv6"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

func main() {
	opts := &redis.UniversalOptions{
		MasterName: os.Getenv("REDIS_MASTER_NAME"),
		Addrs:      []string{os.Getenv("BROKER")},
	}

	redis := redis.NewUniversalClient(opts)

	rpcServer := grpc.NewServer()
	tm := TaskManagerServer{srv: rpcServer, redis: redis, FinishedExpiration: 4 * time.Hour}

	protocol.RegisterTaskManagerServer(rpcServer, &tm)
	api := &ExternalAPI{redis: redis}

	http.Handle("/v1/", api)
	http.HandleFunc("/queueLength", func(w http.ResponseWriter, r *http.Request) {
		works, err := redis.Keys(r.Context(), "pending_*").Result()
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprint(w, "Unable to fetch work: ", err)
			return
		}
		fmt.Fprint(w, len(works))
	})

	h2s := &http2.Server{}

	log.Fatal(http.ListenAndServe(":80", h2c.NewHandler(&tm, h2s)))
}

// ExternalAPI provides a meechanism
type ExternalAPI struct {
	redis redis.Cmdable
}

// ServeHTTP implements http.Handler
func (api *ExternalAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Got request for %q", r.URL)
	ctx := r.Context()
	switch r.Method {
	case http.MethodGet:
		id := r.URL.Path[len("/v1/"):]
		log.Printf("Fetching %q", id)

		val, err := api.redis.Get(ctx, "pending_"+id).Result()
		if err == redis.Nil {
			// See if it has been completed.
			val, err = api.redis.Get(ctx, "done_"+id).Result()
		}
		if err != nil {
			w.WriteHeader(404)
			fmt.Fprintf(w, "No task %q found", id)
			return
		}
		work := Work{}
		if err := json.Unmarshal([]byte(val), &work); err != nil {
			w.WriteHeader(500)
			fmt.Fprintf(w, "Failed to unmarshal %q: %v", id, err)
			return
		}
		retval := struct {
			ID      string              `json:"id"`
			Status  string              `json:"status"`
			Result  int                 `json:"result,omitempty"`
			Headers map[string][]string `json:"headers,omitempty"`
			Payload string              `json:"payload,omitempty"`
		}{
			ID:      work.ID,
			Status:  work.Status(),
			Result:  0, // TODO: plumb this through
			Headers: work.ResponseHeaders,
		}
		if utf8.Valid(work.ResponseBody) {
			retval.Payload = string(work.ResponseBody)
		} else {
			retval.Payload = base64.StdEncoding.EncodeToString(work.ResponseBody)
		}
		out, err := json.Marshal(retval)
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprint(w, "Failed to marshal response: ", err)
			return
		}
		w.Write(out)
	case http.MethodPost:
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprint(w, "Unable to read body:", err)
			return
		}
		now := time.Now()
		id := gouuidv6.NewFromTime(now)
		work := Work{
			ID:          id.String(),
			Queue:       "queuename", // TODO: pick up from URL
			Headers:     r.Header,
			Body:        data,
			EnqueueTime: time.Now(),
		}

		workJSON, err := json.Marshal(work)
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprint(w, "Unable to marshal work", err)
			return
		}

		ok, err := api.redis.SetNX(ctx, work.Key(), workJSON, 0).Result()
		if err != nil || !ok {
			log.Printf("Failed to publish %q (%t): %v", work.ID, ok, err)
			w.WriteHeader(500)
			fmt.Fprint(w, "Failed to publish task", err)
			return
		}

		fmt.Fprintf(w, `{"id": "%s"}`, work.ID)
		fmt.Fprint(w, "\n")
		log.Printf("Created task %q", work.ID)
	default:
		w.WriteHeader(400)
		fmt.Fprintf(w, "Unimplemented!")
	}
}

// Work is the storage representation (converts to JSON) of a single unit of work.
type Work struct {
	ID      string
	Queue   string
	Headers http.Header
	Body    []byte
	// Timing info
	EnqueueTime time.Time
	LeaseUntil  time.Time
	CompletedAt time.Time
	Retries     []time.Time
	// Response info
	ResponseHeaders http.Header
	ResponseBody    []byte
	FailureStatus   string
}

// Status provides a human-readable status for Work.
func (w Work) Status() string {
	if w.CompletedAt.IsZero() {
		return "pending"
	}
	return "completed"
}

// Key returns the redis row key for the Work given its current state.
func (w Work) Key() string {
	if w.CompletedAt.IsZero() {
		return "pending_" + w.ID
	}
	return "done_" + w.ID
}

// TaskManagerServer implements protocol.TaskManagerServer
type TaskManagerServer struct {
	srv                *grpc.Server
	redis              redis.UniversalClient
	FinishedExpiration time.Duration
}

// ServeHTTP implements http.Handler to mux between grpc and standard HTTP
func (tms *TaskManagerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
		tms.srv.ServeHTTP(w, r)
	} else {
		http.DefaultServeMux.ServeHTTP(w, r)
	}
}

// Get implements protocol.TaskManagerServer
func (tms *TaskManagerServer) Get(ctx context.Context, req *protocol.GetRequest) (*protocol.Work, error) {
	remoteAddr := "unknown"
	if clientPeer, ok := peer.FromContext(ctx); ok {
		remoteAddr = clientPeer.Addr.String()
	}

	works, err := tms.redis.Keys(ctx, "pending_*").Result()
	if err != nil {
		return nil, err
	}

	for _, key := range works {
		workJSON, err := tms.redis.Get(ctx, key).Result()
		if err != nil {
			log.Printf("Error fetching %q, continuing: %v", key, err)
			continue
		}
		work := Work{}
		if err := json.Unmarshal([]byte(workJSON), &work); err != nil {
			log.Printf("Error unmarshalling %q: %v", key, err)
		}
		if work.LeaseUntil.Before(time.Now()) {
			leaseEnd, err := tms.update(ctx, work, func(work *Work) {
				work.LeaseUntil = time.Now().Add(10 * time.Minute)
			})
			if err != nil {
				log.Printf("Failed to claim %q: %v", work.ID, err)
				continue
			}
			enqueueTime, err := ptypes.TimestampProto(work.EnqueueTime)
			if err != nil {
				log.Printf("Unable to encode time for %q (%s): %v", work.ID, work.EnqueueTime, err)
				continue
			}
			leaseTime, err := ptypes.TimestampProto(leaseEnd)
			if err != nil {
				log.Printf("Unable to encode time for %q (%s): %v", work.ID, leaseEnd, err)
				continue
			}
			retval := &protocol.Work{
				Id:          work.ID,
				Payload:     work.Body,
				EnqueuedAt:  enqueueTime,
				LeasedUntil: leaseTime,
				Headers:     make(map[string]*protocol.HeaderValue, len(work.Headers)),
			}

			for k, v := range work.Headers {
				retval.Headers[k] = &protocol.HeaderValue{Value: v}
			}

			log.Printf("Sending task %q to %q", work.ID, remoteAddr)
			return retval, nil
		}
	}

	// Return an empty work to signal no more tasks.
	return &protocol.Work{}, nil
}

// Renew implements protocol.TaskManagerServer
func (tms *TaskManagerServer) Renew(ctx context.Context, req *protocol.RenewRequest) (*empty.Empty, error) {
	work, err := tms.fetchKey(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	_, err = tms.update(ctx, *work, func(work *Work) {
		work.LeaseUntil = time.Now().Add(10 * time.Minute)
	})
	if err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

// Finish implements protocol.TaskManagerServer
func (tms *TaskManagerServer) Finish(ctx context.Context, req *protocol.FinishRequest) (*empty.Empty, error) {
	work, err := tms.fetchKey(ctx, req.Id)
	oldKey := work.Key()
	if err != nil {
		return nil, err
	}
	work.CompletedAt = time.Now()
	work.ResponseBody = req.Result
	work.ResponseHeaders = make(http.Header, len(req.ResultHeaders))
	for k, v := range req.ResultHeaders {
		work.ResponseHeaders[k] = v.Value
	}
	if err := tms.finish(ctx, work, oldKey); err != nil {
		return nil, err
	}
	log.Printf("Task %q finished!", req.Id)
	return &empty.Empty{}, nil
}

// Nack implements protocol.TaskManagerServer
func (tms *TaskManagerServer) Nack(ctx context.Context, req *protocol.NackRequest) (*empty.Empty, error) {
	work, err := tms.fetchKey(ctx, req.Id)
	oldKey := work.Key()
	if err != nil {
		return nil, err
	}
	if len(work.Retries) > 2 {
		work.CompletedAt = time.Now()
		work.FailureStatus = req.Error
		if err := tms.finish(ctx, work, oldKey); err != nil {
			return nil, err
		}
		log.Printf("Failed task %q", req.Id)
		return &empty.Empty{}, nil
	}
	// Enqueue another retry
	_, err = tms.update(ctx, *work, func(work *Work) {
		work.Retries = append(work.Retries, time.Now())
		work.LeaseUntil = time.Time{}
	})
	if err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (tms *TaskManagerServer) update(ctx context.Context, work Work, update func(work *Work)) (time.Time, error) {
	oldJSON, err := json.Marshal(work)
	if err != nil {
		return time.Time{}, err
	}

	update(&work)
	workJSON, err := json.Marshal(work)
	if err != nil {
		return time.Time{}, err
	}

	txn := func(tx *redis.Tx) error {
		prev, err := tx.Get(ctx, work.Key()).Result()
		if err != nil {
			return err
		}
		if !bytes.Equal([]byte(prev), oldJSON) {
			return fmt.Errorf("Newer data since %s:\n%s\n%s", work.LeaseUntil, oldJSON, prev)
		}
		return tx.Set(ctx, work.Key(), workJSON, 0).Err()
	}
	return work.LeaseUntil, tms.redis.Watch(ctx, txn, work.Key())
}

func (tms *TaskManagerServer) finish(ctx context.Context, work *Work, oldKey string) error {
	workJSON, err := json.Marshal(work)
	if err != nil {
		return err
	}
	if err := tms.redis.Set(ctx, work.Key(), workJSON, tms.FinishedExpiration).Err(); err != nil {
		return err
	}
	return tms.redis.Del(ctx, oldKey).Err()
}

func (tms *TaskManagerServer) fetchKey(ctx context.Context, id string) (*Work, error) {
	workJSON, err := tms.redis.Get(ctx, "pending_"+id).Result()
	if err != nil {
		return nil, err
	}
	work := Work{}
	if err := json.Unmarshal([]byte(workJSON), &work); err != nil {
		return nil, err
	}
	return &work, nil
}
