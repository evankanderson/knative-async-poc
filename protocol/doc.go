// The protocol package contains gRPC definitions for the internal services for
// the saync prototype.
package protocol

// The following line runs the protocol buffer compiler from `go generate ./...`
//go:generate protoc -I . ./taskqueue.proto --go_out=plugins=grpc:. --go_opt=paths=source_relative
