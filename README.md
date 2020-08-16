A proof of concept for [the futures API](https://docs.google.com/document/d/1pJd12gb32XH3GU9mBmp9YSlYLPU5efiIinDJPu6H998/edit) data plane.

(See that doc for pretty pictures)

This consists of three components:

1. A Redis cluster for backing storage

   This is provisioned via an operator, so requires no direct action.

2. A stateless API manager for the data plane

   This is curretly a Deployment + Service, but it could easily be a Knative Service if desired.

   This is the only component that talks to Redis. It exposes a simple HTTP API with three endpoints:

   - `POST /v1/`: creates a new Work unit. Returns an ID for the work unit
   - `GET /v1/<id>`: gets the status of a work unit.
   - `GET /queueLength`: returns the number of items available for work.

3) A set of workers, which consist of a fixed `queue-worker` and a user-controlled sidecar.

   Currently a hand-rolled deployment with 1 replica -- TODO: convert this to https://keda.sh/ scaling, and put under controller automation.

   Uses [a GRPC protocol](protocol/taskqueue.proto) to communicate with the API manager
