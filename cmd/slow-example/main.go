package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// Simple server that takes 45s to respond and logs its requests.
func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Handling %s to %s", r.Method, r.URL.String())
		log.Printf("Headers: %v", r.Header)
		time.Sleep(45 * time.Second)
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Print("Failed to read body: ", err)
			w.WriteHeader(500)
			fmt.Fprint(w, "Failed to read body: ", err)
			return
		}
		log.Print(string(body))
		log.Println()
		w.WriteHeader(200)
		fmt.Fprint(w, "Logged data")
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
