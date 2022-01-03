package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

//NewHTTPServer creates a http server at addr
// POSTs to "/" are handled by Produce endpoint
// GETs to "/" are handled by Consume endpoint
func NewHTTPServer(addr string) *http.Server {
	httpsrvr := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpsrvr.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrvr.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}

}

type httpServer struct {
	Logger *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{Logger: NewLog()}
}

type ProduceRequest struct {
	Rec Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Rec Record `json:"record"`
}

//handleProduce is handler for the produce request
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	off, err := s.Logger.Append(req.Rec)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rec, err := s.Logger.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := ConsumeResponse{Rec: rec}
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
