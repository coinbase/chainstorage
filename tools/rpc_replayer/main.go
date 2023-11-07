package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/spf13/viper"
)

const (
	baseFolder string = "tools/rpc_replayer"
)

var v *viper.Viper
var hostMatcher *regexp.Regexp

func init() {
	hostMatcher = regexp.MustCompile(`^(.+)\.local\.gd(\:\d+)$`)
	v = viper.New()
	v.SetConfigType("yaml")
	v.AutomaticEnv()
	v.AllowEmptyEnv(true)
	v.SetEnvPrefix("RPC_REPLAYER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	f, err := os.Open(path.Join(baseFolder, "secrets.yml"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		panic(err)
	}
	if err == nil {
		err = v.ReadConfig(f)
		if err != nil {
			panic(err)
		}
	}
}

func handleRpcRequest(w http.ResponseWriter, r *http.Request) {
	g := hostMatcher.FindAllStringSubmatch(r.Host, -1)
	if len(g) != 1 {
		http.Error(w, "Invalid host", http.StatusBadRequest)
		return
	}
	chain := g[0][1]
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Printf("Requesting %v with %v\n", chain, string(body))
	var jsonBody interface{}
	err = json.Unmarshal(body, &jsonBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	batchRequest := true
	if v, ok := jsonBody.(map[string]interface{}); ok {
		jsonBody = []interface{}{v}
		batchRequest = false
	}
	var responseBody []interface{}
	for _, r := range jsonBody.([]interface{}) {
		req := r.(map[string]interface{})
		params, err := json.Marshal(req["params"])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		cachePath := path.Join(
			baseFolder, "seed", chain,
			req["method"].(string),
			fmt.Sprintf("%x.json", sha256.Sum256(params)))
		fmt.Printf("%+v, cache path: %v\n", jsonBody, cachePath)
		cache, err := os.ReadFile(cachePath)
		if err == nil {
			_, _ = w.Write(cache)
			return
		}
		if !errors.Is(err, fs.ErrNotExist) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		err = os.MkdirAll(path.Dir(cachePath), 0o755)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rpc := v.GetString(chain)
		if rpc == "" {
			http.Error(w, fmt.Sprintf("RPC url not configured for %v", chain), http.StatusInternalServerError)
			return
		}
		response, err := http.Post(rpc, "application/json", bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result, err := io.ReadAll(response.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = os.WriteFile(cachePath, result, 0o644)
		responseBody = append(responseBody, json.RawMessage(result))
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/json")
	var resultBody []byte
	if batchRequest {
		resultBody, err = json.Marshal(responseBody)
	} else {
		resultBody, err = json.Marshal(responseBody[0])
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(resultBody)
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleRpcRequest)
	log.Fatal(http.ListenAndServe(":7090", mux))
}
