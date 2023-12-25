package telemetry

//import (
//	"bytes"
//	"crypto/tls"
//	"encoding/json"
//	"log"
//	"net/http"
//	"os"
//	"runtime"
//)
//
//const TELEMETRY_ENDPOINT = "https://telemetry.inspectadb.org"
//
//type signal struct {
//	Command       string         `json:"command"`
//	Driver        string         `json:"driver"`
//	AppVersion    string         `json:"appVersion"`
//	ServerVersion string         `json:"serverVersion"`
//	OS            string         `json:"os"`
//	CI            bool           `json:"ci"`
//	Docker        bool           `json:"docker"`
//	Profile       map[string]any `json:"profile"`
//}
//
//func (s signal) Send() {
//	defer func() {
//		if recover() != nil {
//		}
//	}()
//
//	client := http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
//
//	req, err := client.Post(TELEMETRY_ENDPOINT, "application/json", bytes.NewBufferString(s.ToJSON()))
//
//	if err != nil {
//		log.Printf("failed to send telemetry request. this will not stop execution. You can help us out by reporting this as an issue. %s", err)
//	}
//
//	defer req.Body.Close()
//}
//
//func (s signal) ToJSON() string {
//	b, _ := json.Marshal(s)
//
//	return string(b)
//}
//
//func NewSignal(command string, driver string, serverVersion string, profile map[string]any) *signal {
//	isCI, ciExists := os.LookupEnv("CI")
//	isDocker, dockerExists := os.LookupEnv("IS_DOCKER")
//
//	if !ciExists {
//		isCI = false
//	}
//
//	return &signal{
//		Command:       command,
//		Driver:        driver,
//		AppVersion:    "",
//		ServerVersion: serverVersion,
//		OS:            runtime.GOOS,
//		CI:            ciExists,
//		Docker:        dockerExists,
//		Profile:       profile,
//	}
//}
