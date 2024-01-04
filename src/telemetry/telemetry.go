package telemetry

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"github.com/inspectadb/inspectadb/src/consts"
	"net/http"
	"os"
	"runtime"
)

const Endpoint = "https://telemetry.auditdb.org"

type signal struct {
	Command       string         `json:"command"`
	Driver        string         `json:"driver"`
	AppVersion    string         `json:"appVersion"`
	ServerVersion string         `json:"serverVersion"`
	OS            string         `json:"os"`
	CI            bool           `json:"ci"`
	Docker        bool           `json:"docker"`
	Profile       map[string]any `json:"profile"`
}

func (s signal) Send() {
	defer func() {
		if recover() != nil {
		}
	}()

	pl, _ := json.Marshal(s)
	client := http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	req, err := client.Post(Endpoint, "application/json", bytes.NewBufferString(string(pl)))

	if err != nil {
	}

	defer req.Body.Close()
}

func NewSignal(command string, driver string, serverVersion string, profile map[string]any) *signal {
	return &signal{
		Command:       command,
		Driver:        driver,
		AppVersion:    consts.AppVersion,
		ServerVersion: serverVersion,
		OS:            runtime.GOOS,
		CI:            isRunningInCI(),
		Docker:        isRunningInDocker(),
		Profile:       profile,
	}
}

// isRunningInCI
// GitHub, GitLab, Travis, CircleCI => CI env var
// Jenkins => JENKINS_HOME
// Azure DevOps => BUILD_REASON = IndividualCI ||  BatchedCI
func isRunningInCI() bool {
	ci := false

	if val, exists := os.LookupEnv("CI"); exists && val == "true" {
		ci = true
	} else if val, exists = os.LookupEnv("JENKINS_HOME"); exists && val != "" {
		ci = true
	} else if val, exists = os.LookupEnv("BUILD_REASON"); exists && (val == "IndividualCI" || val == "BatchedCI") {
		ci = true
	}

	return ci
}

// isRunningInDocker
// Check if they're using our docker image
func isRunningInDocker() bool {
	val, isDocker := os.LookupEnv("IS_DOCKER")

	return isDocker && val == "true"
}
