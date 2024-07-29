package lablog

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Retrieve the verbosity level from an environment variable
// ref: https://blog.josejg.com/debugging-pretty/
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type LogTopic string

const (
	Heart  LogTopic = "HART"
	Info   LogTopic = "INFO"
	Config LogTopic = "CONF"
	Vote   LogTopic = "VOTE"
	Timer  LogTopic = "TIMR"
	Leader LogTopic = "LEAD"
	Append LogTopic = "APND"
	Error  LogTopic = "ERRO"
	Start  LogTopic = "STRT"
	Commit LogTopic = "COMT"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	// disable datetime logging
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(serverId int, topic LogTopic, format string, a ...interface{}) {
	if debugVerbosity == 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		if serverId >= 0 {
			prefix += fmt.Sprintf("S%d ", serverId)
		}
		format = prefix + format
		log.Printf(format, a...)
	}
}
