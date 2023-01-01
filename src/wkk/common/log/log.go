package log

import (
    "fmt"
    "os"
    "path"
    "runtime"
    "strings"
    "time"
)

type Level int

const (
    DEBUG    = Level(0)
    INFO     = Level(1)
    WARN     = Level(2)
    ERROR    = Level(3)
    CRITICAL = Level(4)
    FATAL    = Level(5)
)

var (
    logLevel = DEBUG
    logFName = ""
)

func (level Level) String() string {
    switch level {
    case DEBUG:     return "DEBUG"
    case INFO:      return "INFO"
    case WARN:      return "WARN"
    case ERROR:     return "ERROR"
    case CRITICAL:  return "CRIT"
    case FATAL:     return "FATAL"
    default:        panic("UNREACHABLE")
    }
}

func PrimeLog2(verbose bool, fname string)  {
    if verbose {
        logLevel = INFO
    } else {
        logLevel = ERROR
    }
    logFName = fname
}

func Debug(format string, a ...interface{})  {
    if logLevel <= DEBUG {
        logOne(DEBUG, format, a...)
    }
}

func Info(format string, a ...interface{})  {
    if logLevel <= INFO {
        logOne(INFO, format, a...)
    }
}

func Warn(format string, a ...interface{})  {
    if logLevel <= WARN {
        logOne(WARN, format, a...)
    }
}

func Error(format string, a ...interface{})  {
    if logLevel <= ERROR {
        logOne(ERROR, format, a...)
    }
}

func Fatal(format string, a ...interface{})  {
    logOne(FATAL, format, a...)
    panic("fatal")
}

func FatalIf(cond bool, format string, a ...interface{})  {
    if cond {
        logOne(FATAL, format, a...)
        panic("fatal")
    }
}

func ExitIf(cond bool, format string, a ...interface{})  {
    if cond {
        logOne(INFO, "exit by " + format, a...)
        os.Exit(1)
    }
}

func logOne(level Level, format string, a ...interface{}) {
    if level < logLevel {
        return
    }

    var funcName string
    pc, file, ln, ok := runtime.Caller(2)
    if ok {
        tmp := strings.Split(runtime.FuncForPC(pc).Name(), ".")
        file, funcName = path.Base(file), tmp[len(tmp)-1]
    } else {
        file, funcName, ln = "??", "??", 0
    }

    prefix0 := time.Now().UTC().Format("2006-01-02T15:04:05 ")
    prefix1 := fmt.Sprintf("%s:%d/%s  %s ", file, ln, funcName, level)

    _, _ = fmt.Fprintf(os.Stdout,
        prefix0 + prefix1 + fmt.Sprintf(format, a...) + "\n")
}
