package gsrunner

import (
	"bufio"
	"errors"
	"flag"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/gsdocker/gsconfig"
	"github.com/gsdocker/gserrors"
	"github.com/gsdocker/gslogger"
	"github.com/gsrpc/gorpc"
)

// Error list
var (
	ErrFlag     = errors.New("flag error")
	ErrConfig   = errors.New("config load error")
	ErrRegistry = errors.New("registry file error")
)

// Runner gsdocker services runner
type Runner interface {
	gslogger.Log
	FlagString(name string, fullname string, defaultVal string, description string) Runner
	FlagInt(name string, fullname string, defaultVal int64, description string) Runner
	FlagUint(name string, fullname string, defaultVal uint64, description string) Runner
	FlagFloat32(name string, fullname string, defaultVal float32, description string) Runner
	FlagFloat64(name string, fullname string, defaultVal float64, description string) Runner
	Seconds(name string, fullname string, defaultVal uint64, description string) Runner
	Milliseconds(name string, fullname string, defaultVal uint64, description string) Runner
	Run(main func(runner Runner))
}

type _Runner struct {
	gslogger.Log
	flagString       map[string]*string
	flagInt          map[string]*int64
	flagUint         map[string]*uint64
	flagFloat32      map[string]*float64
	flagFloat64      map[string]*float64
	flagSecond       map[string]*uint64
	flagMilliseconds map[string]*uint64
	fullname         map[string]string
}

// New create new gsdocker runner
func New(name string) Runner {
	runner := &_Runner{
		Log:              gslogger.Get(name),
		flagString:       make(map[string]*string),
		flagInt:          make(map[string]*int64),
		flagUint:         make(map[string]*uint64),
		flagFloat32:      make(map[string]*float64),
		flagFloat64:      make(map[string]*float64),
		flagSecond:       make(map[string]*uint64),
		flagMilliseconds: make(map[string]*uint64),
		fullname:         make(map[string]string),
	}

	runner.FlagString(
		"log", "gsrunner.log", "", "the gsrunner log root path",
	).FlagString(
		"level", "gsrunner.log.level", "", "the gsrunner log level",
	).FlagString(
		"pprof", "gsrunner.pprof", "", "set gsrunner pprof listen address",
	).FlagString(
		"config", "gsrunner.config", "", "set gsrunner config file",
	).FlagString(
		"registry", "gsrunner.registry", "", "set the rpc services registry file",
	)

	return runner
}

func (runner *_Runner) checkName(name, fullname string) {
	if _, ok := runner.fullname[name]; ok {
		gserrors.Panicf(ErrFlag, "duplicate flag name :%s", name)
	}

	for _, v := range runner.fullname {
		if v == fullname {
			gserrors.Panicf(ErrFlag, "duplicate flag fullname :%s", fullname)
		}
	}

	runner.fullname[name] = fullname
}

func (runner *_Runner) FlagString(name string, fullname string, defaultVal string, description string) Runner {

	runner.checkName(name, fullname)

	runner.flagString[name] = flag.String(name, defaultVal, description)

	return runner
}

func (runner *_Runner) FlagInt(name string, fullname string, defaultVal int64, description string) Runner {
	runner.checkName(name, fullname)

	runner.flagInt[name] = flag.Int64(name, defaultVal, description)

	return runner
}

func (runner *_Runner) FlagUint(name string, fullname string, defaultVal uint64, description string) Runner {
	runner.checkName(name, fullname)
	runner.flagUint[name] = flag.Uint64(name, defaultVal, description)

	return runner
}

func (runner *_Runner) FlagFloat32(name string, fullname string, defaultVal float32, description string) Runner {

	runner.checkName(name, fullname)

	runner.flagFloat32[name] = flag.Float64(name, float64(defaultVal), description)

	return runner

}

func (runner *_Runner) FlagFloat64(name string, fullname string, defaultVal float64, description string) Runner {
	runner.checkName(name, fullname)
	runner.flagFloat64[name] = flag.Float64(name, defaultVal, description)

	return runner
}

func (runner *_Runner) Seconds(name string, fullname string, defaultVal uint64, description string) Runner {
	runner.checkName(name, fullname)
	runner.flagSecond[name] = flag.Uint64(name, defaultVal, description)

	return runner
}

func (runner *_Runner) Milliseconds(name string, fullname string, defaultVal uint64, description string) Runner {
	runner.checkName(name, fullname)

	runner.flagMilliseconds[name] = flag.Uint64(name, defaultVal, description)

	return runner
}

func (runner *_Runner) Run(main func(runner Runner)) {
	defer func() {
		if e := recover(); e != nil {
			runner.E("catch unknown exception\n\t%s", e)
		}

		runner.I("service stopped.")

		gslogger.Join()
	}()

	flag.Parse()

	configpath := *runner.flagString["config"]

	runner.D("config file path :%s", configpath)

	if configpath != "" {
		switch filepath.Ext(configpath) {
		case ".json":
			if err := gsconfig.LoadJSON(configpath); err != nil {
				gserrors.Panicf(ErrConfig, "load config file error :%s", configpath)
			}
		default:
			runner.W("can't load config file :%s", configpath)
		}
	}

	for k, v := range runner.flagString {
		gsconfig.Update(runner.fullname[k], *v)
	}
	for k, v := range runner.flagInt {
		gsconfig.Update(runner.fullname[k], *v)
	}
	for k, v := range runner.flagUint {
		gsconfig.Update(runner.fullname[k], *v)
	}
	for k, v := range runner.flagFloat32 {
		gsconfig.Update(runner.fullname[k], *v)
	}
	for k, v := range runner.flagFloat64 {
		gsconfig.Update(runner.fullname[k], *v)
	}
	for k, v := range runner.flagSecond {
		gsconfig.Update(runner.fullname[k], *v)
	}
	for k, v := range runner.flagMilliseconds {
		gsconfig.Update(runner.fullname[k], *v)
	}

	logroot := gsconfig.String("gsrunner.log", "")
	runner.D("log root path :%s", logroot)
	loglevel := gsconfig.String("gsrunner.log.level", "")
	runner.D("log level :%s", loglevel)
	registryFile := gsconfig.String("gsrunner.registry", "")
	runner.D("registry file:%s", registryFile)

	if logroot != "" {

		fullpath, _ := filepath.Abs(logroot)

		dir := filepath.Dir(fullpath)

		name := filepath.Base(fullpath)

		gslogger.SetLogDir(dir)

		gslogger.NewSink(gslogger.NewFilelog("gschat-mailhub", name, gsconfig.Int64("gschat.mailhub.log.maxsize", 0)))
	}

	if loglevel != "" {
		gslogger.NewFlags(gslogger.ParseLevel(loglevel))
	}

	if registryFile != "" {

		runner.I("load gsrpc services registry file :%s", registryFile)

		file, err := os.Open(registryFile)

		if err != nil {
			gserrors.Panicf(err, "open registry file error :%s", registryFile)
		}

		runner.loadRegistry(file, registryFile)

		runner.I("load gsrpc services registry file :%s -- success", registryFile)
	}

	runner.I("service started.")

	main(runner)
}

var registryRegex = regexp.MustCompile(`^(?P<name>[A-Za-z0-9_](\.[A-Za-z0-9_])+)=(?P<id>[0-9]+)$`)

func (runner *_Runner) loadRegistry(file io.Reader, path string) {

	reader := bufio.NewReader(file)

	lines := 0

	items := make(map[string]uint16)

	for {
		line, err := reader.ReadString('\n')

		if err != nil {
			if err == io.EOF {
				break
			}

			gserrors.Panicf(err, "read registry file error :%s", path)
		}

		tokens := registryRegex.FindStringSubmatch(line)

		if tokens == nil {
			gserrors.Panicf(ErrRegistry, "load registry file error:\n\tinvalid format\n\t%s(%d)", path, lines)
		}

		val, err := strconv.ParseInt(tokens[1], 0, 32)

		if err != nil {
			gserrors.Panicf(err, "load registry file error:\n\tinvalid format\n\t%s(%d)", path, lines)
		}

		if val > math.MaxUint16 {
			gserrors.Panicf(ErrRegistry, "load registry file error:\n\tid out of range\n\t%s(%d)", path, lines)
		}

		items[tokens[0]] = uint16(val)

		lines++

	}

	gorpc.RegistryUpdate(items)
}
