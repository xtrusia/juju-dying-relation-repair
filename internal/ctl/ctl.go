// Package ctl holds what the tool needs to act on a Juju 3.6 controller
// machine: its agent.conf, a TLS session to the Juju MongoDB, running the
// tool there from a Juju client, transaction checks and private JSON backups.
package ctl

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/juju/mgo/v3"
	"gopkg.in/yaml.v3"
)

const (
	agentConfGlob    = "/var/lib/juju/agents/machine-*/agent.conf"
	defaultStatePort = 37017

	DefaultMongoCert = "/var/snap/juju-db/common/server.pem"
	MongoServerName  = "juju-mongodb"
	StateDB          = "juju"
)

type AgentConf struct {
	CACert         string `yaml:"cacert"`
	ControllerCert string `yaml:"controllercert"`
	ControllerKey  string `yaml:"controllerkey"`
	StatePassword  string `yaml:"statepassword"`
	ModelTag       string `yaml:"model"`
	ModelUUID      string `yaml:"-"`
	StatePortConf  int    `yaml:"stateport"`
}

func (c *AgentConf) StatePort() int {
	if c.StatePortConf > 0 {
		return c.StatePortConf
	}
	return defaultStatePort
}

func OnController() bool {
	m, _ := filepath.Glob(agentConfGlob)
	return len(m) > 0
}

func FindAgentConf() (string, error) {
	matches, err := filepath.Glob(agentConfGlob)
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no agent.conf under /var/lib/juju/agents; run this on a controller machine")
	}
	return matches[0], nil
}

func LoadAgentConf(path string) (*AgentConf, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var conf AgentConf
	if err := yaml.Unmarshal(data, &conf); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if conf.ControllerCert == "" || conf.StatePassword == "" || conf.ModelTag == "" {
		return nil, fmt.Errorf("%s is not a controller agent.conf", path)
	}
	const modelTagPrefix = "model-"
	if !strings.HasPrefix(conf.ModelTag, modelTagPrefix) || len(conf.ModelTag) == len(modelTagPrefix) {
		return nil, fmt.Errorf("%s has an invalid model tag", path)
	}
	conf.ModelUUID = strings.TrimPrefix(conf.ModelTag, modelTagPrefix)
	return &conf, nil
}

func MongoTLSConfig(conf *AgentConf, certPath string) (*tls.Config, error) {
	source := filepath.Join(filepath.Dir(certPath), "ca.crt")
	caPEM, err := os.ReadFile(source)
	if errors.Is(err, os.ErrNotExist) {
		if conf.CACert != "" {
			source, caPEM, err = "agent.conf cacert", []byte(conf.CACert), nil
		} else {
			source = certPath
			caPEM, err = os.ReadFile(source)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("reading mongo CA: %w", err)
	}
	pool := x509.NewCertPool()
	caCount := 0
	for len(caPEM) > 0 {
		block, rest := pem.Decode(caPEM)
		if block == nil {
			break
		}
		caPEM = rest
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing mongo CA from %s: %w", source, err)
		}
		if cert.IsCA {
			pool.AddCert(cert)
			caCount++
		}
	}
	if caCount == 0 {
		return nil, fmt.Errorf("no CA certificate found in %s", source)
	}
	cert, err := tls.LoadX509KeyPair(certPath, certPath)
	if err != nil {
		return nil, fmt.Errorf("loading mongo client certificate: %w", err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
		ServerName:   MongoServerName,
	}, nil
}

func DialMongo(localTag string, conf *AgentConf, certPath string) (*mgo.Session, error) {
	tlsConfig, err := MongoTLSConfig(conf, certPath)
	if err != nil {
		return nil, err
	}
	addr := net.JoinHostPort("127.0.0.1", fmt.Sprint(conf.StatePort()))
	info := &mgo.DialInfo{
		// Seed with the local mongod but let mgo discover the rest of the
		// replica set, so writes are routed to the primary even when the tool
		// runs on a secondary controller.
		Addrs:    []string{addr},
		Direct:   false,
		Timeout:  20 * time.Second,
		Database: "admin",
		Username: localTag,
		Password: conf.StatePassword,
		DialServer: func(server *mgo.ServerAddr) (net.Conn, error) {
			c, err := net.DialTimeout("tcp", server.TCPAddr().String(), 15*time.Second)
			if err != nil {
				return nil, err
			}
			tc := tls.Client(c, tlsConfig)
			if err := tc.Handshake(); err != nil {
				c.Close()
				return nil, err
			}
			return tc, nil
		},
	}
	session, err := mgo.DialWithInfo(info)
	if err != nil {
		return nil, fmt.Errorf("connecting to the mongo primary through %s: %w", addr, err)
	}
	session.SetMode(mgo.Strong, true)
	return session, nil
}

// PickRunner returns the id of the first started controller machine, sorted
// for determinism.
func PickRunner(model string) (string, error) {
	out, err := exec.Command("juju", "status", "-m", model, "--format", "json").Output()
	if err != nil {
		return "", fmt.Errorf("getting controller status: %w", err)
	}
	var s struct {
		Machines map[string]struct {
			JujuStatus struct {
				Current string `json:"current"`
			} `json:"juju-status"`
		} `json:"machines"`
	}
	if err := json.Unmarshal(out, &s); err != nil {
		return "", fmt.Errorf("parsing controller status: %w", err)
	}
	var ids []string
	for id := range s.Machines {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if s.Machines[id].JujuStatus.Current == "started" {
			return id, nil
		}
	}
	return "", fmt.Errorf("no started controller machine found")
}

func Juju(args ...string) error {
	out, err := exec.Command("juju", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("juju %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func JujuStream(args ...string) error {
	cmd := exec.Command("juju", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

type RunParams struct {
	Model        string
	Machine      string
	RemoteBin    string
	RemoteBackup string
	// Args are passed to the remote run. "-backup RemoteBackup" is added.
	Args []string
	// Backup is where the file the run writes to RemoteBackup is fetched to,
	// after a successful run or, with Apply, after any run.
	Apply  bool
	Backup string
}

func RunOnMachine(p RunParams) error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("finding own path: %w", err)
	}
	keepRemoteBackup := false

	fmt.Println("copying tool to the controller...")
	if err := Juju("scp", "-m", p.Model, self, p.Machine+":"+p.RemoteBin); err != nil {
		return fmt.Errorf("copying tool: %w", err)
	}
	defer func() {
		cleanup := "sudo rm -f " + p.RemoteBin
		if !keepRemoteBackup {
			cleanup += " " + p.RemoteBackup
		}
		if err := Juju("ssh", "-m", p.Model, p.Machine, cleanup); err != nil {
			fmt.Fprintf(os.Stderr, "warning: leaving %s on %s: %v\n", p.RemoteBin, p.Machine, err)
		}
	}()

	remote := "sudo chmod +x " + p.RemoteBin + " && sudo " + p.RemoteBin
	for _, arg := range append(p.Args, "-backup", p.RemoteBackup) {
		remote += " " + shellQuote(arg)
	}

	fmt.Println("running on the controller:")
	fmt.Println("----")
	runErr := JujuStream("ssh", "-m", p.Model, p.Machine, remote)
	fmt.Println("----")

	if p.Apply || runErr == nil {
		owner := `sudo chown "$(id -u):$(id -g)" ` + p.RemoteBackup
		if err := Juju("ssh", "-m", p.Model, p.Machine, owner); err != nil {
			keepRemoteBackup = true
			fmt.Fprintf(os.Stderr, "warning: could not prepare %s for fetch: %v; it is retained at %s:%s\n", p.RemoteBackup, err, p.Machine, p.RemoteBackup)
		} else if err := Juju("scp", "-m", p.Model, p.Machine+":"+p.RemoteBackup, p.Backup); err != nil {
			keepRemoteBackup = true
			fmt.Fprintf(os.Stderr, "warning: could not fetch %s: %v; it is retained at %s:%s\n", p.RemoteBackup, err, p.Machine, p.RemoteBackup)
		} else {
			fmt.Printf("fetched %s\n", p.Backup)
		}
	}
	if runErr != nil {
		return fmt.Errorf("running tool on %s: %w", p.Machine, runErr)
	}
	return nil
}

func shellQuote(s string) string {
	if s != "" && strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./:") == "" {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// AssertNoPendingTxn refuses to touch a document that mgo/txn is mid-way
// through changing; racing an in-flight transaction would corrupt state.
func AssertNoPendingTxn(doc map[string]interface{}, coll, id string) error {
	q, ok := doc["txn-queue"]
	if !ok || q == nil {
		return nil
	}
	if arr, ok := q.([]interface{}); ok && len(arr) == 0 {
		return nil
	}
	return fmt.Errorf("%s/%s has a pending transaction (txn-queue not empty); retry once Juju settles", coll, id)
}

func WriteBackup(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return err
	}
	return os.Chmod(path, 0o600)
}
