// juju-dying-relation-repair removes the relations that keep a dying
// application or a dying consumed SaaS from going away in a Juju 3.6 model.
//
// Juju 3.6 removes a relation with no units in a transaction that asserts
// both of its applications are Alive. Once one of them is Dying that
// assertion can never hold: "juju remove-application --force",
// "juju remove-relation --force" and "juju remove-saas --force" all fail or
// change nothing, and the documents stay in the model.
//
// The tool removes every relation of the named dying application or SaaS
// in one Juju transaction: the relations and their satellite documents,
// the relation counts of the applications at their other ends, and the
// cleanups that let Juju remove dying local applications left with no
// relation. Dying SaaS left with no relation are then removed with
// "juju remove-saas --force", so Juju cleans up their remote entities and
// external controller references.
//
// It writes a JSON backup of every document it changes and does nothing
// unless -yes is given.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/juju/mgo/v3"
	"github.com/juju/mgo/v3/bson"
	"github.com/juju/mgo/v3/sstxn"

	"github.com/xtrusia/juju-dying-relation-repair/internal/ctl"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const (
	defaultBackupPath = "juju-dying-relation-repair-backup.json"
	defaultPlanPath   = "juju-dying-relation-repair-plan.json"
)

func main() {
	model := flag.String("model", "", "run from a Juju client: the model to repair, as given to juju -m")
	modelUUID := flag.String("model-uuid", "", "run on a controller machine: the UUID of the model to repair")
	application := flag.String("application", "", "required: the dying local application or consumed SaaS")
	backupPath := flag.String("backup", "", "where to write the plan with every document it changes (default "+defaultBackupPath+", or "+defaultPlanPath+" without -yes)")
	apply := flag.Bool("yes", false, "apply the changes; without this the tool only reports what it would do")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *backupPath == "" {
		*backupPath = defaultPlanPath
		if *apply {
			*backupPath = defaultBackupPath
		}
	}
	onController := ctl.OnController()
	if err := checkArgs(onController, *model, *modelUUID, *application); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		flag.Usage()
		os.Exit(2)
	}

	var err error
	if onController {
		err = run(*modelUUID, *application, *backupPath, *apply)
	} else {
		err = drive(*model, *application, *backupPath, *apply)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func checkArgs(onController bool, model, modelUUID, application string) error {
	if application == "" {
		return fmt.Errorf("-application is required")
	}
	if onController && modelUUID == "" {
		return fmt.Errorf("-model-uuid is required on a controller machine")
	}
	if !onController && model == "" {
		return fmt.Errorf("-model is required")
	}
	return nil
}

// ---------- driver (runs on a Juju client) ----------

type modelInfo struct {
	UUID           string `json:"model-uuid"`
	ControllerName string `json:"controller-name"`
}

func drive(model, application, backup string, apply bool) error {
	if abs, err := filepath.Abs(backup); err == nil {
		backup = abs
	}
	out, err := exec.Command("juju", "show-model", model, "--format", "json").Output()
	if err != nil {
		return fmt.Errorf("showing model %q: %w", model, err)
	}
	info, err := parseShowModel(out)
	if err != nil {
		return err
	}
	controllerModel := info.ControllerName + ":controller"
	runner, err := ctl.PickRunner(controllerModel)
	if err != nil {
		return err
	}
	fmt.Printf("driving through controller machine %s of %q\n", runner, info.ControllerName)

	args := []string{"-model-uuid", info.UUID, "-application", application}
	remoteFile := "/tmp/" + defaultPlanPath
	if apply {
		args = append(args, "-yes")
		remoteFile = "/tmp/" + defaultBackupPath
	}
	if err := ctl.RunOnMachine(ctl.RunParams{
		Model:        controllerModel,
		Machine:      runner,
		RemoteBin:    "/tmp/juju-dying-relation-repair",
		RemoteBackup: remoteFile,
		Args:         args,
		Apply:        apply,
		Backup:       backup,
	}); err != nil {
		return err
	}
	if !apply {
		return nil
	}

	saas, err := saasInBackup(backup)
	if err != nil {
		return fmt.Errorf("the relations were removed, but the SaaS to remove could not be read from the backup: %w; run 'juju remove-saas -m %s <saas> --force --no-wait' for each dying SaaS", err, model)
	}
	for _, name := range saas {
		fmt.Printf("removing SaaS %s: juju remove-saas -m %s %s --force --no-wait\n", name, model, name)
		if err := ctl.JujuStream("remove-saas", "-m", model, name, "--force", "--no-wait"); err != nil {
			return fmt.Errorf("removing SaaS %s: %w", name, err)
		}
	}
	names := []string{application}
	for _, name := range saas {
		if name != application {
			names = append(names, name)
		}
	}
	fmt.Printf("\ndone. Watch 'juju status -m %s' until %s disappear.\n", model, strings.Join(names, ", "))
	return nil
}

func parseShowModel(out []byte) (*modelInfo, error) {
	var models map[string]modelInfo
	if err := json.Unmarshal(out, &models); err != nil {
		return nil, fmt.Errorf("parsing juju show-model: %w", err)
	}
	if len(models) != 1 {
		return nil, fmt.Errorf("juju show-model returned %d models, want 1", len(models))
	}
	for _, info := range models {
		if info.UUID == "" || info.ControllerName == "" {
			return nil, fmt.Errorf("juju show-model did not report the model UUID and controller name")
		}
		return &info, nil
	}
	return nil, nil
}

func saasInBackup(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p plan
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	if p.Application == "" {
		return nil, fmt.Errorf("%s holds no repair plan", path)
	}
	return p.RemoveSaaS, nil
}

// ---------- on the controller ----------

func run(modelUUID, application, backup string, apply bool) error {
	agentConf, err := ctl.FindAgentConf()
	if err != nil {
		return err
	}
	conf, err := ctl.LoadAgentConf(agentConf)
	if err != nil {
		return err
	}
	localTag := filepath.Base(filepath.Dir(agentConf))
	session, err := ctl.DialMongo(localTag, conf, ctl.DefaultMongoCert)
	if err != nil {
		return err
	}
	defer session.Close()
	db := session.DB(ctl.StateDB)
	st := mongoStore{db: db}

	p, err := planRepair(st, modelUUID, application)
	if err != nil {
		return err
	}
	printPlan(os.Stdout, p)
	if err := ctl.WriteBackup(backup, p); err != nil {
		return err
	}
	if !apply {
		fmt.Printf("\ndry run: nothing was changed. The plan and the %d documents it would change, as they are now, are in %s. Re-run with -yes to apply.\n", backedUp(p), backup)
		return nil
	}
	fmt.Printf("\nbackup of %d documents written to %s\n", backedUp(p), backup)
	fresh, err := planRepair(st, modelUUID, application)
	if err != nil {
		return fmt.Errorf("revalidating the plan before writing: %w", err)
	}
	if !samePlan(p, fresh) {
		return fmt.Errorf("Mongo state changed after planning; no changes were applied, retry the command")
	}
	if err := sstxn.NewRunner(db, nil).Run(repairOps(p), "", nil); err != nil {
		return fmt.Errorf("applying the repair transaction: %w", err)
	}
	fmt.Printf("mongo: %d relations of %s removed\n", len(p.Relations), p.Application)
	for _, saas := range p.RemoveSaaS {
		fmt.Printf("\nnext: juju remove-saas -m %s %s --force --no-wait\n", p.Model, saas)
	}
	return nil
}

// samePlan compares two plans without the documents of the changes that
// assert no txn-revno: Juju keeps rewriting those, and their changes assert
// every field they rely on.
func samePlan(a, b *plan) bool {
	return reflect.DeepEqual(withoutLiveDocs(a), withoutLiveDocs(b))
}

func withoutLiveDocs(p *plan) plan {
	out := *p
	out.Changes = make([]change, len(p.Changes))
	for i, c := range p.Changes {
		if _, ok := c.Assert["txn-revno"]; !ok {
			c.Doc = nil
		}
		out.Changes[i] = c
	}
	return out
}

func backedUp(p *plan) int {
	n := 0
	for _, c := range p.Changes {
		if c.Doc != nil {
			n++
		}
	}
	return n
}

func printPlan(w io.Writer, p *plan) {
	fmt.Fprintf(w, "model %s (%s)\n", p.Model, p.ModelUUID)
	fmt.Fprintf(w, "%s: Dying, removing its %d relations\n", p.Application, len(p.Relations))
	for _, r := range p.Relations {
		fmt.Fprintf(w, "relation %q (id %d): Dying\n", r.Key, r.ID)
	}
	fmt.Fprintf(w, "\ntransaction:\n")
	for _, c := range p.Changes {
		switch {
		case c.Insert != nil:
			fmt.Fprintf(w, "  insert %s: %s\n", c.Collection, describeFields(c.Insert))
		case c.Remove:
			fmt.Fprintf(w, "  remove %s/%s\n", c.Collection, c.ID)
		default:
			fmt.Fprintf(w, "  update %s/%s: %s\n", c.Collection, c.ID, describeUpdate(c.Update))
		}
		if len(c.Assert) > 0 {
			fmt.Fprintf(w, "    if %s\n", describeFields(c.Assert))
		}
	}
	for _, saas := range p.RemoveSaaS {
		fmt.Fprintf(w, "then: juju remove-saas -m %s %s --force --no-wait\n", p.Model, saas)
	}
}

func describeUpdate(update bson.M) string {
	var parts []string
	for _, op := range sortedKeys(update) {
		fields, _ := asMap(update[op])
		for _, field := range sortedKeys(fields) {
			parts = append(parts, fmt.Sprintf("%s %s %v", op, field, fields[field]))
		}
	}
	return strings.Join(parts, ", ")
}

// describeFields shows field values only. The documents go to the plan
// file, since relation settings can hold credentials.
func describeFields(fields map[string]interface{}) string {
	var parts []string
	for _, k := range sortedKeys(fields) {
		v := fields[k]
		if t, ok := v.(time.Time); ok && t.IsZero() {
			// Juju calls a zero cleanup time asap: due at once.
			v = "asap"
		}
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	return strings.Join(parts, ", ")
}

// ---------- mongo ----------

type mongoStore struct {
	db *mgo.Database
}

func (s mongoStore) doc(collection, id string) (map[string]interface{}, error) {
	var doc map[string]interface{}
	err := s.db.C(collection).FindId(id).One(&doc)
	if err == mgo.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading %s/%s: %w", collection, id, err)
	}
	return doc, nil
}

func (s mongoStore) docs(collection, modelUUID, field string, value interface{}) ([]map[string]interface{}, error) {
	return s.find(collection, bson.D{{Name: "model-uuid", Value: modelUUID}, {Name: field, Value: value}})
}

func (s mongoStore) docsWithField(collection, modelUUID, field string) ([]map[string]interface{}, error) {
	return s.find(collection, bson.D{{Name: "model-uuid", Value: modelUUID}, {Name: field, Value: bson.M{"$exists": true}}})
}

func (s mongoStore) docsWithPrefix(collection, prefix string) ([]map[string]interface{}, error) {
	return s.find(collection, bson.M{"_id": bson.RegEx{Pattern: "^" + regexp.QuoteMeta(prefix)}})
}

func (s mongoStore) find(collection string, query interface{}) ([]map[string]interface{}, error) {
	var docs []map[string]interface{}
	if err := s.db.C(collection).Find(query).Sort("_id").All(&docs); err != nil {
		return nil, fmt.Errorf("reading %s: %w", collection, err)
	}
	return docs, nil
}
