package main

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/juju/mgo/v3/bson"
	"github.com/juju/mgo/v3/txn"
	"gopkg.in/yaml.v3"
)

const testModel = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

// fakeStore answers store queries from a "juju dump-db" style document map.
type fakeStore map[string][]map[string]interface{}

func loadDump(t *testing.T) fakeStore {
	t.Helper()
	data, err := os.ReadFile("testdata/example-dump.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	st := fakeStore{}
	for collection, v := range raw {
		switch docs := v.(type) {
		case []interface{}:
			for _, d := range docs {
				st[collection] = append(st[collection], d.(map[string]interface{}))
			}
		case map[string]interface{}:
			st[collection] = []map[string]interface{}{docs}
		}
	}
	return st
}

func (f fakeStore) find(collection string, match func(map[string]interface{}) bool) []map[string]interface{} {
	var out []map[string]interface{}
	for _, d := range f[collection] {
		if match(d) {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["_id"].(string) < out[j]["_id"].(string) })
	return out
}

func (f fakeStore) doc(collection, id string) (map[string]interface{}, error) {
	docs := f.find(collection, func(d map[string]interface{}) bool { return d["_id"] == id })
	if len(docs) == 0 {
		return nil, nil
	}
	return docs[0], nil
}

func (f fakeStore) docs(collection, modelUUID, field string, value interface{}) ([]map[string]interface{}, error) {
	return f.find(collection, func(d map[string]interface{}) bool {
		return d["model-uuid"] == modelUUID && fieldMatches(d, strings.Split(field, "."), value)
	}), nil
}

func (f fakeStore) docsWithField(collection, modelUUID, field string) ([]map[string]interface{}, error) {
	return f.find(collection, func(d map[string]interface{}) bool {
		return d["model-uuid"] == modelUUID && fieldMatches(d, strings.Split(field, "."), nil)
	}), nil
}

func (f fakeStore) docsWithPrefix(collection, prefix string) ([]map[string]interface{}, error) {
	return f.find(collection, func(d map[string]interface{}) bool {
		return strings.HasPrefix(d["_id"].(string), prefix)
	}), nil
}

// fieldMatches follows a dotted path like MongoDB, through arrays of
// documents. A nil value only checks that the field exists.
func fieldMatches(doc map[string]interface{}, path []string, value interface{}) bool {
	v, ok := doc[path[0]]
	if !ok {
		return false
	}
	if len(path) == 1 {
		return value == nil || sameValue(v, value)
	}
	if list, ok := v.([]interface{}); ok {
		for _, item := range list {
			if m, ok := asMap(item); ok && fieldMatches(m, path[1:], value) {
				return true
			}
		}
		return false
	}
	m, ok := asMap(v)
	return ok && fieldMatches(m, path[1:], value)
}

// pythonOp renders an op the way scripts/check-db-orphans.py writes it for
// mgo-run-txn, without the generated cleanup _id and the "when" field the
// script leaves to Juju's default.
func pythonOp(op txn.Op) map[string]interface{} {
	out := map[string]interface{}{"c": op.C}
	if op.Insert != nil {
		insert := map[string]interface{}{}
		for k, v := range op.Insert.(bson.M) {
			if k != "when" {
				insert[k] = v
			}
		}
		out["a"], out["i"] = "d-", insert
		return out
	}
	out["d"] = op.Id
	assert := map[string]interface{}{}
	for _, e := range op.Assert.(bson.D) {
		assert[e.Name] = e.Value
	}
	out["a"] = assert
	if op.Remove {
		out["r"] = true
	}
	if op.Update != nil {
		out["u"] = op.Update
	}
	return out
}

func normalize(t *testing.T, v interface{}) interface{} {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out interface{}
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPlanMatchesPythonScript(t *testing.T) {
	p, err := planRepair(loadDump(t), testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	var got []map[string]interface{}
	for _, op := range repairOps(p) {
		got = append(got, pythonOp(op))
	}
	data, err := os.ReadFile("testdata/example-repair.json")
	if err != nil {
		t.Fatal(err)
	}
	var want []map[string]interface{}
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	for _, op := range want {
		if op["c"] == "cleanups" {
			delete(op, "d")
		}
	}
	if g, w := normalize(t, got), normalize(t, want); !reflect.DeepEqual(g, w) {
		gj, _ := json.MarshalIndent(g, "", " ")
		t.Fatalf("ops differ from check-db-orphans.py:\n%s", gj)
	}
	if !reflect.DeepEqual(p.RemoveSaaS, []string{"postgres"}) || len(p.Relations) != 1 || p.Relations[0].ID != 4 || p.Model != "/" {
		t.Fatalf("plan identity = %v %v %q", p.RemoveSaaS, p.Relations, p.Model)
	}
}

func TestPlanIsStableForRevalidation(t *testing.T) {
	st := loadDump(t)
	first, err := planRepair(st, testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := planRepair(st, testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("planning the same state twice gave different plans")
	}
	changed := loadDump(t)
	changed["relations"][0]["txn-revno"] = 10
	third, err := planRepair(changed, testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(first, third) {
		t.Fatal("a relation changed after planning must make the plans differ")
	}
}

func TestRepairOpsInsertCleanupWithNewID(t *testing.T) {
	p, err := planRepair(loadDump(t), testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	cleanupID := func() interface{} {
		for _, op := range repairOps(p) {
			if op.C == "cleanups" {
				if op.Assert != txn.DocMissing || !strings.HasPrefix(op.Id.(string), testModel+":") {
					t.Fatalf("cleanup op = %#v", op)
				}
				return op.Id
			}
		}
		t.Fatal("no cleanup op")
		return nil
	}
	if cleanupID() == cleanupID() {
		t.Fatal("each transaction must insert a cleanup with a new _id")
	}
}

func TestPlanRemovesOnlyThisRelationsDocuments(t *testing.T) {
	st := loadDump(t)
	key := "app1:database postgres:database"
	add := func(collection string, doc map[string]interface{}) {
		doc["model-uuid"] = testModel
		doc["txn-revno"] = 3
		st[collection] = append(st[collection], doc)
	}
	add("relationNetworks", map[string]interface{}{"_id": testModel + ":" + key + ":ingress:default"})
	add("relationNetworks", map[string]interface{}{"_id": testModel + ":" + key + "2:ingress:default"})
	add("secretPermissions", map[string]interface{}{"_id": testModel + ":secret#1", "scope-tag": relationTag(key)})
	add("secretPermissions", map[string]interface{}{"_id": testModel + ":secret#2", "scope-tag": "relation-other"})
	add("applicationOfferConnections", map[string]interface{}{"_id": testModel + ":conn-4", "relation-id": 4})
	add("applicationOfferConnections", map[string]interface{}{"_id": testModel + ":conn-40", "relation-id": 40})

	p, err := planRepair(st, testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	removed := map[string]bool{}
	for _, c := range p.Changes {
		if c.Remove {
			removed[c.Collection+"/"+c.ID] = true
		}
	}
	for _, want := range []string{
		"relationNetworks/" + testModel + ":" + key + ":ingress:default",
		"secretPermissions/" + testModel + ":secret#1",
		"applicationOfferConnections/" + testModel + ":conn-4",
		"settings/" + testModel + ":r#4#app1",
	} {
		if !removed[want] {
			t.Errorf("%s was not removed", want)
		}
	}
	for _, keep := range []string{
		"relationNetworks/" + testModel + ":" + key + "2:ingress:default",
		"secretPermissions/" + testModel + ":secret#2",
		"applicationOfferConnections/" + testModel + ":conn-40",
		"settings/" + testModel + ":r#40#other",
	} {
		if removed[keep] {
			t.Errorf("%s belongs to another relation but was removed", keep)
		}
	}
}

func TestPlanRefusesOtherStates(t *testing.T) {
	set := func(collection, id, field string, value interface{}) func(fakeStore) {
		return func(st fakeStore) {
			for _, d := range st[collection] {
				if d["_id"] == testModel+":"+id {
					d[field] = value
				}
			}
		}
	}
	tests := []struct {
		name    string
		change  func(fakeStore)
		wantErr string
	}{
		{"alive application", set("applications", "app1", "life", 0), "life=0"},
		{"application with units", set("applications", "app1", "unitcount", 1), "unitcount=1"},
		{"unit document left", func(st fakeStore) {
			st["units"] = append(st["units"], map[string]interface{}{"_id": testModel + ":app1/0", "model-uuid": testModel, "application": "app1"})
		}, "unit documents"},
		{"second relation that is alive", func(st fakeStore) {
			st["relations"] = append(st["relations"], map[string]interface{}{
				"_id": testModel + ":app1:admin postgres:admin", "model-uuid": testModel, "id": 5, "key": "app1:admin postgres:admin", "life": 0,
				"endpoints": []interface{}{map[string]interface{}{"applicationname": "app1"}, map[string]interface{}{"applicationname": "postgres"}},
			})
		}, "life=0"},
		{"endpoint that does not exist", func(st fakeStore) {
			st["relations"][0]["endpoints"] = []interface{}{map[string]interface{}{"applicationname": "app1"}, map[string]interface{}{"applicationname": "ghost"}}
		}, "neither an application nor a SaaS"},
		{"alive relation", set("relations", "app1:database postgres:database", "life", 0), "life=0"},
		{"relation without a unit count", set("relations", "app1:database postgres:database", "unitcount", true), "no integer unitcount"},
		{"consumer proxy", set("remoteApplications", "postgres", "is-consumer-proxy", true), "is-consumer-proxy=true"},
		{"consumer proxy endpoint", func(st fakeStore) {
			st["remoteApplications"][0]["is-consumer-proxy"] = true
		}, "consumer proxies on the offering side"},
		{"missing txn-revno", func(st fakeStore) { delete(st["settings"][0], "txn-revno") }, "no integer txn-revno"},
		{"boolean count", set("applications", "app1", "relationcount", true), "no integer relationcount"},
		{"fractional life", set("applications", "app1", "life", 1.5), "life=1.5"},
		{"document of another model", set("statuses", "r#4", "model-uuid", "other"), "model-uuid other"},
		{"unknown application", func(st fakeStore) { delete(st, "applications") }, "not found"},
		{"missing consumer proxy field", func(st fakeStore) { delete(st["remoteApplications"][0], "is-consumer-proxy") }, "has no is-consumer-proxy field"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			st := loadDump(t)
			test.change(st)
			_, err := planRepair(st, testModel, "app1")
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("got error %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestPlanAcceptsWholeDoubles(t *testing.T) {
	want, err := planRepair(loadDump(t), testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	st := loadDump(t)
	for _, collection := range []string{"applications", "remoteApplications", "relations"} {
		for _, d := range st[collection] {
			for _, field := range []string{"life", "unitcount", "relationcount"} {
				if n, ok := asInt(d[field]); ok {
					d[field] = float64(n)
				}
			}
		}
	}
	got, err := planRepair(st, testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Changes) != len(want.Changes) || len(got.Relations) != len(want.Relations) {
		t.Fatalf("got %d changes for %d relations, want %d for %d", len(got.Changes), len(got.Relations), len(want.Changes), len(want.Relations))
	}
}

func TestPrintPlanShowsTransactionAndNextStep(t *testing.T) {
	p, err := planRepair(loadDump(t), testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	printPlan(&out, p)
	for _, want := range []string{
		"remove relations/" + testModel + ":app1:database postgres:database",
		"update applications/" + testModel + ":app1: $inc relationcount -1",
		"insert cleanups: args=[false false], kind=application, model-uuid=" + testModel + ", prefix=app1, when=asap",
		"    if id=4, key=app1:database postgres:database, life=1, txn-revno=",
		"    if life=1, name=app1, relationcount=1, txn-revno=",
		"update unitstates/" + testModel + ":u#app1/0#charm: $unset relation-state.4 1",
		"then: juju remove-saas -m / postgres --force --no-wait",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("plan output lacks %q:\n%s", want, out.String())
		}
	}
}

func TestParseShowModel(t *testing.T) {
	info, err := parseShowModel([]byte(`{"prod":{"model-uuid":"` + testModel + `","controller-name":"ctl","owner":"admin"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if info.UUID != testModel || info.ControllerName != "ctl" {
		t.Fatalf("parsed %+v", info)
	}
	if _, err := parseShowModel([]byte(`{}`)); err == nil {
		t.Fatal("expected an empty show-model result to be rejected")
	}
}

func TestCheckArgs(t *testing.T) {
	for _, test := range []struct {
		onController            bool
		model, uuid, app, error string
	}{
		{false, "prod", "", "", "-application"},
		{false, "", "", "app1", "-model is required"},
		{true, "", "", "app1", "-model-uuid"},
		{false, "prod", "", "app1", ""},
		{true, "", testModel, "app1", ""},
	} {
		err := checkArgs(test.onController, test.model, test.uuid, test.app)
		if (err == nil) != (test.error == "") || (err != nil && !strings.Contains(err.Error(), test.error)) {
			t.Errorf("checkArgs(%v, %q, %q, %q) = %v, want %q", test.onController, test.model, test.uuid, test.app, err, test.error)
		}
	}
}

// addDoc adds a document of the test model with a txn-revno.
func addDoc(st fakeStore, collection string, doc map[string]interface{}) {
	doc["_id"] = testModel + ":" + doc["_id"].(string)
	doc["model-uuid"] = testModel
	if _, ok := doc["txn-revno"]; !ok {
		doc["txn-revno"] = 3
	}
	st[collection] = append(st[collection], doc)
}

func setField(st fakeStore, collection, id, field string, value interface{}) {
	for _, d := range st[collection] {
		if d["_id"] == testModel+":"+id {
			d[field] = value
		}
	}
}

// addLocalRelation adds app2, an alive local application, and a dying
// relation with id 5 between it and app1.
func addLocalRelation(st fakeStore) string {
	key := "app1:db app2:db"
	addDoc(st, "applications", map[string]interface{}{"_id": "app2", "name": "app2", "life": 0, "unitcount": 1, "relationcount": 1})
	addDoc(st, "relations", map[string]interface{}{
		"_id": key, "id": 5, "key": key, "life": 1, "unitcount": 0,
		"endpoints": []interface{}{map[string]interface{}{"applicationname": "app1"}, map[string]interface{}{"applicationname": "app2"}},
	})
	addDoc(st, "settings", map[string]interface{}{"_id": "r#5#app2"})
	addDoc(st, "statuses", map[string]interface{}{"_id": "r#5"})
	setField(st, "applications", "app1", "relationcount", 2)
	return key
}

// changesByID indexes the plan's changes by collection and _id.
func changesByID(p *plan) map[string]change {
	out := map[string]change{}
	for _, c := range p.Changes {
		if c.Insert != nil {
			out[c.Collection+":"+c.Insert["kind"].(string)+" "+c.Insert["prefix"].(string)] = c
			continue
		}
		out[c.Collection+"/"+c.ID] = c
	}
	return out
}

func wantUpdate(t *testing.T, changes map[string]change, id string, want bson.M) {
	t.Helper()
	c, ok := changes[id]
	if !ok {
		t.Fatalf("no change for %s", id)
	}
	if g, w := normalize(t, c.Update), normalize(t, want); !reflect.DeepEqual(g, w) {
		t.Errorf("update of %s = %v, want %v", id, g, w)
	}
}

func TestPlanRemovesEveryRelation(t *testing.T) {
	st := loadDump(t)
	key := addLocalRelation(st)
	p, err := planRepair(st, testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Relations) != 2 || p.Relations[1] != (relationRef{Key: key, ID: 5}) {
		t.Fatalf("relations = %v", p.Relations)
	}
	changes := changesByID(p)
	for _, id := range []string{
		"relations/" + testModel + ":" + key,
		"settings/" + testModel + ":r#5#app2",
		"statuses/" + testModel + ":r#5",
	} {
		if !changes[id].Remove {
			t.Errorf("%s is not removed", id)
		}
	}
	wantUpdate(t, changes, "applications/"+testModel+":app1", bson.M{"$inc": bson.M{"relationcount": -2}})
	wantUpdate(t, changes, "applications/"+testModel+":app2", bson.M{"$inc": bson.M{"relationcount": -1}})
	if _, ok := changes["cleanups:application app1"]; !ok {
		t.Error("no cleanup for app1")
	}
	if _, ok := changes["cleanups:application app2"]; ok {
		t.Error("alive app2 got a cleanup")
	}
	if !reflect.DeepEqual(p.RemoveSaaS, []string{"postgres"}) {
		t.Errorf("RemoveSaaS = %v", p.RemoveSaaS)
	}
	if a := changes["applications/"+testModel+":app2"].Assert; a["life"] != int64(0) || a["relationcount"] != int64(1) {
		t.Errorf("app2 assert = %v", a)
	}
}

func TestPlanRemovesScopesOfUnitsThatAreGone(t *testing.T) {
	st := loadDump(t)
	setField(st, "relations", "app1:database postgres:database", "unitcount", 2)
	addDoc(st, "relationscopes", map[string]interface{}{"_id": "r#4#requirer#app1/0"})
	addDoc(st, "relationscopes", map[string]interface{}{"_id": "r#4#provider#postgres/0"})
	addDoc(st, "relationscopes", map[string]interface{}{"_id": "r#40#provider#other/0"})
	p, err := planRepair(st, testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	changes := changesByID(p)
	for _, id := range []string{"r#4#requirer#app1/0", "r#4#provider#postgres/0"} {
		if !changes["relationscopes/"+testModel+":"+id].Remove {
			t.Errorf("scope %s is not removed", id)
		}
	}
	if _, ok := changes["relationscopes/"+testModel+":r#40#provider#other/0"]; ok {
		t.Error("a scope of another relation is removed")
	}
	rel := changes["relations/"+testModel+":app1:database postgres:database"]
	if rel.Assert["unitcount"] != int64(2) {
		t.Errorf("relation assert = %v, want the unit count as read", rel.Assert)
	}
}

func TestPlanRefusesUnitStillInScope(t *testing.T) {
	st := loadDump(t)
	addLocalRelation(st)
	addDoc(st, "units", map[string]interface{}{"_id": "app2/0", "application": "app2"})
	addDoc(st, "relationscopes", map[string]interface{}{"_id": "r#5#provider#app2/0"})
	_, err := planRepair(st, testModel, "app1")
	if err == nil || !strings.Contains(err.Error(), "unit app2/0 is still in scope") {
		t.Fatalf("got error %v", err)
	}
}

func TestPlanSetsDriftedRelationCounts(t *testing.T) {
	st := loadDump(t)
	setField(st, "remoteApplications", "postgres", "relationcount", 2)
	setField(st, "applications", "app1", "relationcount", 0)
	p, err := planRepair(st, testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	changes := changesByID(p)
	wantUpdate(t, changes, "applications/"+testModel+":app1", bson.M{"$set": bson.M{"relationcount": 0}})
	wantUpdate(t, changes, "remoteApplications/"+testModel+":postgres", bson.M{"$set": bson.M{"relationcount": 0}})
	if a := changes["remoteApplications/"+testModel+":postgres"].Assert; a["relationcount"] != int64(2) {
		t.Errorf("postgres assert = %v, want the count as read", a)
	}
	if !reflect.DeepEqual(p.RemoveSaaS, []string{"postgres"}) {
		t.Errorf("RemoveSaaS = %v", p.RemoveSaaS)
	}
}

func TestPlanSetsCountOfApplicationWithoutRelations(t *testing.T) {
	st := loadDump(t)
	delete(st, "relations")
	p, err := planRepair(st, testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	changes := changesByID(p)
	if len(p.Changes) != 2 {
		t.Fatalf("changes = %v", p.Changes)
	}
	wantUpdate(t, changes, "applications/"+testModel+":app1", bson.M{"$set": bson.M{"relationcount": 0}})
	if _, ok := changes["cleanups:application app1"]; !ok {
		t.Error("no cleanup for app1")
	}

	setField(st, "applications", "app1", "relationcount", 0)
	if _, err := planRepair(st, testModel, "app1"); err == nil || !strings.Contains(err.Error(), "nothing to repair") {
		t.Fatalf("got error %v", err)
	}
}

func TestPlanForDyingSaaS(t *testing.T) {
	st := loadDump(t)
	setField(st, "applications", "app1", "life", 0)
	p, err := planRepair(st, testModel, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	changes := changesByID(p)
	if !changes["relations/"+testModel+":app1:database postgres:database"].Remove {
		t.Error("the relation is not removed")
	}
	if _, ok := changes["cleanups:application app1"]; ok {
		t.Error("alive app1 got a cleanup")
	}
	if a := changes["applications/"+testModel+":app1"].Assert; a["unitcount"] != nil {
		t.Errorf("app1 assert = %v: only the dying target is asserted to have no units", a)
	}
	if !reflect.DeepEqual(p.RemoveSaaS, []string{"postgres"}) {
		t.Errorf("RemoveSaaS = %v", p.RemoveSaaS)
	}
}

func TestPlanKeepsAliveSaaS(t *testing.T) {
	st := loadDump(t)
	setField(st, "remoteApplications", "postgres", "life", 0)
	p, err := planRepair(st, testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.RemoveSaaS) != 0 {
		t.Errorf("RemoveSaaS = %v, want the alive SaaS kept", p.RemoveSaaS)
	}
	wantUpdate(t, changesByID(p), "remoteApplications/"+testModel+":postgres", bson.M{"$inc": bson.M{"relationcount": -1}})
}

func TestPlanRemovesWholeRelationStateWhenEmptied(t *testing.T) {
	st := loadDump(t)
	st["unitstates"][0]["relation-state"] = map[string]interface{}{"4": map[string]interface{}{}}
	p, err := planRepair(st, testModel, "app1")
	if err != nil {
		t.Fatal(err)
	}
	wantUpdate(t, changesByID(p), "unitstates/"+testModel+":u#app1/0#charm", bson.M{"$unset": bson.M{"relation-state": 1}})
}
