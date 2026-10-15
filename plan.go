package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/juju/mgo/v3/bson"
	"github.com/juju/mgo/v3/txn"

	"github.com/xtrusia/juju-dying-relation-repair/internal/ctl"
)

const lifeDying = 1

// store reads Juju state. Every method returns documents sorted by _id, and
// a dotted field matches inside arrays of documents, as in MongoDB.
type store interface {
	doc(collection, id string) (map[string]interface{}, error)
	docs(collection, modelUUID, field string, value interface{}) ([]map[string]interface{}, error)
	docsWithField(collection, modelUUID, field string) ([]map[string]interface{}, error)
	docsWithPrefix(collection, prefix string) ([]map[string]interface{}, error)
}

type plan struct {
	ModelUUID   string        `json:"model_uuid"`
	Model       string        `json:"model"`
	Application string        `json:"application"`
	Relations   []relationRef `json:"relations"`
	// RemoveSaaS lists the dying SaaS left with no relation. Juju 3.6 has
	// no cleanup that removes them, so they are removed with
	// "juju remove-saas --force" after the transaction.
	RemoveSaaS []string `json:"remove_saas,omitempty"`
	Changes    []change `json:"changes"`
}

type relationRef struct {
	Key string `json:"key"`
	ID  int    `json:"id"`
}

type change struct {
	Collection string                 `json:"collection"`
	ID         string                 `json:"_id,omitempty"`
	Assert     map[string]interface{} `json:"assert,omitempty"`
	Remove     bool                   `json:"remove,omitempty"`
	Update     bson.M                 `json:"update,omitempty"`
	Insert     bson.M                 `json:"insert,omitempty"`
	Doc        map[string]interface{} `json:"doc_before,omitempty"`
}

type application struct {
	name    string
	remote  bool
	doc     map[string]interface{}
	removed int
}

func (a *application) collection() string {
	if a.remote {
		return "remoteApplications"
	}
	return "applications"
}

// planRepair plans the transaction that removes every relation of the
// dying application, which is a local application or a consumed SaaS, the
// way Juju's Relation.removeOps would if it did not assert that both
// applications are alive.
func planRepair(st store, modelUUID, name string) (*plan, error) {
	model, err := st.doc("models", modelUUID)
	if err != nil {
		return nil, err
	}
	if model == nil {
		return nil, fmt.Errorf("model %s not found", modelUUID)
	}
	p := &plan{ModelUUID: modelUUID, Application: name}
	owner, _ := model["owner"].(string)
	modelName, _ := model["name"].(string)
	p.Model = owner + "/" + modelName

	target, err := readApplication(st, modelUUID, name)
	if err != nil {
		return nil, err
	}
	if target == nil {
		return nil, fmt.Errorf("application %q not found in model %s", name, p.Model)
	}
	if err := requireFields(target.doc, target.collection(), map[string]interface{}{"name": name, "life": lifeDying}); err != nil {
		return nil, err
	}
	if !target.remote {
		if err := requireFields(target.doc, "applications", map[string]interface{}{"unitcount": 0}); err != nil {
			return nil, err
		}
		units, err := st.docs("units", modelUUID, "application", name)
		if err != nil {
			return nil, err
		}
		if len(units) > 0 {
			return nil, fmt.Errorf("application %q still has %d unit documents", name, len(units))
		}
	}

	relations, err := st.docs("relations", modelUUID, "endpoints.applicationname", name)
	if err != nil {
		return nil, err
	}
	apps := []*application{target}
	byName := map[string]*application{name: target}
	for _, relation := range relations {
		ref, err := relationIdentity(st, modelUUID, relation)
		if err != nil {
			return nil, err
		}
		relAssert := map[string]interface{}{"life": lifeDying, "id": ref.ID, "key": ref.Key}
		if err := requireFields(relation, "relations", relAssert); err != nil {
			return nil, err
		}
		endpoints, err := endpointApplications(relation)
		if err != nil {
			return nil, err
		}
		for _, ep := range endpoints {
			app := byName[ep]
			if app == nil {
				if app, err = readApplication(st, modelUUID, ep); err != nil {
					return nil, err
				}
				if app == nil {
					return nil, fmt.Errorf("relation %q references %q, which is neither an application nor a SaaS", ref.Key, ep)
				}
				apps = append(apps, app)
				byName[ep] = app
			}
			app.removed++
		}
		// The relation's unit count is asserted as read: scope documents
		// whose units are gone are removed with it.
		unitCount, ok := asInt(relation["unitcount"])
		if !ok {
			return nil, fmt.Errorf("relations/%v has no integer unitcount", relation["_id"])
		}
		relAssert["unitcount"] = unitCount
		if err := p.remove("relations", relation, relAssert); err != nil {
			return nil, err
		}
		if err := p.removeRelationChildren(st, modelUUID, ref); err != nil {
			return nil, err
		}
		p.Relations = append(p.Relations, ref)
	}

	// removeLocalEndpointOps and removeRemoteEndpointOps, once per
	// application however many of its relations are removed.
	for _, app := range apps {
		if err := p.updateRelationCount(st, modelUUID, app, app == target); err != nil {
			return nil, err
		}
	}
	if len(p.Changes) == 0 {
		return nil, fmt.Errorf("%q has no relations and a relation count of 0: nothing to repair; check that Juju's cleanups are running", name)
	}

	if err := p.removeRelationState(st, modelUUID); err != nil {
		return nil, err
	}
	return p, nil
}

func readApplication(st store, modelUUID, name string) (*application, error) {
	doc, err := modelDoc(st, modelUUID, "applications", modelUUID+":"+name)
	if err != nil {
		return nil, err
	}
	if doc != nil {
		return &application{name: name, doc: doc}, nil
	}
	doc, err = modelDoc(st, modelUUID, "remoteApplications", modelUUID+":"+name)
	if err != nil || doc == nil {
		return nil, err
	}
	if err := requireFields(doc, "remoteApplications", map[string]interface{}{"is-consumer-proxy": false}); err != nil {
		return nil, fmt.Errorf("%w: consumer proxies on the offering side are not handled", err)
	}
	return &application{name: name, remote: true, doc: doc}, nil
}

// removeRelationChildren plans the removal of the documents
// Relation.removeOps and removeRelationChildRecordOps remove with the
// relation, and of the scope documents of units that no longer exist.
func (p *plan) removeRelationChildren(st store, modelUUID string, ref relationRef) error {
	scopePrefix := fmt.Sprintf("%s:r#%d#", modelUUID, ref.ID)
	scopes, err := st.docsWithPrefix("relationscopes", scopePrefix)
	if err != nil {
		return err
	}
	for _, scope := range scopes {
		id, _ := scope["_id"].(string)
		unit := id[strings.LastIndex(id, "#")+1:]
		if doc, err := st.doc("units", modelUUID+":"+unit); err != nil {
			return err
		} else if doc != nil {
			return fmt.Errorf("unit %s is still in scope of relation %q; remove the unit first", unit, ref.Key)
		}
	}
	if err := p.removeAll(modelUUID, "relationscopes", scopes); err != nil {
		return err
	}
	settings, err := st.docsWithPrefix("settings", scopePrefix)
	if err != nil {
		return err
	}
	if err := p.removeAll(modelUUID, "settings", settings); err != nil {
		return err
	}
	if err := p.removeID(st, modelUUID, "statuses", fmt.Sprintf("%s:r#%d", modelUUID, ref.ID)); err != nil {
		return err
	}
	connections, err := st.docs("applicationOfferConnections", modelUUID, "relation-id", ref.ID)
	if err != nil {
		return err
	}
	if err := p.removeAll(modelUUID, "applicationOfferConnections", connections); err != nil {
		return err
	}
	tag := relationTag(ref.Key)
	if err := p.removeID(st, modelUUID, "remoteEntities", modelUUID+":"+tag); err != nil {
		return err
	}
	networks, err := st.docsWithPrefix("relationNetworks", modelUUID+":"+ref.Key+":")
	if err != nil {
		return err
	}
	if err := p.removeAll(modelUUID, "relationNetworks", networks); err != nil {
		return err
	}
	permissions, err := st.docs("secretPermissions", modelUUID, "scope-tag", tag)
	if err != nil {
		return err
	}
	return p.removeAll(modelUUID, "secretPermissions", permissions)
}

// updateRelationCount plans the relation count of an application that
// loses app.removed relations. The count becomes the number of relations
// left: when the stored count is right this is the decrement Juju makes,
// otherwise the count is set, as removeRemoteEndpointOps does for a stale
// count.
func (p *plan) updateRelationCount(st store, modelUUID string, app *application, target bool) error {
	stored, ok := asInt(app.doc["relationcount"])
	if !ok {
		return fmt.Errorf("%s/%v has no integer relationcount", app.collection(), app.doc["_id"])
	}
	all, err := st.docs("relations", modelUUID, "endpoints.applicationname", app.name)
	if err != nil {
		return err
	}
	left := len(all) - app.removed
	if left == int(stored) && app.removed == 0 {
		return nil
	}
	life, ok := asInt(app.doc["life"])
	if !ok {
		return fmt.Errorf("%s/%v has no integer life", app.collection(), app.doc["_id"])
	}
	assert := map[string]interface{}{"name": app.name, "life": life, "relationcount": stored}
	if app.remote {
		assert["is-consumer-proxy"] = false
	} else if target {
		assert["unitcount"] = 0
	}
	var update bson.M
	if int(stored) == len(all) {
		update = bson.M{"$inc": bson.M{"relationcount": -app.removed}}
	} else {
		update = bson.M{"$set": bson.M{"relationcount": left}}
	}
	if err := p.update(app.collection(), app.doc, assert, update); err != nil {
		return err
	}
	if left > 0 || life != lifeDying {
		return nil
	}
	if app.remote {
		p.RemoveSaaS = append(p.RemoveSaaS, app.name)
		return nil
	}
	// The cleanup Juju queues for a dying application that loses its
	// last relation: cleanupApplication with destroyStorage and force
	// false, scoped to the model as newModelScopedCleanupOp does. The
	// RemoveOrphanedApplicationRelations upgrade step of Juju 3.6.29
	// queues the same. Juju ignores it while the application still has
	// units.
	p.Changes = append(p.Changes, change{
		Collection: "cleanups",
		Insert: bson.M{
			"kind":       "application",
			"when":       time.Time{},
			"prefix":     app.name,
			"args":       []interface{}{false, false},
			"model-uuid": modelUUID,
		},
	})
	return nil
}

// removeRelationState plans removing the removed relations from the
// relation state of unit agents. Like Juju's relation state cleanup, it
// removes the whole map rather than leave it empty.
func (p *plan) removeRelationState(st store, modelUUID string) error {
	byID := map[string]map[string]interface{}{}
	removing := map[string][]string{}
	for _, ref := range p.Relations {
		key := fmt.Sprint(ref.ID)
		states, err := st.docsWithField("unitstates", modelUUID, "relation-state."+key)
		if err != nil {
			return err
		}
		for _, state := range states {
			if err := requireModel(modelUUID, "unitstates", state); err != nil {
				return err
			}
			id, _ := state["_id"].(string)
			byID[id] = state
			removing[id] = append(removing[id], key)
		}
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		state := byID[id]
		unset := bson.M{}
		all, _ := asMap(state["relation-state"])
		if len(all) == len(removing[id]) {
			unset["relation-state"] = 1
		} else {
			for _, key := range removing[id] {
				unset["relation-state."+key] = 1
			}
		}
		if err := p.update("unitstates", state, nil, bson.M{"$unset": unset}); err != nil {
			return err
		}
	}
	return nil
}

func modelDoc(st store, modelUUID, collection, id string) (map[string]interface{}, error) {
	doc, err := st.doc(collection, id)
	if err != nil || doc == nil {
		return doc, err
	}
	return doc, requireModel(modelUUID, collection, doc)
}

func requireModel(modelUUID, collection string, doc map[string]interface{}) error {
	id, _ := doc["_id"].(string)
	if !strings.HasPrefix(id, modelUUID+":") {
		return fmt.Errorf("%s/%v does not belong to model %s", collection, doc["_id"], modelUUID)
	}
	if owner, ok := doc["model-uuid"]; ok && owner != modelUUID {
		return fmt.Errorf("%s/%s has model-uuid %v, want %s", collection, id, owner, modelUUID)
	}
	return nil
}

func requireFields(doc map[string]interface{}, collection string, want map[string]interface{}) error {
	id, _ := doc["_id"].(string)
	for _, field := range sortedKeys(want) {
		got, ok := doc[field]
		if !ok {
			return fmt.Errorf("%s/%s has no %s field", collection, id, field)
		}
		if !sameValue(got, want[field]) {
			return fmt.Errorf("%s/%s has %s=%v, want %v", collection, id, field, doc[field], want[field])
		}
	}
	return nil
}

func sameValue(got, want interface{}) bool {
	if w, ok := asInt(want); ok {
		g, ok := asInt(got)
		return ok && g == w
	}
	return got == want
}

func asInt(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		// The mongo shell stores a plain number as a double. Juju reads a
		// whole one as that integer, and MongoDB compares numbers by value.
		if n == math.Trunc(n) && math.Abs(n) < 1<<53 {
			return int64(n), true
		}
	}
	return 0, false
}

func relationIdentity(st store, modelUUID string, relation map[string]interface{}) (relationRef, error) {
	rawID, _ := relation["_id"].(string)
	id, ok := asInt(relation["id"])
	key, keyOK := relation["key"].(string)
	if !ok || id < 0 || !keyOK || rawID != modelUUID+":"+key {
		return relationRef{}, fmt.Errorf("relation %s has no valid id and key", rawID)
	}
	same, err := st.docs("relations", modelUUID, "id", int(id))
	if err != nil {
		return relationRef{}, err
	}
	if len(same) != 1 {
		return relationRef{}, fmt.Errorf("relation id %d matches %d relations, want 1", id, len(same))
	}
	return relationRef{Key: key, ID: int(id)}, requireModel(modelUUID, "relations", relation)
}

func endpointApplications(relation map[string]interface{}) ([]string, error) {
	raw, _ := relation["endpoints"].([]interface{})
	var names []string
	for _, ep := range raw {
		m, ok := asMap(ep)
		if !ok {
			return nil, fmt.Errorf("relation %v has an invalid endpoint", relation["_id"])
		}
		name, ok := m["applicationname"].(string)
		if !ok || name == "" {
			return nil, fmt.Errorf("relation %v has an endpoint without an application", relation["_id"])
		}
		names = append(names, name)
	}
	if len(names) == 0 || len(names) > 2 || (len(names) == 2 && names[0] == names[1]) {
		return nil, fmt.Errorf("relation %v has endpoints %v", relation["_id"], names)
	}
	return names, nil
}

func asMap(v interface{}) (map[string]interface{}, bool) {
	switch m := v.(type) {
	case map[string]interface{}:
		return m, true
	case bson.M:
		return m, true
	}
	return nil, false
}

// relationTag is names.NewRelationTag(key).String().
func relationTag(key string) string {
	return "relation-" + strings.NewReplacer(":", ".", " ", "#").Replace(key)
}

func (p *plan) remove(collection string, doc map[string]interface{}, fields map[string]interface{}) error {
	c, err := newChange(collection, doc, fields)
	if err != nil {
		return err
	}
	c.Remove = true
	p.Changes = append(p.Changes, c)
	return nil
}

func (p *plan) removeAll(modelUUID, collection string, docs []map[string]interface{}) error {
	for _, doc := range docs {
		if err := requireModel(modelUUID, collection, doc); err != nil {
			return err
		}
		if err := p.remove(collection, doc, nil); err != nil {
			return err
		}
	}
	return nil
}

func (p *plan) removeID(st store, modelUUID, collection, id string) error {
	doc, err := modelDoc(st, modelUUID, collection, id)
	if err != nil || doc == nil {
		return err
	}
	return p.remove(collection, doc, nil)
}

func (p *plan) update(collection string, doc map[string]interface{}, fields map[string]interface{}, update bson.M) error {
	c, err := newChange(collection, doc, fields)
	if err != nil {
		return err
	}
	c.Update = update
	p.Changes = append(p.Changes, c)
	return nil
}

// newChange asserts the document's txn-revno and fields, so the transaction
// aborts if Juju changed the document after it was read.
func newChange(collection string, doc map[string]interface{}, fields map[string]interface{}) (change, error) {
	id, _ := doc["_id"].(string)
	if err := ctl.AssertNoPendingTxn(doc, collection, id); err != nil {
		return change{}, err
	}
	if _, ok := asInt(doc["txn-revno"]); !ok {
		return change{}, fmt.Errorf("%s/%s has no integer txn-revno", collection, id)
	}
	assert := map[string]interface{}{"txn-revno": doc["txn-revno"]}
	for k, v := range fields {
		assert[k] = v
	}
	return change{Collection: collection, ID: id, Assert: assert, Doc: doc}, nil
}

func repairOps(p *plan) []txn.Op {
	ops := make([]txn.Op, 0, len(p.Changes))
	for _, c := range p.Changes {
		if c.Insert != nil {
			ops = append(ops, txn.Op{
				C:      c.Collection,
				Id:     p.ModelUUID + ":" + bson.NewObjectId().Hex(),
				Assert: txn.DocMissing,
				Insert: c.Insert,
			})
			continue
		}
		assert := bson.D{}
		for _, k := range sortedKeys(c.Assert) {
			assert = append(assert, bson.DocElem{Name: k, Value: c.Assert[k]})
		}
		op := txn.Op{C: c.Collection, Id: c.ID, Assert: assert, Remove: c.Remove}
		if c.Update != nil {
			op.Update = c.Update
		}
		ops = append(ops, op)
	}
	return ops
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
