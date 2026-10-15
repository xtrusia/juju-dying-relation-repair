# juju-dying-relation-repair

`juju-dying-relation-repair` removes the relations that keep a dying application or a dying consumed SaaS in a Juju 3.6 model.

Juju 3.6 removes a relation with no units in a transaction that asserts both of its applications are alive. Once one of them is dying, that assertion never holds, so `juju remove-application --force`, `juju remove-relation --force` and `juju remove-saas --force` fail or change nothing. A fix in Juju itself is proposed in [juju/juju#23352](https://github.com/juju/juju/pull/23352).

This tool edits Juju state in MongoDB. It checks the state below and refuses anything else.

## When to use it

- The application, or the consumed SaaS, is dying. A local application also has no units.
- Every relation it has is dying, and no unit that still exists is in its scope.
- No end of a relation is a consumer proxy, the offering side of a cross-model relation.
- The controller runs on machines and its MongoDB has a primary.
- The `--force` removal commands above did not remove it.

## Usage

Download the binary for the controller architecture from [Releases](https://github.com/xtrusia/juju-dying-relation-repair/releases) and run it from a Juju client logged in as a controller administrator.

```text
# Show the repair plan without changing anything.
juju-dying-relation-repair -model mycontroller:admin/mymodel -application myapp

# Apply it.
juju-dying-relation-repair -model mycontroller:admin/mymodel -application myapp -yes
```

`-application` names a local application or a consumed SaaS. The client copies the binary to a started controller machine and runs it there with `sudo`. It then runs `juju remove-saas -m <model> <saas> --force --no-wait` for each dying SaaS left with no relation. Watch `juju status -m <model>` until they disappear.

Both commands print every change with the values it requires, and save the plan with every document it changes, as read before the change, to a JSON file readable only by its owner: `juju-dying-relation-repair-plan.json` for the dry run and `juju-dying-relation-repair-backup.json` when applying, unless `-backup` names another file. Only the file holds the documents, since relation settings can hold credentials.

You can also run it on a controller machine, then run the `juju remove-saas` commands it prints:

```text
sudo ./juju-dying-relation-repair -model-uuid <model UUID> -application myapp -yes
```

## What it changes

In one Juju transaction, the tool removes every relation of the application with the documents Juju removes along with it, sets the relation count of the applications and SaaS at both ends to the relations they have left, queues the cleanup that removes a dying local application, and removes the relations from the relation state of unit agents. Juju then removes the dying local applications.

Every operation asserts the document revision the tool read, and the tool plans again just before writing, so nothing changes if Juju changed any of those documents first. The backup file is a record of those documents, not an automatic rollback.

## Build

```text
CGO_ENABLED=0 go build -o juju-dying-relation-repair .
```

## License

Apache-2.0. See [LICENSE](LICENSE).
