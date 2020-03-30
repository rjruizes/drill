
# FOLIO Storage Plugin

Development is currently in progress. This plugin will enable FOLIO storage to be SQL queryable.

### Running Tests

Currently, there is one test that confirms the plugin functions. It can be run via VS Code or `mvn test`. Before running the test, run these commands:

```
cd src/main/resources
cp bootstrap-storage-plugins.sample.json bootstrap-storage-plugins.json
```

And supply a password in bootstrap-storage-plugins.json

### Pushdown filters

The following SQL operations are pushed down into CQL:
- `WHERE`
- `AND`
- `OR`
- `=`

The following are not:
- `ORDER BY` is not pushed down; apparently no option to. It is done by Drill client-side.
- `>`, `>=`, `<`, `<=` have not been tested