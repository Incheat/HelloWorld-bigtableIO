# HelloWorld-bigtableIO

## Ref
- https://beam.apache.org/releases/javadoc/2.58.0/org/apache/beam/sdk/io/gcp/bigtable/BigtableIO.html
- https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigtable/BigtableIO.java
- https://github.com/GoogleCloudPlatform/cloud-bigtable-examples/blob/main/java/dataflow-connector-examples/src/main/java/com/google/cloud/bigtable/dataflow/example/HelloWorldWrite.java

## Run Command - creditial
gcloud auth application-default login

## Run Command - execute
mvn package exec:exec -DHelloWorldWrite -Dbigtable.projectID=[PROJECT_ID] -Dbigtable.instanceID=[BIGTABLE_INSTANCE] -Dbigtable.tableID=[BIGTABLE_TABLE] -Dbigtable.region=[BIGTABLE_REGION] -Dgs=gs://[STAGE_GCS_BUCKET]
