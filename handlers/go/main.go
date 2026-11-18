// Go TaskHandlers for the Python Dags `orders_elt_pipeline` and `ingest_bench`.
//
// The Python Dag files declare the graph with @task.stub(queue="golang"); this
// binary only supplies task bodies. ExecutableCoordinator finds it in the
// `go-task-handlers` Dag bundle and runs one process per task instance.
package main

import (
	"log"

	"github.com/apache/airflow/go-sdk/airflow"
)

func main() {
	bundle := airflow.Bundle()
	bundle.Register(
		airflow.TaskHandler("orders_elt_pipeline", "ingest_go", Ingest),
		airflow.TaskHandler("ingest_bench", "ingest_go", Ingest),
		airflow.TaskHandler("ingest_bench", "noop_go", Noop),
	)
	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}
