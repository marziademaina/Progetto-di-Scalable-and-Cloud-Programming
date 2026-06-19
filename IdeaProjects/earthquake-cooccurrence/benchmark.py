import subprocess
import time
import json
import platform

PROJECT_ID    = "earthquake-project-497621"
REGION        = "europe-west1"
BUCKET_NAME   = "earthquake-bucket-497621"
CLUSTER_NAME  = "earthquake-cluster"
LOCAL_JAR_PATH = "target/scala-2.12/earthquake-cooccurrence-assembly-1.0.jar"
GCS_JAR_PATH   = f"gs://{BUCKET_NAME}/jars/earthquake-cooccurrence-assembly-1.0.jar"
INPUT_CSV      = f"gs://{BUCKET_NAME}/data/dataset-earthquakes-full.csv"
OUTPUT_BASE    = f"gs://{BUCKET_NAME}/output"
MAIN_CLASS = "earthquake.EarthquakeCooccurrence"

WORKER_COUNTS    = [2, 3, 4]
PARTITION_COUNTS = [4, 8, 16, 32, 64, 128, 256]

MACHINE_TYPE = "n2-standard-2" # "n2-standard-4"


def run_command(cmd, shell=False):
    if platform.system() == "Windows" and isinstance(cmd, list):
        if cmd[0] in ["gsutil", "gcloud"]:
            cmd[0] = f"{cmd[0]}.cmd"

    print(f"--> Running: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    try:
        result = subprocess.run(
            cmd,
            shell=shell,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e.stderr}")
        raise e


def upload_jar():
    print("Uploading JAR to GCS...")
    run_command(["gsutil", "cp", LOCAL_JAR_PATH, GCS_JAR_PATH])
    print("Upload complete.")


def create_cluster(num_workers):
    print(f"Creating cluster with {num_workers} workers...")
    cmd = [
        "gcloud", "dataproc", "clusters", "create", CLUSTER_NAME,
        f"--region={REGION}",
        f"--num-workers={num_workers}",
        "--master-boot-disk-size=240",
        "--worker-boot-disk-size=240",
        f"--master-machine-type={MACHINE_TYPE}",
        f"--worker-machine-type={MACHINE_TYPE}"
    ]
    run_command(cmd)
    print("Cluster created.")


def delete_cluster():
    print("Deleting cluster...")
    try:
        run_command([
            "gcloud", "dataproc", "clusters", "delete", CLUSTER_NAME,
            f"--region={REGION}",
            "--quiet"
        ])
        print("Cluster deleted.")
    except Exception as e:
        print(f"Warning: Could not delete cluster (might not exist). {e}")


def submit_job(run_id, num_partitions):
    output_dir = f"{OUTPUT_BASE}/{run_id}"

    try:
        run_command(f"gsutil rm -r {output_dir}", shell=True)
    except:
        pass

    print(f"Submitting Job (ID: {run_id}, Partitions: {num_partitions})...")

    start_time = time.time()

    cmd = [
        "gcloud", "dataproc", "jobs", "submit", "spark",
        f"--cluster={CLUSTER_NAME}",
        f"--region={REGION}",
        f"--class={MAIN_CLASS}",
        f"--jars={GCS_JAR_PATH}",
        "--",
        INPUT_CSV,
        output_dir,
        str(num_partitions)
    ]

    run_command(cmd)
    end_time = time.time()
    duration = end_time - start_time

    print(f"Job finished in {duration:.2f} seconds.")

    try:
        result_content = run_command(f"gsutil cat {output_dir}/part-00000", shell=True)
    except:
        result_content = "ERROR: Could not read output file."

    return duration, result_content


def main(to_upload_jar=False, write_full_output=False):
    results = []

    if to_upload_jar:
        upload_jar()

    try:
        for workers in WORKER_COUNTS:
            try:
                create_cluster(workers)

                for partitions in PARTITION_COUNTS:
                    run_id = f"w{workers}_p{partitions}"
                    print(f"\n=== BENCHMARKING: Workers={workers}, Partitions={partitions} ===")

                    try:
                        duration, output = submit_job(run_id, partitions)

                        record = {
                            "workers": workers,
                            "partitions": partitions,
                            "time_seconds": round(duration, 2),
                            "output_snippet": output.replace("\n", " | ")[:100] + "..."
                        }
                        results.append(record)

                        with open("benchmark_log.txt", "a") as f:
                            f.write(json.dumps(record) + "\n")
                            if write_full_output:
                                f.write(f"Full Output:\n{output}\n")
                            f.write("-" * 40 + "\n")

                    except Exception as e:
                        print(f"Job failed for {run_id}: {e}")

            finally:
                delete_cluster()

    except KeyboardInterrupt:
        print("\nScript interrupted by user. Attempting cleanup...")
        delete_cluster()

    print("\n\n====== FINAL BENCHMARK REPORT ======")
    print(f"{'Workers':<10} | {'Partitions':<12} | {'Time (s)':<10}")
    print("-" * 40)
    for r in results:
        print(f"{r['workers']:<10} | {r['partitions']:<12} | {r['time_seconds']:<10}")


if __name__ == "__main__":
    main(to_upload_jar=False, write_full_output=False)