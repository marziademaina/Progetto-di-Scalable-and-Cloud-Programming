# Progetto di Scalable and Cloud Programming
Marzia De Maina (mat. 0001194461) — Anno Accademico 2025/2026


Alma Mater Studiorum — Università di Bologna 

---

## Descrizione

Applicazione distribuita in **Scala** e **Apache Spark** che, dato un dataset di eventi sismici (~3,4 milioni di record), individua la **coppia di celle geografiche** in cui i terremoti si verificano nello stesso giorno con la **massima frequenza**, elencando tutte le date in cui tale co-occorrenza si è verificata.

**Risultato**: la coppia `((38.8, -122.8), (38.8, -122.7))` co-occorre **10.014 volte**.

---

## Struttura del repository

```
.
├── IdeaProjects/
│   └── earthquake-cooccurrence/
│       ├── src/main/scala/earthquake/
│       │   └── EarthquakeCooccurrence.scala   # Applicazione Spark principale
│       ├── build.sbt                          # Configurazione sbt + sbt-assembly
│       ├── project/
│       │   ├── build.properties               # Versione sbt
│       │   └── plugins.sbt                    # Plugin sbt-assembly
│       ├── benchmark.py                       # Script benchmark automatizzato su GCP
│       └── benchmark_log.txt                  # Log dei risultati del benchmark
└── report/
    ├── main.tex                               # Report LaTeX
    ├── main.pdf                               # Report compilato
    └── media/                                 # Grafici e immagini del report
```

---

## Prerequisiti

| Strumento | Versione consigliata |
|-----------|----------------------|
| Java (JDK) | 8 o 11 |
| Scala | 2.12.18 |
| sbt | 1.9.x |
| Apache Spark | 3.3.2 (solo per esecuzione locale) |
| Python | 3.8+ (solo per il benchmark su GCP) |
| Google Cloud SDK (`gcloud`, `gsutil`) | qualsiasi versione recente (solo GCP) |

---

## Compilazione

Il progetto usa **sbt-assembly** per produrre un fat JAR (tutte le dipendenze incluse, Spark escluso perché lo fornisce il cluster).

```bash
cd IdeaProjects/earthquake-cooccurrence
sbt assembly
```

Il JAR viene generato in:

```
target/scala-2.12/earthquake-cooccurrence-assembly-1.0.jar
```

---

## Esecuzione

### Parametri dell'applicazione

```
EarthquakeCooccurrence <inputPath> <outputPath> [numPartitions]
```

| Parametro | Descrizione | Default |
|-----------|-------------|---------|
| `inputPath` | Percorso del CSV di input (locale o `gs://`) | obbligatorio |
| `outputPath` | Percorso della directory di output (locale o `gs://`) | obbligatorio |

### Esecuzione locale

```bash
spark-submit \
  --master local[*] \
  --class earthquake.EarthquakeCooccurrence \
  target/scala-2.12/earthquake-cooccurrence-assembly-1.0.jar \
  /path/to/dataset-earthquakes.csv \
  /path/to/output \
  16
```

> Se il percorso di output esiste già, l'applicazione lo elimina automaticamente prima di scrivere.

### Esecuzione su Google Cloud Dataproc

> Sostituisci `<PROJECT_ID>` con il tuo ID progetto GCP e `<BUCKET>` con il nome del tuo bucket Cloud Storage.

**0. Prerequisiti GCP**

Assicurati di aver abilitato le API necessarie e di essere autenticato:

```bash
gcloud auth login
gcloud config set project <PROJECT_ID>

# Abilita le API richieste
gcloud services enable dataproc.googleapis.com storage.googleapis.com

# Crea un bucket (se non esiste già)
gsutil mb -l europe-west1 gs://<BUCKET>
```

**1. Carica il dataset su Google Cloud Storage:**

```bash
gsutil cp /path/to/dataset-earthquakes.csv \
  gs://<BUCKET>/data/dataset-earthquakes.csv
```

**2. Compila e carica il JAR su Google Cloud Storage:**

```bash
cd IdeaProjects/earthquake-cooccurrence
sbt assembly

gsutil cp target/scala-2.12/earthquake-cooccurrence-assembly-1.0.jar \
  gs://<BUCKET>/jars/earthquake-cooccurrence-assembly-1.0.jar
```

**3. Crea un cluster Dataproc:**

```bash
gcloud dataproc clusters create earthquake-cluster \
  --region=europe-west1 \
  --num-workers=3 \
  --master-machine-type=n2-standard-4 \
  --worker-machine-type=n2-standard-4 \
  --master-boot-disk-size=240 \
  --worker-boot-disk-size=240
```

**4. Sottometti il job:**

```bash
gcloud dataproc jobs submit spark \
  --cluster=earthquake-cluster \
  --region=europe-west1 \
  --class=earthquake.EarthquakeCooccurrence \
  --jars=gs://<BUCKET>/jars/earthquake-cooccurrence-assembly-1.0.jar \
  -- \
  gs://<BUCKET>/data/dataset-earthquakes.csv \
  gs://<BUCKET>/output/run1 \
  128
```

**5. Leggi l'output:**

```bash
gsutil cat gs://<BUCKET>/output/run1/part-00000
```

**6. Elimina il cluster (per risparmiare costi):**

```bash
gcloud dataproc clusters delete earthquake-cluster \
  --region=europe-west1 --quiet
```
---

## Script di benchmark automatizzato

`benchmark.py` esegue automaticamente il ciclo completo di benchmark su GCP: crea il cluster, sottomette i job con tutte le combinazioni di worker e partizioni, raccoglie i tempi e infine elimina il cluster.

### Configurazione

Modifica le costanti in cima al file:

```python
PROJECT_ID    = "earthquake-project-497621"
REGION        = "europe-west1"
BUCKET_NAME   = "earthquake-bucket-497621"
CLUSTER_NAME  = "earthquake-cluster"
WORKER_COUNTS    = [2, 3, 4]
PARTITION_COUNTS = [4, 8, 16, 32, 64, 128, 256]
MACHINE_TYPE  = "n2-standard-2"  # oppure "n2-standard-4"
```

### Esecuzione

```bash
cd IdeaProjects/earthquake-cooccurrence

# Esegui il benchmark (il JAR è già su GCS)
python benchmark.py

# Se vuoi anche caricare il JAR prima di iniziare:
# modifica main(to_upload_jar=True) nel file
```

I risultati vengono salvati incrementalmente in `benchmark_log.txt` (una riga JSON per run).

---

## Risultati del benchmark

### n2-standard-4 (4 vCPU, 16 GB RAM)

| Workers | 4 part | 8 part | 16 part | 32 part | 64 part | 128 part | **256 part** |
|---------|--------|--------|---------|---------|---------|----------|-------------|
| 2 | 971,89 s | 657,41 s | 521,98 s | 477,32 s | 434,11 s | 379,22 s | **358,74 s** |
| 3 | 981,99 s | 594,39 s | 393,49 s | 344,46 s | 306,04 s | 273,06 s | **268,27 s** |

### n2-standard-2 (2 vCPU, 8 GB RAM)

| Workers | 4 part | 8 part | 16 part | 32 part | 64 part | **128 part** | 256 part |
|---------|--------|--------|---------|---------|---------|-------------|---------|
| 2 | 2662,86 s | 1321,49 s | 980,96 s | 930,70 s | 860,00 s | 808,87 s | **780,90 s** |
| 3 | 1804,11 s | 1079,47 s | 661,29 s | 618,87 s | 573,18 s | 567,88 s | **563,20 s** |
| 4 | 983,68 s | 860,37 s | 517,71 s | 520,13 s | 474,82 s | **444,70 s** | 465,59 s |

### Strong scaling (best time per configurazione)

| Macchina | Workers | Best time | Speedup | Efficienza |
|----------|---------|-----------|---------|------------|
| n2-standard-4 | 2 | 358,74 s | 1,00 | 1,000 |
| n2-standard-4 | 3 | 268,27 s | 1,34 | 0,891 |
| n2-standard-2 | 2 | 780,90 s | 1,00 | 1,000 |
| n2-standard-2 | 3 | 563,20 s | 1,39 | 0,924 |
| n2-standard-2 | 4 | 444,70 s | 1,76 | 0,878 |

La legge di Amdahl stima una **frazione seriale ≈ 14%** (frazione parallelizzabile ≈ 86%).

---

## Note

- Con poche partizioni (es. 4) i core rimangono parzialmente inattivi: su un cluster con 4 worker `n2-standard-2` (8 core totali), 4 task sfruttano solo il 50% della capacità.
- Sopra-partizionare introduce overhead di scheduling e shuffle su task molto piccoli: il punto ottimale dipende dalla macchina e dal numero di worker.
- L'approccio con `groupByKey` è stato valutato e scartato: causa OOM sulle macchine `n2-standard-2` per mancanza di riduzione locale prima dello shuffle.
