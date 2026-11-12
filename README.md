# osm-to-3d-geojson

A Spark (Scala) batch job that turns OpenStreetMap building data into **3D-ready GeoJSON**.
Every output building has a footprint polygon, a height in meters, an address, and tags that
record where its height and geometry came from.

```
 .osm.pbf ──► osm-parquetizer ──► *.osm.pbf.{node,way,relation}.parquet
                                              │
              admin hierarchy (parquet) ──────┤
              Overture buildings (optional) ──┤
              ML floor-count model (optional) ┤
                                              ▼
                                     osm-to-3d-geojson
                                              │
                                              ▼
                      <out-path>/v<timestamp>/u/u8/u8c/.../u8c5dm.geojson
```

The output tiles are the data that `3d-buildings-backend` and `front` in this repository serve and display.

---

## Contents

1. [Requirements](#1-requirements)
2. [Quick start](#2-quick-start)
3. [Input data](#3-input-data)
4. [Configuration](#4-configuration)
5. [What the pipeline does](#5-what-the-pipeline-does)
6. [Output format](#6-output-format)
7. [Floor-count ML model](#7-floor-count-ml-model)
8. [Running from IntelliJ IDEA](#8-running-from-intellij-idea)
9. [Project structure](#9-project-structure)
10. [Known behaviour and troubleshooting](#10-known-behaviour-and-troubleshooting)

---

## 1. Requirements

| Tool  | Version                         | Notes                                                    |
|-------|---------------------------------|----------------------------------------------------------|
| JDK   | 17                              | Spark needs the extra JVM flags listed in [2.3](#23-run) |
| Maven | 3.9+                            | Builds the project and the fat jar                       |
| RAM   | about 12 GB free for the JVM    | The run commands use `-Xmx12g`; lower it for small areas |

Main libraries (see `pom.xml`): Scala 2.12.17, Apache Spark 3.5.0 (core, SQL, MLlib),
Apache Sedona 1.6.1, JTS 1.19, PureConfig, Play JSON / play-geojson.

The build also downloads `com.intellias.mobility.geo:spatial-tools` from the extra GitLab Maven
repository declared in `pom.xml`. The build needs network access to that repository the first time.

Spark always runs in **local mode** (`local[*]`, all CPU cores of your machine).

---

## 2. Quick start

### 2.1 Convert OSM to Parquet

The job does not read `.osm.pbf` directly. Convert it with `osm-parquetizer`
(it is in `../osm-parquetizer` in this repository):

```powershell
cd ..\osm-parquetizer
mvn clean package
java -jar target\osm-parquetizer-1.0.1-SNAPSHOT.jar C:\path\to\data\input\lviv.osm.pbf
```

This writes three files next to the `.pbf`:

```
lviv.osm.pbf.node.parquet
lviv.osm.pbf.way.parquet
lviv.osm.pbf.relation.parquet
```

### 2.2 Build

```powershell
cd osm-to-3d-geojson
mvn clean package
```

Result: `target\osm-to-3d-geojson-1.0-SNAPSHOT-jar-with-dependencies.jar`.

### 2.3 Run

1. Create your own config file, for example `src/main/resources/my-application.conf`
   (this file name is in `.gitignore`, so your local paths are not committed).
   A minimal config needs only four values:

   ```hocon
   data-dir = "ukraine"                                   # folder with the admin hierarchy, see 3.2
   osm.osm-in-path = "C:/path/to/data/input"              # folder with the *.osm.pbf.*.parquet files
   out-path = "C:/path/to/data/output"                    # base folder for the results
   common.geohash-precision = 6                           # tile size, see 6.1
   ```

   Everything you do not set is taken from `src/main/resources/application.conf`.

2. Run from the **project root** (`data-dir` and `model-path` are relative paths):

   ```powershell
   java --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/java.nio=ALL-UNNAMED `
        --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED `
        --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED `
        --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED `
        --add-opens=java.base/java.io=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED `
        -Xmx12g `
        -jar target\osm-to-3d-geojson-1.0-SNAPSHOT-jar-with-dependencies.jar `
        src\main\resources\my-application.conf
   ```

   The only program argument is the path to your config file.

3. Check the result: a new folder `<out-path>/v<yyyyMMddHHmmss>/` with `.geojson` tiles.

---

## 3. Input data

### 3.1 OSM Parquet (required)

`osm.osm-in-path` must point to a **folder**. The job reads every file in it that matches:

```
*osm.pbf.node.parquet
*osm.pbf.way.parquet
*osm.pbf.relation.parquet
```

If the folder holds files for several regions, all of them are read together.
Use one folder per region (for example `data/input/lviv/`) to process one region only.

The schema is the one produced by `osm-parquetizer` (`id`, `tags` as key/value array,
`latitude`/`longitude` for nodes, `nodes` as `(index, nodeId)` array for ways, `members` for relations).

### 3.2 Administrative hierarchy (required)

The job reads two pre-computed Parquet datasets with administrative areas (country, oblast, city, ...).
It uses them to attach a city name to every building. The paths are built from `data-dir`:

```
<data-dir>/compiler_processed/admin_hierarchy/admin_place          # areas with tags (name) and parents
<data-dir>/compiler_processed/admin_hierarchy/ground_admin_place   # areas used for the spatial match
```

This project does **not** generate these datasets. A ready set for Ukraine is committed in
`ukraine/`, so `data-dir = "ukraine"` works out of the box for any area inside Ukraine.

The file `src/main/resources/mapping/admin/dispute_admin_view.csv` decides which country's admin
areas are used where two countries claim the same territory (for example `UKR;RUS` → `UKR`).

### 3.3 Overture Maps buildings (optional)

Disabled by default. When enabled, buildings from [Overture Maps](https://overturemaps.org/)
are merged with the OSM buildings (see [5](#5-what-the-pipeline-does), step 4).

`overture.overture-in-path` is a path or glob to Overture **buildings** GeoParquet files
(theme `buildings`, type `building`). One way to download them for a bounding box is the
official `overturemaps` Python CLI:

```powershell
pip install overturemaps
New-Item -ItemType Directory -Force C:\path\to\data\input\overture\lviv
overturemaps download --bbox=23.90,49.77,24.15,49.90 -f geoparquet --type=building -o C:\path\to\data\input\overture\lviv\buildings.parquet
```

`--bbox` is `west,south,east,north` in degrees (the example covers Lviv).

Only the extent of the OSM buildings (plus a small margin) is read from the Overture files,
unless you set an explicit `overture.bbox`.

### 3.4 Trained floor-count model (optional)

Only needed when `building-levels-model.prediction-enabled = true`.
See [7. Floor-count ML model](#7-floor-count-ml-model).

---

## 4. Configuration

The config format is [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md).
Your file is merged on top of `src/main/resources/application.conf`: a value in your file wins,
anything missing falls back to the default.

### 4.1 Main settings

| Key                          | Default          | Description                                                                                       |
|------------------------------|------------------|---------------------------------------------------------------------------------------------------|
| `data-dir`                   | `"NA"`           | Folder that contains `compiler_processed/admin_hierarchy/...` (see 3.2). **Must be set.**         |
| `osm.osm-in-path`            | `"NA"`           | Folder with the OSM Parquet files (see 3.1). **Must be set.**                                     |
| `out-path`                   | `"NA"`           | Base output folder. **Must be set.** Created automatically for tiled output.                      |
| `tiled-output`               | `true`           | `true`: one file per geohash cell. `false`: one combined file `buildings-3d-v<timestamp>.geojson`. |
| `common.geohash-precision`   | `7`              | Geohash length used to group buildings into tiles (see 6.1).                                      |
| `common.default-storage-level` | `"DISK_ONLY"`  | Spark storage level for cached datasets.                                                          |
| `common.workflows`           | `""`             | Can be overridden with the `WORKFLOWS` env variable. Leave empty (see 10).                       |
| `spark-config.app-name`      | `"OSM to 3D GeoJSON"` | Spark application name.                                                                      |
| `spark-config.master`        | `"local[*]"`     | Spark master. `local[4]` limits the job to 4 cores.                                               |

### 4.2 Admin places

| Key                               | Default                                                    |
|-----------------------------------|------------------------------------------------------------|
| `admin-place.disputed-view-path`  | `mapping/admin/dispute_admin_view.csv` (classpath, then file system) |
| `admin-place.admin-place-out`     | `${data-dir}/compiler_processed/admin_hierarchy/admin_place`         |
| `admin-place.admin-place-ground-out` | `${data-dir}/compiler_processed/admin_hierarchy/ground_admin_place` |

### 4.3 Overture merge

| Key                          | Default  | Description                                                                         |
|------------------------------|----------|-------------------------------------------------------------------------------------|
| `overture.enabled`           | `false`  | `false` skips the Overture reader and the merge completely.                         |
| `overture.overture-in-path`  | `"NA"`   | Path or glob to Overture buildings GeoParquet.                                      |
| `overture.buffer-meters`     | `1.0`    | Buffer around each OSM building used for matching.                                  |
| `overture.min-overlap-ratio` | `0.9`    | Share of the Overture footprint that must lie inside the OSM buffer to count as the same building. |
| `overture.grid-cell-degrees` | `0.005`  | Grid cell size for the spatial pre-join (performance only).                         |
| `overture.bbox`              | not set  | Optional `{ min-lon, min-lat, max-lon, max-lat }`. If absent, derived from the OSM buildings. |

### 4.4 Floor-count model

| Key                                              | Default                         | Description                                                    |
|--------------------------------------------------|---------------------------------|----------------------------------------------------------------|
| `building-levels-model.prediction-enabled`       | `false`                         | Predict floors for buildings without `building:levels`.        |
| `building-levels-model.model-path`               | `data/model/building-levels-rf` | Where the model is saved (training) and loaded (prediction).   |
| `building-levels-model.fallback-levels`          | `1`                             | Floors used when there is no tag and no prediction.            |
| `building-levels-model.mode`                     | `"train"`                       | Trainer only: `train` or `load` (load + evaluate).             |
| `building-levels-model.neighborhood-radius-meters` | `75.0`                        | Radius for neighbour features.                                 |
| `building-levels-model.train-ratio`              | `0.8`                           | Train/test split.                                              |
| `building-levels-model.num-trees`                | `200`                           | Random forest size.                                            |
| `building-levels-model.max-depth`                | `15`                            | Random forest depth.                                           |
| `building-levels-model.seed`                     | `42`                            | Random seed.                                                   |

### 4.5 Full example

```hocon
data-dir = "ukraine"
osm.osm-in-path = "C:/Users/me/master-diploma-lnu/data/input/lviv"
out-path = "C:/Users/me/master-diploma-lnu/data/output/lviv"
tiled-output = true

common.geohash-precision = 6

overture {
  enabled = true
  overture-in-path = "C:/Users/me/master-diploma-lnu/data/input/overture/lviv"
}

building-levels-model {
  prediction-enabled = true
  model-path = "data/model/building-levels-rf"
}
```

---

## 5. What the pipeline does

`Main` runs two workflows in order. Each workflow is a chain of processors that pass Spark
datasets to each other.

**Workflow `building`** (`compiler/building/BuildingWorkflow.scala`)

| # | Processor                       | What it does                                                                                     |
|---|---------------------------------|--------------------------------------------------------------------------------------------------|
| 1 | `OsmSourceReader`               | Reads OSM nodes, ways and relations from Parquet.                                                |
| 2 | `AdminHierarchyReader`          | Reads the two admin hierarchy datasets.                                                          |
| 3 | `BuildingGeometryProcessor`     | Builds footprint (multi)polygons from closed ways and relations tagged `building=*`. Invalid polygons are dropped. |
| 4 | `OvertureSourceReader` + `BuildingSourceMergeProcessor` | *Only if `overture.enabled`.* An OSM and an Overture building are treated as one when ≥ 90 % of the Overture footprint lies inside the OSM footprint buffered by 1 m. The OSM geometry and tags win; missing attributes (height, floors, name, roof shape, facade material) are filled from Overture. Unmatched Overture buildings are added as new buildings. |
| 5 | `BuildingAdminProcessor`        | Finds the admin areas each building lies in.                                                     |
| 6 | `BuildingAddressProcessor`      | Builds the address: city from the admin hierarchy, street and house number from `addr:street` / `addr:housenumber`. |
| 7 | `BuildingHeightProcessor`       | Sets the floor count and height (see below).                                                     |
| 8 | `BuildingModelProcessor`        | Converts each building to the output model and computes its geohash.                             |

**Workflow `geojson-building`** (`geojson/building3d/GeoJsonBuilding3DWorkflow.scala`)

| # | Processor                  | What it does                                                         |
|---|----------------------------|----------------------------------------------------------------------|
| 1 | `GeoJsonBuilding3DBuilder` | Groups buildings by geohash and writes the GeoJSON files.            |

### How height is computed

```
height (m) = floors × 3
```

The floor count is chosen in this order:

1. The building's own `building:levels` tag (from OSM, or filled from Overture in step 4).
   Values like `3;4`, `3,4`, `5 floors` or `1.5` are parsed; the result is rounded and is at least 1.
2. A prediction from the ML model, if `prediction-enabled = true` and the model can be loaded.
   Buildings that exist only in Overture never get a prediction.
3. `fallback-levels` (default `1`, so 3 m).

The tag `height=*` from OSM is **not** used for the output height.

---

## 6. Output format

### 6.1 Tiled output (default)

Each run creates a new version folder. Buildings are grouped by geohash, and each group goes
into its own GeoJSON `FeatureCollection`. The folder tree has one level per geohash character:

```
<out-path>/
└── v20261112122735/                  # run timestamp, yyyyMMddHHmmss
    └── u/
        └── u8/
            └── u8c/
                └── u8c5/
                    └── u8c5d/
                        ├── u8c5dm.geojson
                        └── u8c5dt.geojson
```

A building's geohash is computed from its **top-left corner** (minimum longitude, maximum latitude
over all its coordinates), so each building lands in exactly one tile and the tile stays the same
between runs.

`common.geohash-precision` sets the tile size. Approximate cell sizes:

| Precision | Cell size          |
|-----------|--------------------|
| 5         | ~4.9 km × 4.9 km   |
| 6         | ~1.2 km × 0.6 km   |
| 7         | ~153 m × 153 m     |

### 6.2 Single-file output

With `tiled-output = false` the job writes one file: `<out-path>/buildings-3d-v<timestamp>.geojson`.
All features are collected on the driver first, so use this only for small areas.

### 6.3 Feature format

```json
{
  "type": "Feature",
  "id": "29",
  "geometry": {
    "type": "MultiPolygon",
    "coordinates": [[[[24.0118532, 49.8658678], [24.0119654, 49.8658933], [24.0119951, 49.865838], [24.0118829, 49.8658125], [24.0118532, 49.8658678]]]]
  },
  "properties": {
    "height": 3,
    "address": "Львів, Варшавська вулиця, 171",
    "source": "osm",
    "dataSource": "osm"
  }
}
```

| Field                   | Type   | Meaning                                                                                       |
|-------------------------|--------|-----------------------------------------------------------------------------------------------|
| `id`                    | string | Sequential building id within this run. It is **not** the OSM id and can change between runs. |
| `geometry`              | MultiPolygon | Footprint in WGS84 (longitude, latitude).                                               |
| `properties.height`     | int    | Height in meters (`floors × 3`).                                                              |
| `properties.address`    | string | `"<city>, <street>, <house number>"`. Missing parts are `unknown`.                            |
| `properties.source`     | string | Where the floor count came from: `osm`, `overture`, `predicted` or `fallback`.                |
| `properties.dataSource` | string | Where the building came from: `osm`, `overture` or `osm+overture`.                            |

---

## 7. Floor-count ML model

A Spark MLlib random forest predicts the number of floors for buildings that have no
`building:levels` tag. It is trained on the OSM buildings in your input that do have the tag.

**Features** (`ml/BuildingFeatureExtractor.scala`): building type (`building=*`, one-hot),
whether the building has a name, footprint area, perimeter, compactness, length/width ratio,
rectangularity, vertex count, and neighbourhood statistics within `neighborhood-radius-meters`
(neighbour count, average known neighbour floors, built-up density, average neighbour area).
Geometry is measured in meters after projection to UTM zone 35N, which fits Ukraine.

### 7.1 Train

The trainer is a separate entry point, `com.students.lnu.ml.BuildingLevelsTrainerApp`,
and reads the same config file. It uses `osm.osm-in-path` and does not need the admin hierarchy.

```powershell
java <the same --add-exports / --add-opens flags as in 2.3> -Xmx12g `
     -cp target\osm-to-3d-geojson-1.0-SNAPSHOT-jar-with-dependencies.jar `
     com.students.lnu.ml.BuildingLevelsTrainerApp `
     src\main\resources\my-application.conf
```

With `mode = "train"` (default) it trains, logs RMSE / MAE / R² on the test split and the feature
importances, and saves the model to `model-path` (an existing model is overwritten).
With `mode = "load"` it loads the existing model and logs the same metrics on all labelled buildings.

### 7.2 Use the model in the pipeline

Set `building-levels-model.prediction-enabled = true` in your config and run the main job.
If the model cannot be loaded, the job logs a warning and uses `fallback-levels` instead of failing.
Output buildings with a predicted floor count have `"source": "predicted"`.

---

## 8. Running from IntelliJ IDEA

1. Open the folder as a Maven project and use JDK 17 with the Scala plugin.
2. Create an **Application** run configuration:
   - Main class: `com.students.lnu.Main` (or `com.students.lnu.ml.BuildingLevelsTrainerApp`)
   - Program arguments: `src/main/resources/my-application.conf`
   - Working directory: the `osm-to-3d-geojson` folder
   - VM options:

     ```
     --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED -Xmx12g --add-opens java.xml.crypto/com.sun.org.slf4j.internal=ALL-UNNAMED
     ```

---

## 9. Project structure

```
osm-to-3d-geojson/
├── pom.xml
├── ukraine/compiler_processed/admin_hierarchy/   # pre-computed admin areas for Ukraine (input, see 3.2)
├── data/model/building-levels-rf/                # trained model, created by the trainer (not in git)
└── src/main/
    ├── resources/
    │   ├── application.conf                      # default config
    │   ├── my-application.conf                   # your local config (git-ignored)
    │   └── mapping/admin/dispute_admin_view.csv  # disputed-territory rules
    └── scala/com/students/lnu/
        ├── Main.scala                            # entry point of the pipeline
        ├── AppConfig.scala                       # config classes and loading
        ├── WorkflowManager.scala                 # builds and runs the workflows
        ├── common/                               # Workflow/Processor base, Geohash, Spark session setup
        ├── compiler/
        │   ├── datasource/osm/                   # OSM Parquet reader
        │   ├── datasource/overture/              # Overture GeoParquet reader
        │   ├── geometry/                         # polygon assembly from OSM ways and relations
        │   ├── admin/                            # admin hierarchy reading and assignment
        │   └── building/                         # building workflow and its processors
        ├── geojson/                              # GeoJSON writers (tiled and single file)
        ├── ml/                                   # floor-count model, features, trainer, UTM projection
        ├── model/                                # data classes (Building, Building3DModel, AdminPlace, ...)
        └── tools/                                # file helpers
```

---

## 10. Known behaviour and troubleshooting

**Not every OSM building ends up in the output.** A building is written only if:
- it is a valid polygon tagged `building=*`;
- it lies inside an area from the admin hierarchy (`data-dir`);
- it has an `addr:street` tag. Buildings without a street address are dropped.
  Buildings that come only from Overture are kept, with street and house number set to `unknown`.

**`ERROR StatusLogger Unrecognized format specifier ...` at start-up.** These lines come from the
logging setup inside the fat jar. They do not stop the run, but most application log lines are not
printed when you run the jar. Check the output folder to confirm the run finished.

**`Path does not exist: .../ukraine/compiler_processed/...`.** The job was not started from the
project root, or `data-dir` is wrong. `data-dir` and `model-path` are resolved against the current
working directory.

**`NoSuchFileException` for the output.** With `tiled-output = false`, `out-path` must exist
before the run. The tiled writer creates the folders itself.

**`IllegalAccessError` / `InaccessibleObjectException` from Spark.** The `--add-opens` /
`--add-exports` JVM flags are missing.

**`OutOfMemoryError`.** Increase `-Xmx`, or split the input into smaller regions.

**Spark temp files.** Spark writes shuffle data to `/tmp/spark.local.dir/`
(on Windows: `C:\tmp\spark.local.dir\` on the current drive) and checkpoints to `./checkpoints`.
You can delete both after a run.

**`WORKFLOWS` env variable.** Both workflows currently register under the name `building`.
Leave `WORKFLOWS` unset (or set it to `building`) to run the pipeline; any other value runs nothing.
