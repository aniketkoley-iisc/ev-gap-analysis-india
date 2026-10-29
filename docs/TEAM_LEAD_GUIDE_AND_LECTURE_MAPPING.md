# 📋 Team Lead Master Briefing & DA-231-O Course Lecture Note Mapping

**Prepared for**: Aniket Koley (Team Lead) & Team (`Dashrath Rajpurohit`, `Kumar Bhawenshu`, `Chennuri Manikanta`)  
**Course**: DSBA DA-231-O — *Data Engineering at Scale* (Prof. Yogesh Simmhan, IISc Bangalore)

---

## 1. Track-by-Track & Cell-by-Cell Briefing Guide

### Track 1 (Aniket Koley) — Stages 1 & 2: Lakehouse Storage, Multi-Source Ingestion & Spark 50m Spatial Deduplication

#### Cells 1–5 (`Stage 1: Medallion Lakehouse & Full-Scale Bronze Ingestion`)
* **What It Does**:
  1. Initializes a 3-tier **Medallion Lakehouse (`Bronze` $\rightarrow$ `Silver` $\rightarrow$ `Gold`)** backed by Apache Parquet columnar storage on local SSD (`/content/data/...`) and persistent Google Drive (`/content/drive/MyDrive/EV_Gap_Project/...`).
  2. Downloads the full **`1.59 GB` (`1,712,356,803` bytes) Geofabrik India OpenStreetMap (`india-latest.osm.pbf`)** file and queries the live **OpenStreetMap Overpass API (`610` live stations)**.
  3. Clones 3 real external repositories (`e_amrit_chargers`, `vahan_ev_market`, `india_ev_sales`) and uses the C++ `osmium-tool` binary to filter **`21,402` real spatial infrastructure primitives** (`13,177` `amenity=fuel`, `4,207` `place=town`, `2,339` `amenity=parking`, `566` `amenity=charging_station`, `508` `place=city`, `342` `highway=services`, `263` `power=substation`) into `osm_primitives_bronze.parquet`.
  4. Merges `OSM_Live_Overpass` (`610`), `Geofabrik_India_PBF` (`566`), and `E_Amrit_GeoJSON` (`159`) into `charging_stations_bronze.parquet` (**`1,335` raw charging station records**).
* **Layman Explanation for Your Team**:
  > *"Instead of typing or hardcoding any data, our Bronze Layer pulls directly from 4 real-world sources: the 1.59 GB OpenStreetMap binary map of India, the live Overpass API, the Government E-Amrit Charger GeoJSON, and the Vahan EV registration CSVs. Because Spark reads local disk much faster than network/FUSE mounts, we cache the raw files in Google Drive once and copy them to local SSD on startup, storing every table in columnar Apache Parquet format."*
* **DA-231-O Lecture Note Reference**:
  * **L3: 1. Distributed File Systems — Need of File System & Storage Systems (HDFS / GFS / Columnar Lakehouse)**: Streaming sequential reads over large binary blocks (`1.59 GB` PBF), write-once-read-many (WORM) immutability in the Bronze layer, and columnar projection/compression via Apache Parquet.

---

#### Cell 6 (`Stage 2.1: Spark SQL Vahan EV Data Cleaning & Peak Demand Estimation`)
* **What It Does**:
  * Cleans `electric_vehicle_sales_by_state.csv` (`2,445` rows), trims whitespace, parses `"01-Apr-21"` strings into `DateType`, deduplicates `(state, date, vehicle_category)` via `Window.partitionBy`, extracts `year`, `month`, and Indian `fiscal_year`, and computes `ev_penetration_pct` and `estimated_peak_demand_kw`.
* **Layman Explanation for Your Team**:
  > *"Raw Vahan CSV files store dates as text (`01-Apr-21`) and only give vehicle counts. Using Spark DataFrames and Window functions, we convert text dates into proper calendar and Indian fiscal years, ensure zero duplicate monthly entries per state, and translate vehicle counts into electrical grid load (`kW`) so we can compare vehicle demand against charging station supply."*
* **DA-231-O Lecture Note Reference**:
  * **Processing Systems (Spark DataFrames & Spark SQL Catalyst Optimizer)**: Declarative transformations (`withColumn`, `to_date`, `Window.partitionBy`) compiled by Spark's Catalyst optimizer into whole-stage codegen physical plans.

---

#### Cell 7 (`Stage 2.2: Spark Pair RDD + DataFrame Window 50-Meter Spatial Deduplication`)
* **What It Does**:
  * Maps each station's `(lat, lon)` into a discrete **50-meter spatial cell** (`grid_lat_50m = round(lat / 0.00045)`, `grid_lon_50m = round(lon / 0.00045)`).
  * Uses **Spark Pair RDD (`map` + `reduceByKey`)** to count how many raw portals reported the same physical station within 50m (`merged_sources_count` up to `4`), and **Spark Window `row_number() == 1`** to keep the highest-priority canonical record, followed by a **Broadcast Nearest-City Spatial Join** (`F.broadcast`) to impute missing `city` and `state` labels.
  * **Result**: **`1,335` Raw Stations $\rightarrow$ `632` Canonical Silver Stations (`703` duplicates removed = `52.7%`)**.
* **Layman Explanation for Your Team**:
  > *"When we combined E-Amrit, Live Overpass, and the 1.59 GB OSM extract, the same physical charger (for example, EESL Oriental Bank in Connaught Place, Delhi) appeared 4 times with tiny GPS variations in the 4th decimal place. Doing an all-pairs distance check takes $O(N^2)$ comparisons. Instead, we snapped coordinates to a 50-meter grid ($0.00045^\circ$), used a **MapReduce Pair RDD (`reduceByKey`)** to count duplicates per 50m box, and used a **Spark Window function** to keep 1 canonical record per 50m cell—removing 703 duplicates (52.7%) in seconds!"*
* **DA-231-O Lecture Note Reference**:
  * **MapReduce Paradigm & Spark RDDs (`map`, `reduceByKey`, Shuffle Barrier)** + **Broadcast Joins (`F.broadcast`)**: Classic key-value spatial bucketing avoids an expensive Cartesian shuffle.

---

### Track 2 (Dashrath Rajpurohit) — Stages 3 & 4: Uber H3 Spatial Indexing, Highway Types & Infrastructure Gap Index

#### Cell 8 (`Stage 3.1: C++ Osmium Highway Extraction & Uber H3 Hierarchical Spatial Indexing`)
* **What It Does**:
  * Filters `w/highway=motorway,trunk` from the `1.59 GB` `india-latest.osm.pbf` file, extracting **`155,859` real highway segments** (`19,998` `motorway` Expressways + `135,861` `trunk` National Highways).
  * Applies a **Spark UDF (`h3_res5_udf`, `h3_res7_udf`)** to index all `632` Silver Chargers, `21,402` OSM Primitives, and `155,859` Highway Segments into **Uber H3 Resolution-5 Macro-Hexagons** (`~252 km²` district/corridor level) and **Resolution-7 Micro-Hexagons** (`~5.16 km²` neighborhood level).
* **Layman Explanation for Your Team**:
  > *"Checking whether 155,859 highway segments are near 21,402 substations and 632 chargers using raw latitude/longitude math is computationally explosive. By converting every `(lat, lon)` point into an **Uber H3 Hexagon ID string** (like `853da117fffffff` for Delhi), two objects in the same area get the exact same string ID—turning complex 2D geometry into a fast Spark hash join on strings!"*
* **DA-231-O Lecture Note Reference**:
  * **Spatial Partitioning & User-Defined Functions (`F.udf`) in Distributed DataFrames**: Transforming 2D spatial range queries into 1D equi-join keys (`groupBy("h3_res5")`, `join(on="h3_res5")`).

---

#### Cell 9 (`Stage 4.1 & 4.2: H3 Hexagon Gap & State EV-Charging Infrastructure Gap Analysis`)
* **What It Does**:
  * **`stage_4_1_h3_hexagon_gap`**: Aggregates all **`8,181` active H3 Resolution-5 hexagons** across India, ranking top underserved highway hexagons (`#1 Muradnagar 853da1affffffff` with `391` highway segments & `0` chargers; `#2 Avadi 85618eb7fffffff`; `#6 Patna 853c138ffffffff` with `262` highway segments, `3` substations & `0` chargers).
  * **`stage_4_2_state_ev_gap_analysis`**: Standardizes dirty state names across Vahan and `OperationalPC.csv` (`34` States/UTs) and computes `ev_adoption_pct`, `spending_capacity_4w_share_pct` (4W-to-total EV share as a spending capacity proxy), `veh_to_infra_ratio` (`VIR`), and composite `infrastructure_gap_index` (`IGI`).
* **Layman Explanation for Your Team**:
  > *"We analyze the EV gap at two zoom levels: first, across **8,181 H3 Hexagons** so highway planners can see exact road corridors with heavy expressway traffic and existing substations but zero chargers; second, across all **34 Indian States/UTs** combining Vahan EV sales, 4-Wheeler spending capacity, and operational charging stations. Karnataka (`VIR = 300.7:1`, `IGI = 0.599`), Gujarat (`VIR = 381.1:1`), and Chandigarh (`30.78%` 4W share, `VIR = 439.9:1`) emerge as the top underserved states."*

---

### Track 3 (Kumar Bhawenshu) — Stages 5 & 6: Pregel Highway Graph, Machine Learning at Scale & Test-Location Verification

#### Cell 10 (`Stage 5.1: Pregel Highway Graph Accessibility & PageRank Centrality`)
* **What It Does**:
  * Models India's top `450` highway/urban H3 hubs as graph vertices $V$ and builds `2,244` directed highway edges $E$ (`<= 220 km`, Haversine $\times 1.22$ road tortuosity).
  * Runs **Pregel Vertex-Centric Message Passing (Bulk Synchronous Parallel)** to compute Multi-Source Shortest-Path road distance to the nearest charger (`shortest_charger_dist_km`) and **Iterative PageRank Centrality** ($\alpha = 0.85$).
  * **Result**: Converges in **7 supersteps**, identifying **`46` Critical Underserved Clusters (`>150 km`)** with an average unserved distance of **`84.42 km`**.
* **Layman Explanation for Your Team**:
  > *"Hexagons alone don't tell a highway driver how far they must drive along the road network if their hexagon has zero chargers. So we built a **Highway Graph (`450` hubs, `2,244` road edges)** and ran Google's **Pregel vertex-centric message-passing algorithm**. Hubs with chargers start at `0 km` and pass their distance plus road length to neighbors in synchronized supersteps. In **7 supersteps**, every hub found its exact road distance to the nearest charger, exposing 46 critical highway charging deserts (`>150 km`) like Kota (`380.9 km`) and Sambalpur (`308.4 km`)."*
* **DA-231-O Lecture Note Reference**:
  * **Graph Analytics (Pregel / Giraph — Bulk Synchronous Parallel [BSP] Model)**: Vertex-centric state updates, message passing along out-edges, superstep synchronization barriers, and convergence detection (`active_messages == 0`).

---

#### Cell 11 (`Stage 6.1 & 6.2: Spark ML + TensorFlow + Cross-Silo FedML & Test-Location Result Verification`)
* **What It Does**:
  1. **Machine Learning at Scale (`stage_6_1_ml_at_scale_forecasting`)**:
     * Engineers temporal lag features (`prev_month_ev`, `rolling_3m_ev`, `is_4w`, `ev_penetration_pct`) via Spark Window functions.
     * Benchmarks **Spark ML `RandomForestRegressor`** (`R² = 0.8653`, `RMSE = 1,192.02`), **Spark ML `GBTRegressor`** (`R² = 0.8567`), **TensorFlow Deep Neural Network** (`R² = 0.5906`), and **Cross-Silo Federated Learning (`FedAvg`)** across 4 Regional RTO Silos (*North, South, West, East/NE*, achieving `R² = 0.6922` without sharing raw state rows).
  2. **Test-Location Siting & Result Verification (`stage_6_2_test_location_siting_and_verification`)**:
     * Selects the **Top `K=25` Optimal New Test Locations** by combining Pregel distance gap, PageRank centrality, highway traffic, and `power=substation` availability, matching each hexagon to a **real OpenStreetMap POI** (*Reliance Jio Petrol Pump Kota*, *Indian Oil Muradnagar*, *Patna Highway Hub*, *Chhedanagar Substation*).
     * Re-runs the **Pregel Highway Graph** after injecting those 25 stations:
       * **Mean Unserved Hub Distance**: Drops from **`84.42 km` $\rightarrow$ `59.88 km` (`-29.07%` reduction)**!
       * **Critical Underserved Clusters (`>150 km`)**: Drops from **`46` $\rightarrow$ `18` (`60.87%` resolved)**!
* **DA-231-O Lecture Note Reference**:
  * **Machine Learning at Scale (Spark ML Pipelines, Data-Parallel Ensemble Training, TensorFlow Deep Learning, and Cross-Silo Federated Averaging `FedAvg` $w^{(t+1)} = \sum_k \frac{n_k}{N} w_k^{(t)}$)**.

---

### Track 4 (Chennuri Manikanta) — Stage 7: Scalability Evaluation, Polyglot Persistence, Streaming CEP & Interactive Maps

#### Cells 12 & 13 (`Stage 7.1 & 7.2: Scalability Benchmark, Polyglot SQL/NoSQL, Streaming CEP & Visual Dashboard`)
* **What It Does**:
  1. **Data Scalability Evaluation (`0.25x` to `4.00x`)**: Scales spatial input from `44,947` records (`13.72 MB`) up to **`709,676` records (`216.58 MB`)**, demonstrating sub-second execution (`0.908s`) and linear throughput scaling from **`44,588 records/sec` (`0.0133 GB/s`) up to `781,836.6 records/sec` (`238.60 MB/s` = `0.2330 GB/s`)**.
  2. **Polyglot Persistence (`SQLite` RDBMS + `NoSQL` Key-Value H3 JSON + `GeoJSON`)**: Demonstrates **CAP Theorem** trade-offs (ACID/CP relational analytical queries vs. BASE/AP $O(1)$ key-value lookups by H3 key).
  3. **Windowed Streaming CEP**: Evaluates 5-minute tumbling window charging load against substation capacity (`power=substation`), raising `CRITICAL_GRID_OVERLOAD_DISPATCH_MOBILE_CHARGER` alerts when grid utilization exceeds `150%`.
  4. **Visualizations**: Generates the 4-panel dashboard (`ev_gap_analytics_dashboard.png`) and the multi-layer interactive Folium map with legend (`india_ev_gap_and_test_locations_map.html`).
* **DA-231-O Lecture Note Reference**:
  * **Databases (NoSQL, CAP Theorem, Cassandra/BigTable Key-Value vs. Relational SQL)** & **Streaming Systems (Kafka / Spark Structured Streaming Windowed CEP)**.
