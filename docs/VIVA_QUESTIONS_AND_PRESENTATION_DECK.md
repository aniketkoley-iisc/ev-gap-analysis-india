# EV Gap Analysis India: Faculty Viva Guide & Team Presentation Deck

**Course**: DSBA DA-231-O: Data Engineering at Scale (IISc Bangalore)  
**Instructor**: Prof. Yogesh Simmhan  
**Team**: Aniket Koley (Team Lead), Dashrath Rajpurohit (Track 2), Kumar Bhawenshu (Track 3), Chennuri Manikanta (Track 4)  
**Repository**: `https://github.com/aniketkoley-iisc/ev-gap-analysis-india`

---

## PART 1: COMPREHENSIVE FACULTY VIVA VOCE QUESTION BANK

---

### Category 1: Distributed Lakehouse & Storage Architecture (Lectures 2–5)

#### Q1: Why did you adopt the Medallion Architecture (Bronze $\to$ Silver $\to$ Gold) on Parquet rather than persisting all transformations directly into a relational database or single flat tables?
**Model Viva Answer:**
"We implemented a 3-tier Medallion architecture using Apache Parquet on HDFS for three fundamental data engineering reasons:
1. **Separation of Raw Lineage from Business Logic**: The Bronze layer preserves raw immutable GeoJSON/CSV payloads (OSM highway ways, OpenChargeMap points, Vahan vehicle registrations). This guarantees fault re-computability and auditability under Apache Spark lineage graphs (DAGs).
2. **Columnar Compaction & Snappy Compression**: Spatial analytical queries frequently project only coordinates and tags (e.g., `(lat, lon, highway_class)`). Parquet's columnar layout enables projection and predicate pushdown (`c_level >= 3` or `amenity = 'charging_station'`), reading only the byte offsets of the relevant columns rather than scanning full row records. Our Bronze dataset of 155,859 raw highway segments was compacted into snappy-compressed Parquet with dictionary encoding, reducing raw memory footprint by over 68%.
3. **Partition Pruning by Spatial Hierarchy**: At the Silver layer, records are partitioned by H3 hexagon index (Resolution 7 / Resolution 8) and administrative state. When downstream jobs analyze spatial bounding boxes (e.g., Karnataka NH-44 corridor), Spark skips entire partition directories via file-level metadata without disk I/O."

#### Q2: How did you handle spatial deduplication across heterogeneous datasets (OpenChargeMap, OSM, NITI Aayog), and what is the distributed complexity?
**Model Viva Answer:**
"Spatial entities ingested from diverse crowd-sourced and governmental registries often duplicate identical physical charging plazas with coordinate drift up to 50 meters.
1. **Algorithm**: We implemented an RDD-level spatial bucket join. We discretize coordinates into a high-precision spatial grid ($\approx 50\text{m}$ resolution) and compute pairwise geodesic Haversine distance between points falling within adjacent buckets. Points satisfying $d(p_1, p_2) \le 50\text{m}$ are collapsed into a canonical centroid, taking the maximum connector count and union of charging standards.
2. **Complexity & Skew Mitigation**: A naive Cartesian product is $O(N^2)$. Our spatial grid indexing confines pairwise comparisons to local grid cells, reducing average complexity to $O(N \log N)$. In high-density urban clusters (e.g., Bengaluru Indiranagar), bucket skew was mitigated by secondary salting on the join key. This spatial deduplication pipeline reduced 1,335 raw charging records down to 632 unique, verified physical stations—a 52.7% redundancy elimination."

---

### Category 2: Distributed Spatial Graph Analytics & Pregel (Lectures 6–10)

#### Q3: Explain your GraphX model and the Pregel formulation used to measure highway charging accessibility. Why Pregel over iterative map-reduce?
**Model Viva Answer:**
"We modeled India's national highway grid as a distributed property graph $G = (V, E)$ using Apache Spark GraphX:
- **Vertices ($V$)**: 8,181 Uber H3 hexagonal centroids (Resolution 7, edge length $\approx 1.22\text{ km}$, area $\approx 5.16\text{ km}^2$) representing highway traversal nodes. Each vertex holds properties: `(has_ev_station: Boolean, distance_to_ev: Double, nearest_station_id: Long)`.
- **Edges ($E$)**: 155,859 directed highway road segments connecting adjacent hexagon cells, with edge attribute `weight = segment_length_km`.
- **Pregel Formulation**:
  1. *Initialization*: For each vertex $v \in V$, if $v.\text{has\_ev} = \text{true}$, initial state is $0.0\text{ km}$; otherwise, $+\infty$.
  2. *Vertex Program ($vprog$)*: $d_v^{(t+1)} = \min(d_v^{(t)}, \text{received\_msg})$.
  3. *Send Message ($sendMsg$)*: For edge $(u \to v)$ with weight $w$, if $d_u + w < d_v$, message $d_u + w$ is emitted along the edge.
  4. *Merge Message ($mergeMsg$)*: $\min(m_1, m_2)$.
- **Why Pregel over Iterative Map-Reduce**: Iterative Map-Reduce serializes the entire graph state to HDFS between iterations, causing severe disk and network I/O bottlenecks. Pregel in GraphX maintains vertex and edge states in distributed memory across RDD partitions. Vertices that become inactive (no updated distance) vote to halt and consume zero computation in subsequent supersteps. The highway propagation converged in 7 supersteps, detecting 46 critical unserved clusters where nearest charging distance exceeded 150 km."

#### Q4: What does PageRank represent in your highway network, and how is it used in siting?
**Model Viva Answer:**
"PageRank over the highway topological graph evaluates the structural centrality and bottleneck vulnerability of transportation nodes. Vertices with high PageRank correspond to major highway junctions (e.g., golden quadrilateral intersections, bypass nodes) where multi-state freight and passenger flows converge. When scoring candidate sites for new chargers, PageRank acts as a structural demand multiplier: a charging station at a high-PageRank junction serves traffic spanning multiple incoming corridors rather than a single linear highway."

---

### Category 3: Machine Learning & Federated Learning (Lectures 11–13)

#### Q5: Contrast your Centralized Spark ML Random Forest model with the Track 3 Federated Learning (FedML) framework.
**Model Viva Answer:**
"We implemented both centralized and decentralized predictive pipelines:
- **Centralized Spark ML Pipeline**:
  - Aggregates all regional features (`ev_stock`, `substation_count`, `charger_count`, `traffic_density`) into a centralized DataFrame.
  - Implements distributed `RandomForestRegressor` (100 trees, max depth 8) and `GBTRegressor`.
  - Achieved $R^2 = 0.8653$ and $\text{RMSE} = 0.1824$ on demand prediction.
  - *Limitation*: Requires central pooling of state-level commercial EV charging session logs and regional DISCOM grid telemetry, which violates inter-state commercial confidentiality and data sovereignty regulations.
- **Track 3 Federated Learning (FedML - FedAvg)**:
  - Treats each regional DISCOM/State Transport Authority as an autonomous client node.
  - Raw usage records remain local on each edge/state node. Only gradient weight matrices $\mathbf{w}_k^{(t)}$ are transmitted to the central aggregator.
  - *Aggregation*: $\mathbf{w}^{(t+1)} = \sum_{k=1}^K \frac{n_k}{N} \mathbf{w}_k^{(t)}$.
  - Achieved $R^2 = 0.6922$ across 5 simulated state clients. While centralized RF achieves higher $R^2$ due to global visibility of joint distributions, Federated Learning provides differential privacy and zero raw telemetry sharing across DISCOMs."

---

### Category 4: Siting Optimization, Multi-Criteria Scoring & What is $K=25$?

#### Q6: Explain what $K=25$ represents in Cell 11 and Cell 13, and how those sites were validated.
**Model Viva Answer:**
"$K=25$ represents a pilot candidate deployment portfolio used to test and validate our optimization algorithm against real ground conditions:
1. **Mathematical Objective**: Select $K$ sites that maximize coverage gain subject to grid and amenity constraints:
   $$\max_{S \subset V, |S|=K} \sum_{v \in V} \Delta \text{Accessibility}(v \mid S)$$
2. **Multi-Factor Scoring Equation**:
   $$\text{Score} = 0.40 \cdot \hat{d}_{\text{unserved}} + 0.25 \cdot \text{TrafficDensity} + 0.15 \cdot \text{PageRank} + 0.10 \cdot \text{Substations} + 0.10 \cdot \text{AmenityCount}$$
3. **OSM Amenity Validation**: A high theoretical score is useless if placed in an open forest. We spatially cross-referenced top-ranked hexagons with OpenStreetMap POI coordinates (fuel stations, motels, restaurants, power substations).
4. **Counterfactual Validation**: When $K=25$ stations were added to the GraphX vertex RDD and Pregel was re-simulated:
   - National mean unserved highway distance dropped by **29.07%** (from $84.42\text{ km}$ to $59.88\text{ km}$).
   - Critical unserved gap clusters ($>150\text{ km}$) dropped by **60.87%** (from 46 down to 18).
   - This mathematically proves the efficiency of our distributed siting heuristic."

---

### Category 5: Scalability, Partitioning & Fault Tolerance (Lectures 4, 8, 14)

#### Q7: How did you configure Spark partition counts and handle shuffle data skew?
**Model Viva Answer:**
"1. **Partition Sizing**: We tuned `spark.sql.shuffle.partitions = 200` and configured `spark.sql.adaptive.enabled = true` (AQE) with `coalescePartitions = true`. For GraphX RDDs, we partitioned vertices and edges using `EdgePartition2D`, which guarantees that each edge is placed on a partition determined by vertex IDs, bounding communication overhead to $2\sqrt{P}-1$.
2. **Skew Handling**: High-density urban corridors (e.g., Delhi-NCR, Mumbai-Pune) produce disproportionately dense H3 hexagons. Left unmanaged, tasks processing these hexagons become stragglers. We mitigated this by applying isolated salting on join keys and leveraging AQE's `skewJoin.enabled = true` with split skew factor 5."

#### Q8: What happens if an executor node fails during Pregel superstep 5? How does Spark recover?
**Model Viva Answer:**
"GraphX RDDs maintain lineage graphs across supersteps. However, long iterative lineage graphs can cause stack overflows and expensive full-recomputation chains upon node failure. To guarantee fault tolerance, GraphX internally calls `checkpoint()` or persisted caching (`MEMORY_AND_DISK_SER`) on vertex and edge RDDs across iterative supersteps. If an executor hosting partition $P_i$ fails during superstep 5, Spark's DAG Scheduler reconstructs only the missing partition from the cached checkpoint of superstep 4 rather than recomputing from superstep 0."

---

## PART 2: 10-SLIDE TEAM PRESENTATION DECK (FOR TOMORROW'S TEAM SYNC)

---

### Slide 1: Title & Vision
- **Header**: Data Engineering at Scale: EV Charging Gap Analysis & Optimal Siting for Indian National Highways
- **Sub-header**: A Distributed Big Data & Graph Analytics Pipeline (IISc DSBA DA-231-O)
- **Team**: Aniket Koley (Lead), Dashrath Rajpurohit (Track 2), Kumar Bhawenshu (Track 3), Chennuri Manikanta (Track 4)
- **Visual**: End-to-end architecture banner showing Raw Geospatial Data $\to$ Spark Lakehouse $\to$ GraphX Pregel $\to$ Spark ML / FedML $\to$ Interactive Folium Dashboard.
- **Presenter Notes (Layman Terms)**:
  *"Good morning everyone. Today we are presenting our end-to-end Big Data Engineering project for EV infrastructure in India. Everyone knows India wants electric cars and trucks, but drivers have severe range anxiety on highways. Our project builds a distributed data engine that ingests real geospatial and vehicle data across India, analyzes the entire highway network as a massive graph, and mathematically pinpoints the exact locations where new chargers will have the highest impact."*

---

### Slide 2: The Core Problem & Indian Highway Reality
- **Header**: Why Highway EV Siting is a Massive Distributed Data Challenge
- **Key Points**:
  - **The Range Anxiety Paradox**: Urban areas have charging clusters, but national highways have "charging blackouts" exceeding 150–300 km.
  - **Data Fragmentation**: Infrastructure data is scattered across OpenChargeMap, OpenStreetMap, NITI Aayog, and state Vahan portals.
  - **Volume & Topology**: Over 155,000 highway segments and millions of vehicle registrations cannot be processed on a single laptop without distributed computing.
- **Presenter Notes**:
  *"If you buy an EV in Bangalore and want to drive to Mumbai or Hyderabad, you can't just rely on regular city chargers. Right now, charging data is completely fragmented across government portals and crowd-sourced apps. We needed a unified big data engine that can combine road networks, electrical grids, and vehicle traffic to find where the critical charging gaps exist."*

---

### Slide 3: End-to-End System Architecture (The Big Picture)
- **Header**: 4-Track Integrated Architecture on Apache Spark Lakehouse
```
[Raw Sources: OSM, OpenChargeMap, Vahan]
                     │
                     ▼
[Track 1: Distributed Lakehouse (HDFS + Parquet)]
                     │
         ┌───────────┴───────────┐
         ▼                       ▼
[Track 2: GraphX Pregel]    [Track 3: Spark ML & FedML]
(155k Highway Graph)         (Demand Prediction: RF vs FedAvg)
         │                       │
         └───────────┬───────────┘
                     ▼
[Track 4: Multi-Criteria Siting & Interactive Visual Dashboard]
```
- **Presenter Notes**:
  *"Here is how the four tracks of our team fit together like puzzle pieces:
  Track 1 ingests and cleans the raw data into an HDFS Lakehouse.
  Track 2 builds a road graph to calculate how far any highway point is from an EV charger.
  Track 3 predicts future charging demand using both centralized Spark ML and privacy-preserving Federated Learning.
  Track 4 combines these models to select optimal sites and visualize them on an interactive GIS dashboard."*

---

### Slide 4: Track 1 — Ingestion, Lakehouse & Spatial Deduplication
- **Header**: Ingesting Millions of Records with Fault-Tolerant Lineage
- **Key Metrics & Accomplishments**:
  - **Medallion Lakehouse**: Bronze (raw JSON/CSV) $\to$ Silver (H3 spatial partitions) $\to$ Gold (analytics tables).
  - **50-Meter Spatial Deduplication**: Reduced 1,335 raw charging records to 632 unique physical charging stations (52.7% duplicate removal).
  - **Performance**: 781,836 records/sec ingestion throughput ($0.233\text{ GB/sec}$) with snappy columnar Parquet.
- **Presenter Notes**:
  *"In Track 1, we tackled data quality. Because charging station apps are crowd-sourced, the same charger was often listed three times with slightly different GPS coordinates. We wrote a distributed spatial deduplication algorithm in PySpark that merged nearby duplicates within 50 meters, cleaning our dataset cleanly and reducing redundant records by over 52%."*

---

### Slide 5: Track 2 — Highway Graph Modeling & Pregel Supersteps
- **Header**: Transforming India's Road Network into a Distributed Property Graph
- **Graph Mechanics**:
  - **Vertices**: 8,181 Uber H3 Hexagons (Resolution 7) covering all national highway corridors.
  - **Edges**: 155,859 Highway road segments with physical kilometer distances.
  - **Pregel Algorithm**: Simulates distance propagation in 7 synchronous supersteps.
  - **Discovery**: Detected **46 Critical Charging Deserts** where nearest EV charger is $>150\text{ km}$ away.
- **Presenter Notes**:
  *"In Track 2, we converted India's highway network into an Uber H3 hexagonal grid. Instead of slow Google Maps routing, we ran Google Pregel on Apache Spark GraphX. In just 7 parallel supersteps, distance information flowed across the country, showing that 46 major highway sections are complete deserts where an EV would run out of battery."*

---

### Slide 6: Track 3 — Spark ML vs. Federated Learning (FedML)
- **Header**: Centralized AI Power vs. Decentralized Data Privacy
- **Comparison Table**:
  | Dimension | Centralized Spark ML (RF / GBT) | Decentralized FedML (FedAvg) |
  | :--- | :--- | :--- |
  | **Architecture** | Centralized Data Lake | 5 Regional DISCOM Edge Clients |
  | **Privacy** | Requires raw telemetry sharing | Zero raw data leaves DISCOM |
  | **Predictive Accuracy** | **$R^2 = 0.8653$**, $\text{RMSE} = 0.1824$ | **$R^2 = 0.6922$**, $\text{RMSE} = 0.3105$ |
  | **Best Use Case** | National Master Planning | Multi-State DISCOM Collaboration |
- **Presenter Notes**:
  *"For Track 3, we asked a key research question: Can state electricity boards collaborate without exposing proprietary customer data? We built both: a Centralized Spark ML model that predicts charging demand with high accuracy ($R^2=0.865$), and a Federated Learning model where raw data stays inside each state DISCOM while only model weights are exchanged."*

---

### Slide 7: Track 4 — Optimal Candidate Siting: The $K=25$ Concept
- **Header**: How the Algorithm Chooses Optimal Sites & Validates Ground Reality
- **Scoring Formulation**:
  $$\text{Composite Score} = 40\% \text{ Gap Distance} + 25\% \text{ EV Traffic} + 15\% \text{ PageRank} + 10\% \text{ Grid Power} + 10\% \text{ Amenities}$$
- **The $K=25$ Pilot Test**:
  - Selected top 25 high-priority sites across critical corridors (NH-44, NH-48, NH-16, NH-27).
  - Every single site was spatially validated against real OpenStreetMap fuel stations, hotels, and 66kV/11kV electrical substations.
- **Presenter Notes**:
  *"Now, what is $K=25$? When planning highway infrastructure, you don't build 5,000 stations on day one. You start with a pilot rollout of $K$ priority sites. We set $K=25$ as our test portfolio. Our algorithm scored every highway node based on gap distance, junction traffic, power substation proximity, and nearby amenities like dhabas and petrol pumps."*

---

### Slide 8: The Quantitative Impact (Before vs. After Simulation)
- **Header**: Mathematical Proof: What Happens When You Deploy the $K=25$ Portfolio?
- **Impact Metrics**:
  - **Mean Unserved Highway Distance**: Dropped from **$84.42\text{ km} \to 59.88\text{ km}$** (**29.07% overall improvement**).
  - **Critical Blackout Clusters ($>150\text{ km}$)**: Dropped from **$46 \to 18$** (**60.87% resolved!**).
  - **IEA Benchmark Ratio**: Aligned with the International Energy Agency benchmark ($15:1$ EV-to-charger ratio) for 2026–2030 projected demand.
- **Presenter Notes**:
  *"Here is the most exciting result of the project. We took those 25 recommended stations, added them back into our GraphX graph, and re-ran the Pregel simulation. The result: just 25 well-placed stations cut unserved highway distance across India by almost 30%, and eliminated over 60% of all severe blackout zones! This proves our algorithm works."*

---

### Slide 9: Live Interactive Dashboard Demo
- **Header**: Visualizing Highways, Gap Hotspots & Candidate Plazas
- **Dashboard Highlights**:
  - **Tile Layer**: High-definition Esri WorldStreetMap vector tiles (zero watermark / API key dependencies).
  - **Blue Polylines**: 155,859 active national highway network segments.
  - **Green Markers**: 632 verified existing EV charging plazas.
  - **Red / Amber Rings**: 46 identified critical unserved gaps ($>150\text{ km}$).
  - **Gold Stars**: The $K=25$ recommended charging plazas with full amenity metadata.
- **Presenter Notes**:
  *"In our live Folium notebook demo, you can pan and zoom into any corridor in India—whether it's Bangalore to Hyderabad on NH-44 or the Mumbai-Delhi expressway. The blue lines are highways, green dots are existing chargers, red circles are the dangerous gap areas, and gold stars are our algorithm's recommended new charging plazas."*

---

### Slide 10: Conclusion, Academic Contributions & Next Steps
- **Header**: Delivering Scalable Infrastructure Insights for India's Clean Mobility
- **Summary of Contributions**:
  1. Built a complete, production-grade PySpark Lakehouse running on distributed HDFS.
  2. Implemented iterative GraphX Pregel graph computing for nationwide infrastructure accessibility.
  3. Conducted the first comparative benchmark of Spark ML vs. FedML on Indian EV highway telemetry.
  4. Formulated a multi-criteria siting optimization engine validated by OSM physical amenities.
- **Future Work**: Dynamic real-time telemetry streaming via Apache Kafka for live queue time and charger availability.
- **Presenter Notes**:
  *"To wrap up: this project fulfills all course objectives of DSBA DA-231-O by combining distributed storage, graph computing, scalable machine learning, and optimization. We have prepared full documentation, notebook source code, and test suites on GitHub. Thank you, and we are ready for questions!"*
