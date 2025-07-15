# Supplementary Bike‑Sharing Data Repository

> **Scope** – This repository accompanies the paper **“Data-Driven Insights into (E-)Bike-Sharing: Mining a Large-Scale Dataset on Usage and Urban Characteristics - Descriptive Analysis and Performance Modeling”** (accepted for publication, 2025) and provides the full relational data export (43 Mio km, 2.3 GiB compressed) required to reproduce all analyses, together with 1 000‑row excerpts for quick exploration.

## Authors

| Name                | Affiliation                                                                                                   | Email                    | Notes                                 |
|---------------------|--------------------------------------------------------------------------------------------------------------|--------------------------|---------------------------------------|
| Felix Waldner*     | Technical University of Munich, School of Engineering and Design, Institute of Automotive Technology, Germany | f.waldner@tum.de         | *Corresponding author, Equal contribution|
| Georg Balke        | Technical University of Munich, School of Engineering and Design, Institute of Automotive Technology, Germany |        georg.balke@tum.de                  | Equal contribution                   |
| Felix Rech          | Technical University of Munich, School of Computation, Information and Technology, Germany                   |                          |                                       |
| Martin Lellep       | The University of Edinburgh, School of Physics and Astronomy, United Kingdom                                 |                          |                                       |


---

## Repository layout

```text
.
├── full/                     # full‑resolution CSV exports
│   └── dataset.zip                
│       └── bikes.csv              (≈ 88 k rows )
│       └── bike_types.csv         (≈ 121 rows)
│       └── cities.csv             (267 rows)
│       └── city_areas.csv         (267 rows – WKT/WKB/GeoJSON geometries)
│       └── stations.csv           (≈ 13 k rows)
│       └── station_status.csv     (≈ 38 M rows, 1.7 GiB uncompressed)
│       └── trips.csv              (≈ 25 M rows, 2.7 GiB uncompressed)
├── sample/                   # first 1 000 rows of every file (handy for testing, exploration)
│   ├── bikes.csv
│   ├── …
├── etl/                      # Code used to process the raw data - just informational
│   ├── data_pipeline.py         # Data decompression, explosion, splitting, upload
│   └── schema_definition.sql    # Data inserts, upserts, trip extraction handled in-database (PostgreSQL)
└── README.md                 # you are here
```

## Data model & file schemas

All timestamps are **Unix epoch seconds in UTC**; join via `cities.timezone` to convert to local times. All coordinates are WGS‑84 (EPSG:4326). 
<details>
<summary><code>trips.csv</code> (25 210 627 rows)</summary>

| column                                     | unit | description                                   |
| ------------------------------------------ | ---- | --------------------------------------------- |
| bike\_id                                   | –    | Bike identifier; `bikes.id`                   |
| city\_id                                   | –    | Rental city; `cities.id`                      |
| time\_start                                | s    | Rental start (UTC)                            |
| lon\_start, lat\_start, lon\_end, lat\_end | °    | Start & end coordinates WGS-84                |
| station\_id\_start, station\_id\_end       | –    | Station IDs, `NULL` if free‑floating          |
| battery\_start, battery\_end               | %    | State of charge (e‑bikes only)                |
| duration                                   | s    | Trip duration                                 |
| distance                                   | m    | Great‑circle distance (PostGIS `ST_Distance`) |

</details>

<details>
<summary><code>cities.csv</code> (267 rows)</summary>

| column                     | unit | description                                         |
| -------------------------- | ---- | --------------------------------------------------- |
| id                         | –    | Primary key                                         |
| name                       | –    | Name of the bike‑sharing scheme                     |
| lat, lon                   | °    | Approximate city center                             |
| timezone                   | –    | IANA timezone string (e.g. `Europe/Paris`)          |
| country                    | –    | ISO‑3166 alpha‑3 country code                       |
| return\_to\_official\_only | bool | `true` if bikes must be left at a (virtual) station |

</details>

<details>
<summary><code>city_areas.csv</code> (267 rows)</summary>

| column        | description                     |
| ------------- | ------------------------------- |
| city\_id      | `cities.id`                     |
| geom\_ewkb    | Operational area estimated via DBSCAN in **EWKB** |
| geom\_ewkt    | … in **EWKT**                   |
| geom\_geojson | … in **GeoJSON**                |

</details>

<details>
<summary><code>bike_types.csv</code> (121 rows)</summary>

| column            | unit | description                 |
| ----------------- | ---- | --------------------------- |
| id                | –    | Technical bike type         |
| vehicle\_image    | –    | URL of representative image |
| name              | –    | Commercial name             |
| description       | –    | Free‑text description       |
| form\_factor      | –    | `regular` / `cargo`         |
| rider\_capacity   | –    | Typical seats (1, 2, …)     |
| propulsion\_type  | –    | `human` / `electric`        |
| max\_range        | m    | Nominal electric range      |
| battery\_capacity | Wh   | Battery energy              |

</details>

<details>
<summary><code>bikes.csv</code> (88 444 rows)</summary>

| column         | unit | description                     |
| -------------- | ---- | ------------------------------- |
| id             | –    | Bike identifier                 |
| bike\_type\_id | –    | Technical type; `bike_types.id` |
| computer\_id   | –    | On‑board computer identifier    |

</details>

<details>
<summary><code>stations.csv</code> (13 192 rows)</summary>

| column         | unit | description                     |
| -------------- | ---- | ------------------------------- |
| id             | –    | Station identifier              |
| city\_id       | –    | `cities.id`                     |
| name           | –    | Human‑readable label            |
| app\_number    | –    | Number shown to users           |
| terminal\_type | –    | Hardware generation (12 values) |
| place\_type    | –    | Unknown (23 observed values)    |
| bike\_racks    | –    | Regular parking positions       |
| special\_racks | –    | Charging racks                  |
| lon, lat       | °    | Location                        |

</details>

<details>
<summary><code>station_status.csv</code> (38 279 885 rows)</summary>

| column                     | unit | description                  |
| -------------------------- | ---- | ---------------------------- |
| station\_id                | –    | `stations.id`                |
| time                       | s    | Snapshot timestamp (UTC)     |
| bikes                      | –    | Total bikes currently docked |
| booked\_bikes              | –    | Bikes reserved by users      |
| bikes\_available\_to\_rent | –    | Bikes ready for rental       |
| free\_racks                | –    | Empty regular docks          |
| free\_special\_racks       | –    | Empty charging docks         |
| maintenance                | bool | `true` = offline             |

</details>

---

## Quick start

```bash
# 1. Clone with Git LFS for large files (optional but recommended)
$ git lfs install
$ git clone https://github.com/tumftm/european-bike‑sharing‑dataset.git

# 2. Decompress the dataset
$ cd full
$ unzip dataset.zip  

# 3. Spin up PostGIS (example)
$ createdb bikesharing
$ psql bikesharing -c 'CREATE EXTENSION postgis;'
$ ogr2ogr -f PostgreSQL PG:"dbname=bikesharing" trips.csv -nln trips -oo X_POSSIBLE_NAMES=lon_start -oo Y_POSSIBLE_NAMES=lat_start
```

For exploratory work you can start with the 1 000‑row **sample** files:

```python
import pandas as pd
import geopandas as gpd

trips = pd.read_csv('sample/trips.csv')
# ...
# You might want to perform the EWKB conversion manually
city_areas = gpd.read_file("sample/city_areas.csv")
city_areas = city_areas.set_geometry(gpd.GeoSeries.from_wkb(city_areas.geom_ewkb))
```


## License

The dataset is distributed under the **Creative Commons Attribution - NonCommercial 4.0 International  (CC BY-NC 4.0)** license.  When using or adapting the data, please cite the paper *and* link back to this repository.

```
@article{waldnerbalke2025,
  title={Data-Driven Insights into (E-)Bike-Sharing: Mining a Large-Scale Dataset on Usage and Urban Characteristics - Descriptive Analysis and Performance Modeling},
  author={Waldner, Felix. and Balke, Georg and Rech, Felix and Lellep, Martin},
  year={2025},
  journal={Transportation}
}
```

## Contact

* **Felix Waldner** – [felix.waldner@tum.de](mailto:felix.waldner@tum.de)
* **Georg Balke** – [georg.balke@tum.de](mailto:georg.balke@tum.de)
* **Institute of Automotive Technology**, TU Munich, Germany – [sekretariat.ftm@ed.tum.de](mailto:sekretariat.ftm@ed.tum.de)

---
