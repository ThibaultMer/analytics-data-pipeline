\# Analytics Data Pipeline



This project demonstrates a simple data pipeline architecture, from raw data ingestion

to analytics-ready datasets.



\## Tech Stack

\- Python 3

\- Git / GitHub

\- Open Data APIs

\- CSV / JSON files



\## Roadmap

\- \[x] Bronze ingestion (API \& files)

\- \[ ] Silver transformations

\- \[ ] Gold aggregations and visualizations



\## Project Structure

analytics-data-pipeline/

│

├── data/

│ ├── input/ # Landing zone for source files

│ ├── bronze/ # Raw immutable data

│ ├── silver/ # Cleaned and structured data

│ └── gold/ # Analytics-ready datasets

│

├── src/

│ ├── extract/ # API data extraction

│ ├── ingest/ # File-based ingestion

│ ├── transform/ # Bronze → Silver transformations

│ └── load/ # Silver → Gold / BI



\## Data Layers

\- Input

\- Bronze

\- Silver

\- Gold



\## How to Run

\### API ingestion (Bronze)

```bash

python src/extract/paris\_bike\_bronze.py



\### File-based ingestion (Bronze)

python src/ingest/file\_to\_bronze.py



\## Design Principles

\- The Bronze layer is immutable by design.

\- Duplicate files may exist and are handled during Silver transformations.

\- Source data is never modified or deleted.



