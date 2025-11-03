 PHASE 1 — Excel Data Cleaning
1.	Opened the CSV file in Microsoft Excel.
2.	Removed duplicates based on the hostname field → Data → Remove Duplicates.
3.	Trimmed extra spaces using the =TRIM() function.
4.	Handled missing values:Replaced blanks with "Unknown".
5.	Standardized date format for operating_system_installation_date  ensured YYYY-MM-DD.
6.	Saved the cleaned file as: it_asset_inventory_enriched.csv

PHASE 2 — Indexing Data into Elasticsearch
1.	created index_data.py
2.	Connected to Elasticsearch Cloud using API Key authentication.
3.	Loaded the CSV with Pandas.
4.	Used helpers.bulk() to index data efficiently.
5.	All index_data.py data is showing in elastic search
    


PHASE 3 — Data Transformation & Enrichment
1.	created transform_data.py
2.	Reindexed data to a new index =it_assets_inventory_transformed
3.	Added a new derived field: risk_level = "High" if lifecycle_status in ["EOL", "EOS"] else "Low"
4.	Calculated system age (in years) using: current_year - installation_year
5.	Deleted records missing hostnames or with "Unknown" providers.
6.	Updated all valid records via _update_by_query.
         

PHASE 4 — Visualization & Insights
Created charts such as:
1.	Assets by Country 
2.	Lifecycle Status Distribution 
3.	High vs Low Risk Assets 
4.	Top OS Providers
 
 Sample Business Insight:
“40% of assets are End-of-Life (EOL), indicating an urgent need for OS upgrades, especially in India and Brazil.”

Final Business Insights
1.	The majority of assets are Unknown
2.	This indicates a need to standardize asset location data to improve tracking and regional analysis.
3.	50% of assets are currently Active, while 25% are Planned and 25% are End of Support (EOS).
4.	The EOS assets represent potential operational risk if not upgraded or replaced soon.
5.	75% of assets are in the Low Risk category, while 25% are High Risk due to outdated OS versions (EOL/EOS).
6.	These high-risk systems should be prioritized for upgrades or replacements.
7.	IBM holds the largest share of operating systems in the environment, followed by RedHat.
8.	Vendor concentration may lead to dependency risks — diversifying OS providers could improve resilience.
9.	40% of assets are EOL, indicating an urgent need for OS upgrades, especially in India and Brazil.

