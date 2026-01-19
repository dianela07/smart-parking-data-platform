# Smart Parking Data Platform – Aarhus

This project fetches, processes, and visualizes parking data for **Aarhus, Denmark**. 
It is designed to help users and analysts understand parking occupancy trends and make predictions about parking availability.

## Features

- **Data Fetching:** Automatically downloads raw parking data from the Aarhus open data API.
- **Data Processing:** Converts raw JSON into a clean CSV format with the following columns:
  - `_id`: Record ID
  - `timestamp`: Time of the record
  - `name`: Parking garage code/name
  - `capacity`: Total parking spaces
  - `occupied`: Currently occupied spaces
- **Data Analysis & Visualization:** Ready for creating visualizations and predictive models.
- **CSV Export:** Processed data is saved at `data/processed/Aarhus_parking.csv`.



