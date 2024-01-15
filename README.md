# (AiC02) Multinational retail data centralisation

Project prompt:

> You work for a multinational company that sells various goods across the globe.
> 
>Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.
>
>In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.
>
>Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.
>
>You will then query the database to get up-to-date metrics for the business.

This project is part of the AiCore skill bootcamp.

## Installation

To execute the script, please install the latest version of Python and install the prerequisite libraries in `requirements.txt`. As of the time of writing, the Python version used was 3.12.

## Usage

The file `data_main.py` contains the pipeline to extract, clean and upload the data.

## File structure

- `credentials` folder: contains the API keys and information to connect to the remote and local DBs.
- `data_extraction.py`, `data_cleaning.py`: Python scripts responsible for extracting and cleaning the data.
- `database_utils.py`: Python script to facilitate connection and read/write to a database.
- `data_main.py`: Python script of the pipeline.

## License

Licensed under the [MIT](LICENSE.txt) license.