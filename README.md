KommatiPara Data Pipeline
The primary goal of this project is to build a data pipeline using PySpark for a small company named KommatiPara which deals with Bitcoin trading. The pipeline processes two separate datasets containing client and financial details to prepare a consolidated dataset for a new marketing initiative. This dataset will focus on clients from the United Kingdom and the Netherlands, providing insights into their financial interactions.

Pipeline Steps
Initializing Spark Session:
A Spark session is initialized to enable the processing of data using Spark DataFrame operations.

Loading Data:
Two datasets are loaded into DataFrames from specified file paths.

Filtering Data:
The client dataset is filtered to retain only the records of clients from by the user inputed countries.

Dropping Unwanted Columns:
Personal identifiable information, excluding emails, is removed from the client dataset.
Credit card number is removed from the financial dataset.

Joining DataFrames:
The client and financial datasets are joined on the id field to form a unified dataset.

Renaming Columns:
Column names are modified for better readability and understanding.

Saving the Output:
The final dataset is saved to the client_data directory in the root directory of the project.

Logging
Logging has been implemented to track the progress of the data pipeline and catch any errors that might occur during the data processing.

Testing
Unit tests have been written to ensure the correctness of the data transformations and the overall functionality of the pipeline.

GitHub Actions
GitHub Actions has been utilized to set up a continuous integration pipeline to automate the testing and ensure the codebase remains in a deployable state.