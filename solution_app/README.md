# Batch Processor Exercise

This exercise showcase what it takes to process batch data, text files, with column width delimited.

## Steps

- Process data that is delimited by the width of each column
- Apply data cleaning
- Load the data into a table (PostgreSQL for this case)

## Python Libraries Used
- Pandas (To process specs and cleanup the Data)
- SqlAlchemy (To push all of the DataFrame to PostgreSQL)

## Main Methods 
- [get_specs](https://github.com/DAgustinus/data_upload_proto/blob/92799ff1e016c827be645e0d58799be28f585eb3/solution_app/util/data_importer.py#L35):
  - Search all spec files and gather all of the prefixes
  - Store all of the spec details in memory (dictionary)
- [get_data_files](https://github.com/DAgustinus/data_upload_proto/blob/92799ff1e016c827be645e0d58799be28f585eb3/solution_app/util/data_importer.py#L52):
  - Get all of text files based on the prefixes from get_specs and placed them in self.data_files
  - Filter out files that have “_processed.txt” as their suffix 
- [process_files](https://github.com/DAgustinus/data_upload_proto/blob/92799ff1e016c827be645e0d58799be28f585eb3/solution_app/util/data_importer.py#L66):
  - Process all of the files within self.data_files and concat the DataFrame
  - Rename each files that has been processed and add “_processed” 
- [df_to_psql](https://github.com/DAgustinus/data_upload_proto/blob/92799ff1e016c827be645e0d58799be28f585eb3/solution_app/util/postgresql_uploader.py#L18):
  - For each of the specs, push each of the DataFrame to Table

## Pipeline Design
![pipeline design](https://github.com/DAgustinus/data_upload_proto/blob/main/solution_app/pipeline_design.png)
