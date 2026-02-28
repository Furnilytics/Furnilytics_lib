from furnilytics import Client

cli = Client(api_key="<your_api_key_here>")

print(cli.list_topics().head())# prints topics
print(cli.list_datasets_flat().head())# prints datasets

meta, cols = cli.dataset_info("macro_economics","ppi","woodbased_panels_ppi")# topic, subtopic, dataset
print(meta)# prints metadata of the dataset
print(cols.head())# prints columns of the dataset

print(cli.get("macro_economics","ppi","woodbased_panels_ppi", limit=5))# get first 5 rows of dataset