# Databricks notebook source
# MAGIC %md
# MAGIC Some examples using Qwen-VL a Large Scale Vision and Language model

# COMMAND ----------

pip install transformers_stream_generator

# COMMAND ----------

from IPython.display import display, Image
from PIL import Image as PILImage

# Define the path to your image
image_path = '/Volumes/ben_mackenzie/default/receipts/Screenshot 2024-05-14 at 4.01.01 PM.png'

# Open the image using PIL
img = PILImage.open(image_path)

# Display the image
display(img)


# COMMAND ----------

from transformers import AutoModelForCausalLM, AutoTokenizer
from transformers.generation import GenerationConfig
import torch
import os
torch.manual_seed(1234)

os.environ['TRANSFORMERS_CACHE'] = '/Volumes/benmackenzie/default/llm_cache'

# Note: The default behavior now has injection attack prevention off.
tokenizer = AutoTokenizer.from_pretrained("Qwen/Qwen-VL-Chat", trust_remote_code=True)

# use bf16
model = AutoModelForCausalLM.from_pretrained("Qwen/Qwen-VL-Chat", device_map="cuda", trust_remote_code=True, bf16=True).eval()


# COMMAND ----------


# 1st dialogue turn
query = tokenizer.from_list_format([
    {'image': '/Volumes/ben_mackenzie/default/receipts/Screenshot 2024-05-14 at 4.01.01 PM.png'},
    {'text': 'Please extract the licence plate number, date, address and total purchase amount'},
])
response, history = model.chat(tokenizer, query=query, history=None)
print(response)



# COMMAND ----------



# Define the path to your image
image_path = '/Volumes/ben_mackenzie/default/images/Screenshot 2024-04-17 at 2.18.11 PM.png'

# Open the image using PIL
img = PILImage.open(image_path)

# Display the image
display(img)

# COMMAND ----------

# 1st dialogue turn
query = tokenizer.from_list_format([
    {'image': image_path},
    {'text': 'What correctional department runs the prison?'},
])
response, history = model.chat(tokenizer, query=query, history=None)
print(response)

# COMMAND ----------



image_path = '/Volumes/ben_mackenzie/default/images/Screenshot 2024-04-11 at 1.43.43 PM.png'

# Open the image using PIL
img = PILImage.open(image_path)

# Display the image
display(img)

# COMMAND ----------

query = tokenizer.from_list_format([
    {'image': image_path},
    {'text': 'from what you see in the image, what are the fairings constructed from?'},
])
response, history = model.chat(tokenizer, query=query, history=None)
print(response)

# COMMAND ----------

query = tokenizer.from_list_format([
    {'image': image_path},
    {'text': 'where is the bay door located?'},
])
response, history = model.chat(tokenizer, query=query, history=None)
print(response)


# COMMAND ----------

query = tokenizer.from_list_format([
    {'image': image_path},
    {'text': 'what are the wheel bins constructed from?'},
])
response, history = model.chat(tokenizer, query=query, history=None)
print(response)
