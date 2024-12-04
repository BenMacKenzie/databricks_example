# Databricks notebook source
pip install mlflow mlflow-skinny[databricks]>=2.4.1 scikit-learn

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bmac.default.titanic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder


data= spark.sql('select * from bmac.default.titanic').toPandas()
# Drop columns that won't be used
data = data.drop(columns=['Name', 'Ticket', 'Cabin'])

# Define features and target
X = data.drop(columns=['Survived'])
y = data['Survived']

# Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Model

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model Definition

# COMMAND ----------

from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, f1_score, classification_report, confusion_matrix
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer



numerical_cols = X.select_dtypes(include=['float64', 'int64']).columns.tolist()
categorical_cols = X.select_dtypes(include=['object']).columns.tolist()

preprocessor = ColumnTransformer(
    transformers=[
        ('num', SimpleImputer(strategy='median'), numerical_cols),  # Handle missing numerical values
        ('cat', Pipeline([
            ('imputer', SimpleImputer(strategy='most_frequent')),  # Impute missing categorical values
            ('onehot', OneHotEncoder(handle_unknown='ignore'))  # One-hot encode categorical features
        ]), categorical_cols)
    ])

# Create a full pipeline with preprocessing and a Decision Tree model
pipeline = Pipeline([
    ('preprocessor', preprocessor),
    ('classifier', DecisionTreeClassifier(random_state=42))  # Decision Tree Classifier
])




# COMMAND ----------

# MAGIC %md
# MAGIC #### Train model

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature
mlflow.set_registry_uri("databricks-uc")
mlflow.sklearn.autolog()


# COMMAND ----------


data_sample = X_train.head(2)
signature = infer_signature(X_train, y_train)


with mlflow.start_run() as run:

    pipeline.fit(X_train, y_train)
    mlflow.sklearn.log_model(pipeline, 'model', signature=signature, input_example=data_sample)

     # Predict on training set and validation set
    y_test_pred = pipeline.predict(X_test)
 
    test_accuracy = accuracy_score(y_test, y_test_pred)
    test_f1 = f1_score(y_test, y_test_pred)
  
    mlflow.log_metrics({
        'test_accuracy': test_accuracy,
        'test_f1': test_f1
    })

   
    print(f"Logged to MLflow with run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test Model

# COMMAND ----------

import mlflow
#logged_model = 'runs:/43c970f8a9d14a289c60b625283e569d/model'
logged_model = f"runs:/{run.info.run_id}/model"

loaded_model = mlflow.sklearn.load_model(logged_model)

# Predict on a Pandas DataFrame.

loaded_model.predict(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Model in Unity Catalog

# COMMAND ----------


catalog = "bmac"
schema = "default"
model_name = "sklearn_titanic_2"
mlflow.register_model(logged_model, f"{catalog}.{schema}.{model_name}")

# COMMAND ----------

from mlflow.tracking import MlflowClient
client = MlflowClient()
client.set_registered_model_alias(f"{catalog}.{schema}.{model_name}", "champion", 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inference

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark

# COMMAND ----------

model = mlflow.sklearn.load_model(f"models:/{catalog}.{schema}.{model_name}@champion")
model.predict(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC #### As UDF

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import mlflow.sklearn

# Load the model
model = mlflow.sklearn.load_model(f"models:/{catalog}.{schema}.{model_name}@champion")

# Define a pandas UDF to use the model for predictions
@pandas_udf("double")
def predict_udf(*cols):
    import pandas as pd
    X = pd.concat(cols, axis=1)
    X.columns = ['PassengerId', 'Pclass', 'Sex', 'Age', 'SibSp', 'Parch', 'Fare', 'Embarked']  # Ensure column 
    return pd.Series(model.predict(X))

# Register the UDF
spark.udf.register("predict_udf", predict_udf)



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     PassengerId, 
# MAGIC     Pclass, 
# MAGIC     Sex, 
# MAGIC     Age, 
# MAGIC     Fare, 
# MAGIC     Embarked, 
# MAGIC     Survived,
# MAGIC     predict_udf(PassengerId, Pclass, Sex, Age, SibSp, Parch, Fare, Embarked) AS Predicted_Survival
# MAGIC FROM bmac.default.titanic

# COMMAND ----------

# MAGIC %md
# MAGIC #### As Endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import os

w = WorkspaceClient()

token = w.tokens.create(
    lifetime_seconds=3000
)
os.environ['DATABRICKS_TOKEN'] = token.token_value

# COMMAND ----------

import os
import requests
import numpy as np
import pandas as pd
import json

def create_tf_serving_json(data):
    return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
    url = 'https://adb-984752964297111.11.azuredatabricks.net/serving-endpoints/sklearn_titanic_v2/invocations'
    headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}', 'Content-Type': 'application/json'}
    ds_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
    data_json = json.dumps(ds_dict, allow_nan=True)
    response = requests.request(method='POST', headers=headers, url=url, data=data_json)
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')
    return response.json()
  
import pandas as pd

# Input JSON-like structure
data = {
    "columns": [
        "PassengerId",
        "Pclass",
        "Sex",
        "Age",
        "SibSp",
        "Parch",
        "Fare",
        "Embarked"
    ],
    "data": [
        [1, 3, "male", 22.0, 1, 0, 7.25, "S"],
        [2, 1, "female", 38.0, 1, 0, 71.2833, "C"]
    ]
}

# Convert to DataFrame
df = pd.DataFrame(data["data"], columns=data["columns"])




result = score_model(df)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC #### AI Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_query('sklearn_titanic_v2',
# MAGIC     request => named_struct(
# MAGIC       'PassengerId', 1,
# MAGIC       'Pclass', 3,
# MAGIC       'Sex', 'male',
# MAGIC       'Age', 22,
# MAGIC       'SibSp', 1,
# MAGIC       'Parch', 0,
# MAGIC       'Fare', 7.25,
# MAGIC       'Embarked', 'S'
# MAGIC     ),
# MAGIC     returnType => 'Float'
# MAGIC ) as predicted_survival
