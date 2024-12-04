# Databricks notebook source
pip install catboost mlflow mlflow-skinny[databricks]>=2.4.1 scikit-learn shap


# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import catboost
from catboost import *
import shap


shap.initjs()

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score
from mlflow.models import infer_signature


# COMMAND ----------

df = spark.sql('select * from bmac.default.titanic').toPandas().dropna()
target = df.pop('Survived')
X_train, X_test, y_train, y_test = train_test_split(df, target, train_size=0.8)

# COMMAND ----------

categories = ['Cabin', 'Pclass', 'Sex', 'Embarked', 'Ticket', 'PassengerId', 'Name']
titanic_train_pool = Pool(X_train, y_train, cat_features=categories)
titanic_test_pool = Pool(X_test, y_test, cat_features=categories)

# COMMAND ----------

df_sample = X_train.head(2)
signature = infer_signature(X_train, y_train)


with mlflow.start_run() as run:
    
    model = CatBoostClassifier(iterations=100, depth=2, learning_rate=1, loss_function='Logloss')
    model.fit(titanic_train_pool, eval_set=titanic_test_pool, early_stopping_rounds=20, plot=True)

    # Predict and calculate metrics
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    accuracy = accuracy_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_pred_proba)

  
    # Log model, metrics, and parameters
    mlflow.catboost.log_model(model, 'model', signature=signature, input_example=df_sample)
  
    mlflow.log_metrics({
        'test_accuracy': accuracy,
        'test_roc_auc': auc
    })

    mlflow.log_params(model.get_all_params())

    print(f"Logged to MLflow with run ID: {run.info.run_id}")


# COMMAND ----------

shap_values = model.get_feature_importance(titanic_test_pool, type=EFstrType.ShapValues)

# COMMAND ----------

shap.summary_plot(shap_values[:,:-1], X_test)

# COMMAND ----------

import mlflow
#logged_model = 'runs:/e8cb97d1f9804138aa908349056391c1/model'
logged_model = f"runs:/{run.info.run_id}/model"
# Load model as a PyFuncModel.
loaded_model = mlflow.catboost.load_model(logged_model)

# Predict on a Pandas DataFrame.

loaded_model.predict(X_test)

# COMMAND ----------


catalog = "bmac"
schema = "default"
model_name = "titanic"

mlflow.register_model(logged_model, f"{catalog}.{schema}.{model_name}")

# COMMAND ----------

from mlflow import MlflowClient
client = MlflowClient()
client.set_registered_model_alias(f"{catalog}.{schema}.{model_name}", "champion", 4)

# COMMAND ----------

model = mlflow.catboost.load_model(f"models:/{catalog}.{schema}.{model_name}@champion")

# COMMAND ----------

model.predict(X_test)
