import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns



df = pd.read_csv("/Users/germandilio/airflow/dags/grades/Expanded_data_with_more_features.csv")

df.drop('Unnamed: 0', axis=1, inplace=True)

# Mapping the Studyhours
study_mapping = {
    '< 5': 'Less than 5 hours',
    '5 - 10': 'Between 5-10 hours',
    '> 10': 'More than 10 hours'
}

# Mapping the IsFirstChild
value_mapping = {
    'no': 0,
    'yes': 1
}

# Mapping the TestPrep
test_mapping = {
    'none': 0,
    'completed': 1
}

# Mapping the Schoolbus
bus_mapping = {
    'private': 0,
    'school_bus': 1
}

# Fixing the values in the column
df['WklyStudyHours'] = df['WklyStudyHours'].map(study_mapping)
df['IsFirstChild'] = df['IsFirstChild'].map(value_mapping)
df['TestPrep'] = df['TestPrep'].map(test_mapping)
df['TransportMeans'] = df['TransportMeans'].map(bus_mapping)

# Rename the column from 'education' to 'degree'
df.rename(columns={'TransportMeans': 'School_Bus'}, inplace=True)

# Interpolate for numericial value
df['NrSiblings'] = df['NrSiblings'].interpolate()

# Use Mode for categoricial columns
df['EthnicGroup'] = df['EthnicGroup'].fillna(df['EthnicGroup'].mode()[0])
df['WklyStudyHours'] = df['WklyStudyHours'].fillna(df['WklyStudyHours'].mode()[0])
df['ParentEduc'] = df['ParentEduc'].fillna(df['ParentEduc'].mode()[0])
df['ParentMaritalStatus'] = df['ParentMaritalStatus'].fillna(df['ParentMaritalStatus'].mode()[0])
# Use Mode for binary columns
df['IsFirstChild'] = df['IsFirstChild'].fillna(df['IsFirstChild'].mode()[0])
df['PracticeSport'] = df['PracticeSport'].fillna(df['PracticeSport'].mode()[0])
df['TestPrep'] = df['TestPrep'].fillna(df['TestPrep'].mode()[0])
df['School_Bus'] = df['School_Bus'].fillna(df['School_Bus'].mode()[0])

# Define the function for one-hot encoding
def perform_one_hot_encoding(df, column_name):
    # Perform one-hot encoding on the specified column
    dummies = pd.get_dummies(df[column_name], prefix=column_name, dtype=float)

    # Drop the original column and append the new dummy columns to the dataframe
    df = pd.concat([df.drop(column_name, axis=1), dummies], axis=1)

    return df

data = df.copy()

# Perform one-hot encoding on the categorical variables
for column in ['Gender', 'EthnicGroup', 'ParentEduc', 'LunchType', 'ParentMaritalStatus', 'PracticeSport', 'WklyStudyHours']:
    data = perform_one_hot_encoding(data, column)
    
data_with_only_mathscore = data.copy().drop('ReadingScore', axis=1).drop('WritingScore', axis=1)
data_with_only_readingscore = data.copy().drop('MathScore', axis=1).drop('WritingScore', axis=1)
data_with_only_writingscore = data.copy().drop('ReadingScore', axis=1).drop('MathScore', axis=1)

for curr_data in [(data, 'all_data'),(data_with_only_mathscore, 'only_mathscore'), (data_with_only_readingscore, 'only_readingscore'), (data_with_only_writingscore, 'only_writingscore')]:
    # Compute the correlation matrix
    correlation_matrix = curr_data[0].corr()
    #Graph I.
    plt.figure(figsize=(15, 10))
    sns.heatmap(correlation_matrix, cmap='coolwarm', linewidths=0.5, fmt='.2f')
    plt.title("Correlation Matrix Heatmap")
    plt.savefig(f'/Users/germandilio/airflow/results/correlation_matrix_{curr_data[1]}.png', dpi=300, bbox_inches='tight')


# # Graph II, III, IV: Correlation with MathScore, ReadingScore, WritingScore
for score_column in [('MathScore', data_with_only_mathscore), ('ReadingScore', data_with_only_readingscore), ('WritingScore', data_with_only_writingscore)]:
    # Create a heatmap of the correlations with the target column
    full_corr = data.corr()
    target_corr = full_corr[score_column[0]].drop(score_column[0])

    # Sort correlation values in descending order
    target_corr_sorted = target_corr.sort_values(ascending=False)

    plt.figure(figsize=(5, 10))
    sns.heatmap(target_corr_sorted.to_frame(), cmap="coolwarm", annot=True, fmt='.2f')
    plt.title(f'Correlation with {score_column[0]}')
    plt.savefig(f'/Users/germandilio/airflow/results/Correlation_with_{score_column[0]}.png', dpi=300, bbox_inches='tight')

    only_one_score_corr = score_column[1].corr()
    target_corr_one_score = only_one_score_corr[score_column[0]].drop(score_column[0])

    target_corr_one_score_sorted = target_corr_one_score.sort_values(ascending=False)

    plt.figure(figsize=(5, 10))
    sns.heatmap(target_corr_one_score_sorted.to_frame(), cmap="coolwarm", annot=True, fmt='.2f')
    plt.title(f'Correlation with {score_column[0]} without other scores')
    plt.savefig(f'/Users/germandilio/airflow/results/Correlation_with_{score_column[0]}_without_other_scores.png', dpi=300, bbox_inches='tight')

# Linear Regression
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
sc= SparkContext()
sqlContext = SQLContext(sc)

for data in [('MathScore', data_with_only_mathscore), ('ReadingScore', data_with_only_readingscore), ('WritingScore', data_with_only_writingscore)]:
    spark_dff = sqlContext.createDataFrame(data[1])
    features = spark_dff.columns
    features.remove(data[0])

    vectorAssembler = VectorAssembler(inputCols = features, outputCol = 'features')
    vdf = vectorAssembler.transform(spark_dff)
    vdf = vdf.select(['features', data[0]])
    # vdf.show(3)

    splits = vdf.randomSplit([0.7, 0.3])
    train_df = splits[0]
    test_df = splits[1]

    lr = LinearRegression(featuresCol = 'features', labelCol=data[0], maxIter=10, regParam=0.3, elasticNetParam=0.8)
    lr_model = lr.fit(train_df)
    print("Coefficients: " + str(lr_model.coefficients))
    print("Intercept: " + str(lr_model.intercept))

    trainingSummary = lr_model.summary
    print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
    print("r2: %f" % trainingSummary.r2)

    lr_predictions = lr_model.transform(test_df)
    lr_predictions.select("prediction",data[0],"features").show(5)
    lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
                    labelCol=data[0],metricName="r2")
    print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

    test_result = lr_model.evaluate(test_df)
    print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)

    predictions = lr_model.transform(test_df)
    predictions.select("prediction",data[0],"features").show(20)