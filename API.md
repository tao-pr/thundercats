# API guide

Instruction on how to use Thundercats

## 1. Architecture

Thundercat shapes the API structure as following.

- physical
- model
- evaluation

Most functions inside the packages return `MayFail[_]` monad which may resolve into:

- Fail(message)
- IgnoreableFail(message, data)
- Ok(data)

### 1.1 IO Module

To interact with the in/output screen, you can do following

```scala
import com.tao.thundercats.physical._

Screen.showDF(df, title=Some("My dataframe"), showOpt=Show.Default)
Screen.showSchema(df)
```

So when you write it in Monadic syntax, it looks like

```scala
for {
  a <- Read.csv(...)
  _ <- Screen.showDF(a, Some("Raw data"), Show.Truncate)
  _ <- Screen.showSchema(a)
} yield a
```

Use `Transform` to execute a normal Spark syntax in a Monad block

```scala
for {
  a <- Read.csv(...)
  a <- Transform.apply(a, (df) => {
    // Any transformation code here
    df.withColumn("v", lit(0))
  })
} yield a
```
 
### 1.2 Physical Module

Abstract design of all operations which have to do with physical storage, files, etc.

```scala
import com.tao.thundercats.physical._
```

All major operations are grouped and contained inside objects. 
Each of these operations always return `MayFail[DataFrame]` monad.

```scala
Join.left(...)
Join.inner(...)
Join.broadcast(...)
Group.agg(...)
Optimise.snapshot(...)
Filter.where(...)
```

So you can chain these operations, monad-like.

```scala
val result =
for {
  a <- Read.csv("/path/to/csv", withHeader=true, delimeter=":")
  b <- Read.parquet("/path/to/parquet")
  c <- Read.kafka("topic", "server:9092", ColumnEncoder.Avro("schema"))

  d <- Join.inner(a, b, Join.With('index))
  e <- Join.broadcast(d, e, on=Seq("date"), rightColumns=Seq("person","dept"))

  _ <- Write.kafka(e, "topic", "server:9092")
  _ <- Write.kafka(e, "topic2", "server:9092")
}
yield ()

if (result.isFailing){ ... }
```

From above, an operation stops and captures error message as soon as 
a failure occurs. 

### 1.3 Model Module

All machine learning and statistical methods go here in this module. 
The abstraction of the model is shown below.

```
- ModelDesign      <-- Defines structure of model for training
- Specimen         <-- Trained model from [[ModelDesign]]
```

To train a model

```scala
val design: ModelDesign = ???
val feature: FeatureColumn = AssemblyFeature(Array("f1","f2","f3"))
val model: Specimen = design.toSpecimen(feature, df)
```

To use trained model for prediction

```scala
TBD
```


### 1.4 Evaluation Module

To simplify the evaluation of machine learning models, the abstraction of 
the process is designed as follows.

```
- Measure          <-- Measurement of an individual feature
  - %              <-- Measure as scalar 
  - %%             <-- Measure as map (threshold -> scalar)
                       For classification evaluation
- MeasureVector    <-- Measurement of multiple feature at a time
```

Some subtypes of `Measure`

```
- Measure
  - RegressionMeasure
    - RMSE
    - MAE
    - PearsonCorr
  - ClassificationMeasure
    - RMSE
    - MAE
    - AUC
    - AUCPrecisionRecall
    - Precision     <-- with %% for threshold evaluation
    - Recall        <-- with %% for threshold evaluation
    - FMeasure      <-- with %% for threshold evaluation
```

Some subtypes of `MeasureVector`

```
- MeasureVector
  - RegressionMeasureVector
    - ZScore
```

To evaluate the features by predefined methods

```scala
val design: ModelDesign = ???
val feature: FeatureColumn = AssemblyFeature(Array("f1","f2","f3"))
val model: Specimen = design.toSpecimen(feature, df)

val mae: MayFail[Double] = model.score(df, MAE)
val rmse: MayFail[Double] = model.score(df, MAE)
val zscores: MayFail[Vector[Double]] = model.scoreVector(df, ZScore)
```

To compare multiple features, and identify the best model, best feature out of them

```scala
val design: ModelDesign = ???
val feature = List(Feature("f1"), Feature("f2"), Feature("f3"))
val (bestScore, bestCol, bestSpecimen) = new RegressionFeatureCompare(PearsonCorr)
  .bestOf(design, features, df)
  .get
```

Or, measure all features

```scala
val design: ModelDesign = ???
val feature = List(Feature("f1"), Feature("f2"), Feature("f3"))
val scores = new RegressionFeatureCompare(PearsonCorr)
  .allOf(design, features, df)
  .get
```

### 1.5 Evaluate combinations of features

Try combinations of feature columns by

```scala
val selector = new FeatureAssemblyGenerator(
  minFeatureCombination=1,
  maxFeatureCombination=3,
  ignoreCols=List("i"))

// Preset linear regression pipeline
// NOTE: Always use "features" column
val estimator = Preset.linearReg(Feature("features"), "i", "z")
val combinations = selector.genCombinations(estimator, df)
val design = FeatureModelDesign(
  outputCol="z",
  labelCol="i",
  estimator=estimator)

// Measure feature combinations with MAE
val bestModel = new RegressionFeatureCompare(MAE)
  .bestOf(design, combinations, df.toDF)
  .get

```

### 1.6 Cross validation

Thundercats supports model validation with train & test split and cross validation.

Train-test split

```scala
val cv = SplitValidation(
  measure=MPE,
  trainRatio=0.65f
)

val feature = AssemblyFeature("v"::Nil, "features")
val design = FeatureModelDesign(
  outputCol="z",
  labelCol="i",
  estimator=Preset.linearReg(features=feature, labelCol="i", outputCol="z"))
val score = cv.run(dfPreset, design, feature)
```

Cross validation

```scala
val cv = CrossValidation(
  measure=MPE,
  nFolds=5
)

val feature = AssemblyFeature("v"::Nil, "features")
val design = FeatureModelDesign(
  outputCol="z",
  labelCol="i",
  estimator=Preset.linearReg(features=feature, labelCol="i", outputCol="z"))
val score = cv.run(dfPreset, design, feature)
```

### 1.7 Feature Selection

To choose a subset of features, Thundercats offers a few choices

- FeatureSelector (Base)
  - ZScoreFeatureSelector(significance)
  - BestNFeaturesSelector(top, measure)

The feature selection code looks like

```scala
val selector = ZScoreFeatureSelector(Significance90p) // 90% confidence
val model: ModelDesign = ???
val features: Iterable[FeatureColumn] = ???

val subfeatures = select.selectSubset(
  df,
  model,
  features)
```

