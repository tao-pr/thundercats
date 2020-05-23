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
 
### 1.1 Physical Module

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

### 1.2 Model Module

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


### 1.3 Evaluation Module

To simplify the evaluation of machine learning models, the abstraction of 
the process is designed as follows.

```
- Measure          <-- Measurement of an individual feature
- MeasureVector    <-- Measurement of multiple feature at a time
```

Some subtypes of `Measure`

```
- Measure
  - RegressionMeasure
    - RMSE
    - MAE
    - PearsonCorr
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
