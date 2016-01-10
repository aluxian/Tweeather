package org.apache.spark.ml.ann

import org.apache.hadoop.fs.Path
import org.apache.spark.ml._
import org.apache.spark.ml.classification.{MultilayerPerceptronParams => MultilayerPerceptronClassifierParams}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

/** Params for Multilayer Perceptron. */
trait MultilayerPerceptronParams extends MultilayerPerceptronClassifierParams with HasInputCol with HasOutputCol {}

/**
  * Each layer has sigmoid activation function.
  */
class MultilayerPerceptron(override val uid: String)
  extends Estimator[MultilayerPerceptronModel] with MultilayerPerceptronParams {

  def this() = this(Identifiable.randomUID("multilayerPerceptron"))

  /**
    * Set the name of the column which holds the input layer Vector.
    * @group setParam
    */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /**
    * Set the name of the column which holds the output layer Vector.
    * @group setParam
    */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
    * Set the name of the column which holds the predicted output layer Vector.
    * @group setParam
    */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /**
    * Set the sizes of the ANN layers.
    * @group setParam
    */
  def setLayers(value: Array[Int]): this.type = set(layers, value)

  /**
    * Set the stack size for LBFGSOptimizer.
    * @group setParam
    */
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  /**
    * Set the maximum number of iterations.
    * Default is 100.
    * @group setParam
    */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
    * Set the convergence tolerance of iterations.
    * Smaller value will lead to higher accuracy with the cost of more iterations.
    * Default is 1E-4.
    * @group setParam
    */
  def setTol(value: Double): this.type = set(tol, value)

  setDefault(
    inputCol -> "input",
    outputCol -> "output"
  )

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    SchemaUtils.checkColumnType(schema, $(outputCol), new VectorUDT)
    schema
  }

  /**
    * Train a model using the given dataset and parameters.
    *
    * @param dataset Training dataset
    * @return Fitted model
    */
  protected def train(dataset: DataFrame): MultilayerPerceptronModel = {
    val topology = FeedForwardTopology.multiLayerPerceptron($(layers), softmax = false)
    val trainer = new FeedForwardTrainer(topology, $(layers).head, $(layers).last)
    trainer.LBFGSOptimizer.setConvergenceTol($(tol)).setNumIterations($(maxIter))
    trainer.setStackSize($(blockSize))

    val data = dataset.select($(inputCol), $(outputCol)).map {
      case Row(input: Vector, output: Vector) => (input, output)
    }

    val model = trainer.train(data)
    new MultilayerPerceptronModel(uid, $(layers), model.weights())
  }

  override def fit(dataset: DataFrame): MultilayerPerceptronModel = {
    transformSchema(dataset.schema, logging = true)
    copyValues(train(dataset).setParent(this))
  }

  override def copy(extra: ParamMap): MultilayerPerceptron = defaultCopy(extra)

}

/**
  * Model based on the Multilayer Perceptron.
  * Each layer has the sigmoid activation function.
  *
  * @param uid uid
  * @param layersArray array of layer sizes including input and output layers
  * @param weights vector of initial weights for the model that consists of the weights of layers
  * @return prediction model
  */
class MultilayerPerceptronModel(override val uid: String, val layersArray: Array[Int], val weights: Vector)
  extends Model[MultilayerPerceptronModel] with MultilayerPerceptronParams with MLWritable {

  private val perceptron = FeedForwardTopology.multiLayerPerceptron(layersArray, softmax = false).getInstance(weights)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(predictionCol), new VectorUDT)
  }

  /**
    * Transforms the dataset by reading from [[inputCol]], running the perceptron prediction and storing
    * the predictions as a new column [[predictionCol]].
    *
    * @param dataset input dataset
    * @return transformed dataset with [[predictionCol]] of type [[Double]]
    */
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val predictUDF = udf { (input: Vector) =>
      perceptron.predict(input)
    }
    dataset.withColumn($(predictionCol), predictUDF(col($(inputCol))))
  }

  override def copy(extra: ParamMap): MultilayerPerceptronModel = {
    copyValues(new MultilayerPerceptronModel(uid, layersArray, weights), extra)
  }

  override def write: MLWriter = new MultilayerPerceptronModel.ModelWriter(this)

}

object MultilayerPerceptronModel extends MLReadable[MultilayerPerceptronModel] {

  override def read: MLReader[MultilayerPerceptronModel] = new ModelReader

  override def load(path: String): MultilayerPerceptronModel = super.load(path)

  /** [[MLWriter]] instance for [[MultilayerPerceptronModel]] */
  private class ModelWriter(instance: MultilayerPerceptronModel) extends MLWriter {

    private case class Data(layers: Array[Int], weights: Vector)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: layers, weights
      val data = Data(instance.layersArray, instance.weights)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }

  }

  private class ModelReader extends MLReader[MultilayerPerceptronModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[MultilayerPerceptronModel].getName

    override def load(path: String): MultilayerPerceptronModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath).select("layers", "weights").head()
      val layers = data.getAs[mutable.WrappedArray[Int]](0).toArray
      val weights = data.getAs[Vector](1)
      val model = new MultilayerPerceptronModel(metadata.uid, layers, weights)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }

  }

}
