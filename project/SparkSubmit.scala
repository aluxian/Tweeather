import sbtsparksubmit.SparkSubmitPlugin.autoImport._

import scala.collection.mutable

object SparkSubmit {

  private lazy val sparkMaster = sys.env.getOrElse("TW_SPARK_MASTER", "local[*]")
  private lazy val executorHighMem = "14g"

  private sealed case class Script(scriptName: String, highMem: Boolean = false) {
    def toSparkSubmit = {
      val params = mutable.MutableList(
        "--class", s"com.aluxian.tweeather.scripts.$scriptName",
        "--master", sparkMaster
      )

      if (highMem) {
        params ++= "--executor-memory" :: executorHighMem :: Nil
      }

      SparkSubmitSetting(s"submit-$scriptName", params)
    }
  }

  private lazy val configs = Seq(
    Script("Sentiment140Downloader"),
    Script("Sentiment140Parser", highMem = true),
    Script("Sentiment140Trainer", highMem = true),
    Script("Sentiment140Repl"),
    Script("TwitterEmoCollector", highMem = true),
    Script("TwitterEmoCounter", highMem = true),
    Script("TwitterEmoParser", highMem = true),
    Script("TwitterEmoTrainer", highMem = true),
    Script("TwitterEmoRepl"),
    Script("TwitterFireCollector", highMem = true),
    Script("TwitterFireCounter", highMem = true),
    Script("TwitterFireExportHappiness", highMem = true),
    Script("TwitterFireExportWeather", highMem = true),
    Script("TwitterFireParser", highMem = true),
    Script("TwitterFireTrainer", highMem = true),
    Script("TwitterFireRepl")
  )

  lazy val configurations = SparkSubmitSetting(configs.map(_.toSparkSubmit): _*)

}
