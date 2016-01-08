import sbtsparksubmit.SparkSubmitPlugin.autoImport._

sealed case class Config(name: String, memory: Option[Int] = None)

object SparkSubmit {
  lazy val configurations = SparkSubmitSetting(
    SparkSubmitSetting("submit-ClusterTest", Seq(
      "--class", "com.aluxian.tweeather.scripts.ClusterTest",
      "--master", "spark://spark.r.tweeather.aluxian.com:7077"
    )),
    SparkSubmitSetting("submit-Sentiment140Downloader", Seq(
      "--class", "com.aluxian.tweeather.scripts.Sentiment140Downloader"
    )),
    SparkSubmitSetting("submit-Sentiment140Parser", Seq(
      "--class", "com.aluxian.tweeather.scripts.Sentiment140Parser"
    )),
    SparkSubmitSetting("submit-Sentiment140Trainer", Seq(
      "--class", "com.aluxian.tweeather.scripts.Sentiment140Trainer"
    )),
    SparkSubmitSetting("submit-Sentiment140Repl", Seq(
      "--class", "com.aluxian.tweeather.scripts.Sentiment140Repl"
    )),
    SparkSubmitSetting("submit-TwitterEmoCollector", Seq(
      "--class", "com.aluxian.tweeather.scripts.TwitterEmoCollector"
    )),
    SparkSubmitSetting("submit-TwitterEmoCounter", Seq(
      "--class", "com.aluxian.tweeather.scripts.TwitterEmoCounter",
      "--executor-memory", "14.4g"
    )),
    SparkSubmitSetting("submit-TwitterEmoParser", Seq(
      "--class", "com.aluxian.tweeather.scripts.TwitterEmoParser",
      "--executor-memory", "14.4g"
    )),
    SparkSubmitSetting("submit-TwitterEmoTrainer", Seq(
      "--class", "com.aluxian.tweeather.scripts.TwitterEmoTrainer",
      "--executor-memory", "14.4g"
    )),
    SparkSubmitSetting("submit-TwitterEmoRepl", Seq(
      "--class", "com.aluxian.tweeather.scripts.TwitterEmoRepl"
    )),
    SparkSubmitSetting("submit-TwitterFireCollector", Seq(
      "--class", "com.aluxian.tweeather.scripts.TwitterFireCollector"
    )),
    SparkSubmitSetting("submit-TwitterFireCounter", Seq(
      "--class", "com.aluxian.tweeather.scripts.TwitterFireCounter",
      "--executor-memory", "14.4g"
    )),
    SparkSubmitSetting("submit-TwitterFireParser", Seq(
      "--class", "com.aluxian.tweeather.scripts.TwitterFireParser",
      "--executor-memory", "14.4g"
    )),
    SparkSubmitSetting("submit-TwitterFireTrainer", Seq(
      "--class", "com.aluxian.tweeather.scripts.TwitterFireTrainer",
      "--executor-memory", "14.4g"
    )),
    SparkSubmitSetting("submit-TwitterFireRepl", Seq(
      "--class", "com.aluxian.tweeather.scripts.TwitterFireRepl"
    ))
  )
}
