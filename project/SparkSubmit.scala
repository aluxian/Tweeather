import sbtsparksubmit.SparkSubmitPlugin.autoImport._

sealed case class Config(name: String, memory: Option[Int] = None)

object SparkSubmit {
  lazy val configurations = SparkSubmitSetting(
    SparkSubmitSetting("Sentiment140Downloader",
      Seq("--class", "com.aluxian.tweeather.scripts.Sentiment140Downloader")
    ),
    SparkSubmitSetting("Sentiment140Parser",
      Seq("--class", "com.aluxian.tweeather.scripts.Sentiment140Parser")
    ),
    SparkSubmitSetting("Sentiment140Trainer",
      Seq("--class", "com.aluxian.tweeather.scripts.Sentiment140Trainer")
    ),
    SparkSubmitSetting("Sentiment140Repl",
      Seq("--class", "com.aluxian.tweeather.scripts.Sentiment140Repl")
    ),
    SparkSubmitSetting("TwitterEmoCollector",
      Seq("--class", "com.aluxian.tweeather.scripts.TwitterEmoCollector")
    ),
    SparkSubmitSetting("TwitterEmoCounter",
      Seq("--class", "com.aluxian.tweeather.scripts.TwitterEmoCounter"),
      Seq("--executor-memory", "14.4g")
    ),
    SparkSubmitSetting("TwitterEmoParser",
      Seq("--class", "com.aluxian.tweeather.scripts.TwitterEmoParser"),
      Seq("--executor-memory", "14.4g")
    ),
    SparkSubmitSetting("TwitterEmoTrainer",
      Seq("--class", "com.aluxian.tweeather.scripts.TwitterEmoTrainer"),
      Seq("--executor-memory", "14.4g")
    ),
    SparkSubmitSetting("TwitterEmoRepl",
      Seq("--class", "com.aluxian.tweeather.scripts.TwitterEmoRepl")
    ),
    SparkSubmitSetting("TwitterFireCollector",
      Seq("--class", "com.aluxian.tweeather.scripts.TwitterFireCollector")
    ),
    SparkSubmitSetting("TwitterFireCounter",
      Seq("--class", "com.aluxian.tweeather.scripts.TwitterFireCounter"),
      Seq("--executor-memory", "14.4g")
    ),
    SparkSubmitSetting("TwitterFireParser",
      Seq("--class", "com.aluxian.tweeather.scripts.TwitterFireParser"),
      Seq("--executor-memory", "14.4g")
    ),
    SparkSubmitSetting("TwitterFireTrainer",
      Seq("--class", "com.aluxian.tweeather.scripts.TwitterFireTrainer"),
      Seq("--executor-memory", "14.4g")
    ),
    SparkSubmitSetting("TwitterFireRepl",
      Seq("--class", "com.aluxian.tweeather.scripts.TwitterFireRepl")
    )
  )
}
