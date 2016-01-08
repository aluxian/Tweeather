package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.streaming.TwitterUtils
import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import twitter4j.FilterQuery

/**
  * This script uses Twitter Streaming API to collect tweets which contain one or more emoji
  * characters and are written in English. It uses one or more Twitter apps, whose credentials
  * are stored '''com/aluxian/tweeather/res/twitter.properties'''.
  */
object TwitterEmoCollector extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    val ssc = new StreamingContext(sc, streamingInterval)
    val stream = TwitterUtils.createMultiStream(ssc, queryBuilder)

    stream
      .map(_.getText.replaceAll("[\\n\\r]+", " "))
      .repartition(sc.defaultParallelism)
      .saveAsTextFiles("/tw/sentiment/emo/collected/", "text")

    ssc.start()

    if (!ssc.awaitTerminationOrTimeout(streamingTimeout)) {
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  def queryBuilder(): FilterQuery = {
    new FilterQuery()
      .track(positiveEmoticons ++ negativeEmoticons: _*)
      .language("en")
  }

  val positiveEmoticons = Seq(
    "\uD83D\uDE0D", // SMILING FACE WITH HEART-SHAPED EYES
    "\uD83D\uDE0A", // SMILING FACE WITH SMILING EYES
    "\uD83D\uDE03", // SMILING FACE WITH OPEN MOUTH
    "\uD83D\uDE02", // FACE WITH TEARS OF JOY
    "\uD83D\uDE18", // FACE THROWING A KISS
    "\uD83D\uDE01", // GRINNING FACE WITH SMILING EYES
    "\uD83D\uDE1A", // KISSING FACE WITH CLOSED EYES
    "\uD83D\uDC95", // TWO HEARTS
    "\uD83D\uDC4C", // OK HAND SIGN
    "\uD83D\uDC4D", // THUMBS UP SIGN
    "\uD83D\uDE38", // GRINNING CAT FACE WITH SMILING EYES
    "\uD83D\uDE39", // CAT FACE WITH TEARS OF JOY
    "\uD83D\uDE3A", // SMILING CAT FACE WITH OPEN MOUTH
    "\uD83D\uDE3B", // SMILING CAT FACE WITH HEART-SHAPED EYES
    "\uD83D\uDE3D", // KISSING CAT FACE WITH CLOSED EYES
    "\uD83D\uDC93", // BEATING HEART
    "\uD83D\uDC96", // SPARKLING HEART
    "\uD83D\uDC97", // GROWING HEART
    "\uD83D\uDC99", // BLUE HEART
    "\uD83D\uDC9A", // GREEN HEART
    "\uD83D\uDC9B", // YELLOW HEART
    "\uD83D\uDC9C", // PURPLE HEART
    "\uD83D\uDC9D", // HEART WITH RIBBON
    "\uD83D\uDC9E", // REVOLVING HEARTS
    "\uD83D\uDC9F", // HEART DECORATION
    "\uD83C\uDF89", // PARTY POPPER
    "\uD83D\uDE0E", // SMILING FACE WITH SUNGLASSES
    "\uD83D\uDE00", // GRINNING FACE
    "\uD83D\uDE07", // SMILING FACE WITH HALO
    "\uD83D\uDE17", // KISSING FACE
    "\uD83D\uDE19", // KISSING FACE WITH SMILING EYES
    "\uD83D\uDE0B", // FACE SAVOURING DELICIOUS FOOD
    "\uD83D\uDC8B", // KISS MARK
    "\u2665", // BLACK HEART SUIT
    "\u2764", // HEAVY BLACK HEART
    "\u263A", // WHITE SMILING FACE
    ":)",
    ":-)",
    "=)",
    ":D"
  )

  val negativeEmoticons = Seq(
    "\uD83D\uDE12", // UNAMUSED FACE
    "\uD83D\uDE2D", // LOUDLY CRYING FACE
    "\uD83D\uDE29", // WEARY FACE
    "\uD83D\uDE14", // PENSIVE FACE
    "\uD83D\uDE33", // FLUSHED FACE
    "\uD83D\uDE48", // SEE-NO-EVIL MONKEY
    "\uD83D\uDE13", // FACE WITH COLD SWEAT
    "\uD83D\uDE16", // CONFOUNDED FACE
    "\uD83D\uDE1D", // FACE WITH STUCK-OUT TONGUE AND TIGHTLY-CLOSED EYES
    "\uD83D\uDE1E", // DISAPPOINTED FACE
    "\uD83D\uDE20", // ANGRY FACE
    "\uD83D\uDE21", // POUTING FACE
    "\uD83D\uDE22", // CRYING FACE
    "\uD83D\uDE23", // PERSEVERING FACE
    "\uD83D\uDE25", // DISAPPOINTED BUT RELIEVED FACE
    "\uD83D\uDE28", // FEARFUL FACE
    "\uD83D\uDE2A", // SLEEPY FACE
    "\uD83D\uDE2B", // TIRED FACE
    "\uD83D\uDE30", // FACE WITH OPEN MOUTH AND COLD SWEAT
    "\uD83D\uDE31", // FACE SCREAMING IN FEAR
    "\uD83D\uDE32", // ASTONISHED FACE
    "\uD83D\uDE35", // DIZZY FACE
    "\uD83D\uDE3E", // POUTING CAT FACE
    "\uD83D\uDE3F", // CRYING CAT FACE
    "\uD83D\uDE40", // WEARY CAT FACE
    "\uD83D\uDC4E", // THUMBS DOWN SIGN
    "\uD83D\uDC94", // BROKEN HEART
    "\uD83D\uDE08", // SMILING FACE WITH HORNS
    "\uD83D\uDCA9", // PILE OF POO
    "\uD83D\uDE15", // CONFUSED FACE
    "\uD83D\uDE1F", // WORRIED FACE
    "\uD83D\uDE27", // ANGUISHED FACE
    "\uD83D\uDE26", // FROWNING FACE WITH OPEN MOUTH
    "\uD83D\uDE2E", // FACE WITH OPEN MOUTH
    ":(",
    ":-("
  )

}
