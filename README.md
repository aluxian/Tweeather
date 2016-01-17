# Tweeather

Tweeather is a Machine Learning project that correlates Twitter sentiment to European weather.

I was inspired by a study where user behaviour on Twitter was used to build a predictive model of income: [Studying
User Income through Language, Behaviour and Affect in Social Media][1]. I decided it was the perfect opportunity to venture into the world of Big Data and so I learned Spark and Hadoop.

## Table of Contents

- [Requirements](#requirements)
- [Setup](#setup)
	- [Configuration](#configuration)
	- [Env vars](#env-vars)
	- [System properties](#system-properties)
	- [Submit tasks](#submit-tasks)
- [Scripts](#scripts)
	- [1. *Sentiment140* scripts](#1-sentiment140-scripts)
	- [2. *Emo* scripts](#2-emo-scripts)
	- [3. *Fire* scripts](#3-fire-scripts)
- [Conclusion](#conclusion)
- [Tips](#tips)
- [Suggestions](#suggestions)
- [Downloads](#downloads)

## Requirements

You need these tools to be able to run the project:

- Java 1.7+
- [scala-sbt][7]

And these clusters:

- [Apache Spark 1.6][6] (required for everything)
- [HDFS][8] (required only for multi-machine Spark clusters)

And the following hardware:

- one or more machines
	- preferably a single, powerful one so you don't need Hadoop
- at least 16GB of RAM
	- it should work with less, but make sure you edit the `executorHighMem` size in `SparkSubmit.scala` – see [Setup](#setup)
- at least 8 logical cores
	- it should work with less, but the more you have the better/faster scripts will run
	- to run the Collector scripts, you need to have at least `<number of Twitter apps> + 1` cores

## Setup

### Configuration

The project has 2 configuration files:

- `src/main/resources/com/aluxian/tweeather/res/twitter.properties`
	- used to provide Twitter credentials for the Collector scripts
- `src/main/resources/com/aluxian/tweeather/res/log4j.properties`
	- used to configure logging

Make sure you copy and configure these before running the scripts:

```
$ cp src/main/resources/com/aluxian/tweeather/res/log4j-template.properties \
	src/main/resources/com/aluxian/tweeather/res/log4j.properties
$ cp src/main/resources/com/aluxian/tweeather/res/twitter-template.properties \
	src/main/resources/com/aluxian/tweeather/res/twitter.properties
```

### Env vars

Make sure you set these up before running a script:

- `TW_SPARK_MASTER`
	- a url to a Spark master to connect to
	- if not specified, scripts will run on `local[*]`
	- by default, scripts will run in `client` deploy mode

### System properties

Tweeather supports the following custom system properties:

- `tw.streaming.timeout`
	- the period, in seconds, after which streaming should stop
	- default: *unlimited*
- `tw.streaming.interval`
	- the duration, in seconds, for each streaming batch
	- default: *5 minutes*

### Submit tasks

If your Spark cluster doesn't have at least 14GB of RAM, edit `executorHighMem` in `project/SparkSubmit.scala`.

## Scripts

The project has 3 sets of scripts.

### 1. *Sentiment140* scripts

I used these scripts to train a naive Bayes sentiment analyser with the [Sentiment140][2] dataset. Nothing fancy here. The resulting model has an accuracy of 80%.

#### Processing

The same processing steps were taken as for the [*Emo* scripts](#2-emo-scripts).

#### Running

To run the experiment:

```
# Download the datasets
$ sbt submit-Sentiment140Downloader

# Parse the datasets
$ sbt submit-Sentiment140Parser

# Train the sentiment analyser
$ sbt submit-Sentiment140Trainer

# Test the analyser
$ sbt submit-Sentiment140Repl
```

### Example

Here's an example of prediction done on the Sentiment140 test dataset:

```
|actual|predicted         |raw_text                                                                                                                                 |
|1.0   |0.9906351122104053|Obama's got JOKES!! haha just got to watch a bit of his after dinner speech from last night... i'm in love with mr. president ;)         |
|0.0   |0.3665541605686164|LEbron james got in a car accident i guess..just heard it on evening news...wow i cant believe it..will he be ok ? http://twtad.com/69750|
|1.0   |0.4466193778555213|is it me or is this the best the playoffs have been in years oh yea lebron and melo in the finals                                        |
|1.0   |0.5779917305269712|@khalid0456 No, Lebron is the best                                                                                                       |
|1.0   |0.7991567513906502|@the_real_usher LeBron is cool.  I like his personality...he has good character.                                                         |
|1.0   |0.5027089309407685|Watching Lebron highlights. Damn that niggas good                                                                                        |
|1.0   |0.1677252771491839|@Lou911 Lebron is MURDERING shit.                                                                                                        |
|1.0   |0.2183415128849378|@uscsports21 LeBron is a monsta and he is only 24. SMH The world ain't ready.                                                            |
|1.0   |0.9184651111073650|@cthagod when Lebron is done in the NBA he will probably be greater than Kobe. Like u said Kobe is good but there alot of 'good' players.|
|1.0   |0.6414672757144448|KOBE IS GOOD BT LEBRON HAS MY VOTE                                                                                                       |
|0.0   |0.7777182849481007|Kobe is the best in the world not lebron .                                                                                               |
|1.0   |0.5581821154365963|@asherroth World Cup 2010 Access?? Damn, that's a good look!                                                                             |
```

You can download a file with more examples from the [downloads section](#downloads).

### 2. *Emo* scripts

I used these scripts to train a naive Bayes sentiment analyser with tweets collected by myself. The resulting model had an accuracy of 75% and is available for download in the [downloads section](#downloads).

#### Collection

For collecting the tweets, I used [Twitter's Streaming API][3] with multiple apps configured. The average throughput was of 325 tweets/sec and I collected over 100M tweets in 4 days. However, after removing all the duplicates, only 8.4M remained.

The stream of tweets I received from the Twitter API was filtered by emoji characters. Tweets that contained positive emojis like :grin: were classified as positive, and tweets that contained negative emojis like :cry: were classified as negative. Tweets that contained both types of emojis were excluded.

This method allowed me to gather a fairly large dataset of labelled tweets, while the accuracy of the model didn't seem to suffer.

#### Processing

Before training the analyser, the tweets were pre-processed:

- the feature space was reduced
  - urls were replaced with `URL`
  - `@username` mentions were replaced with `USERNAME`
  - repeated letters were replaced with just 2 occurrences of the letter
- text was sanitized
  - punctuation was removed
  - multiple white spaces were replaced with just one
- stop words like "not", "is", "less", and "or" were removed

#### Training

I used 90% of the tweets for training and the remaining 10% for testing.

#### Running

To run the experiment:

```
# Collect tweets; leave this running for a few hours
$ sbt submit-TwitterHoseEmoCollector

# Parse the collected tweets
$ sbt submit-TwitterHoseEmoParser

# Train the sentiment analyser
$ sbt submit-TwitterHoseEmoTrainer

# Test the analyser
$ sbt submit-TwitterHoseEmoRepl
```

#### Screenshot

Here's a screenshot of my collector running for almost 4 days.

![Emo Collector](https://raw.githubusercontent.com/Aluxian/Tweeather/master/docs/ss-emo-collector.png)

#### Example

Here's an example of some tweets and their predicted polarity:

```
|lat       |lon       |polarity          |raw_text																																																																		|
|33.8733655|35.8495145|0.5220677359995693|@onikashabibi IM HOWLING																																																										|
|33.8733655|35.8495145|0.7705892813840320|am i the only one attracted to Hyde																																																					|
|33.8733655|35.8495145|0.6705864102035392|nicki and gaga better release a track one day or imma cut a bitch																																						|
|33.8733655|35.8495145|0.0851292224547309|@elissamk_ yeah I don't know.  Maybe schedule or major conflict																																							|
|33.8733655|35.8495145|0.4380899284568244|@TWlSTEDFANTASY by the show? it had worse seasons.																																													|
|33.8733655|35.8495145|0.8572554520257938|@ElieRustom well deserved.																																																									|
|33.8733655|35.8495145|0.0683891442591605|Wanted to wake up at 8 am for a morning jog but here I am at 3:37 am scanning Twitter for what I've missed																	|
|33.8733655|35.8495145|0.5289639040745624|@ayaalhakim_ @_NiZS lol same  between its potential and reality. I think its almost inescapably useless. No matter how relevant the content.|
|33.8733655|35.8495145|0.0462046697117165|You get a temporary high as you watch life pass you by Every single day you want to cry Can we wish the tears a fond goodbye #TroubledSoul	|
|33.8733655|35.8495145|0.9536491693812428|I love Yoda so much																																																													|
|33.8733655|35.8495145|0.9381788967926521|The world is completely fucked  99%  completely fucked..But what are we without our fantasies of causes  heroes  and grand battles.					|
|33.8733655|35.8495145|0.6512862750937641|@toogucciforyou someone's keeping me up ??																																																	|
|33.8733655|35.8495145|0.8849331055255223|@toogucciforyou Aflanne that's why ma bjarreb ektob Swedish?																																								|
|33.8733655|35.8495145|0.8364178901299960|Creativity is where you find who you are!																																																		|
|33.8733655|35.8495145|0.6532908315602334|@KrewdPoet all the better id say																																																						|
|33.8733655|35.8495145|0.4084283943748655|Putin Lists U.S. As One Of The Threats To Russia's National Security https://t.co/jd93Drk4N2 https://t.co/l7LKUN4A5C												|
|33.8733655|35.8495145|0.7771673205629203|A Clip on projector  WiGig and LTE all into a single lightweight Lenovo ThinkPad X1 Tablet. WoW! @lenovo  https://t.co/A32Q9RSl6A via @CNET	|
|33.8733655|35.8495145|0.9596987698902406|@Gulan_A thank you. I wish you all the best too																																															|
|33.8733655|35.8495145|0.2695436743946154|AFP: Saudi police shot at in home village of executed cleric																																								|
|33.8733655|35.8495145|0.2694895125816703|Which Countries Are the Most Expensive for Tourists? https://t.co/hgzR6kPIMH https://t.co/lQJHkFyq24																				|
|33.8733655|35.8495145|0.8311372110468187|Good Morning !????																																																													|
|33.8733655|35.8495145|0.1222478140991693|How do some not even feel the tiniest bit sorry for what they put others through? Curious																										|
|33.8733655|35.8495145|0.7039707972753375|@JoumanaGebara @JosephLF @alhayat_ksa joke of the day...																																										|
|33.8733655|35.8495145|0.0709410869696622|With laws preventing me from smoking while there are kids in the car  this will be me as a parent https://t.co/wwa2XiOaVW										|
|33.8733655|35.8495145|0.1955959507977803|ok im gonna go back to sleep now																																																						|
```

You can download the file with 1000 complete rows from the [downloads section](#downloads).

#### Sentiment across Europe

I collected tweets geo-localised in Europe created between 2015-12-26 and 2016-12-04. I ran them through the sentiment analyser, and this is the result:

![Happiness Levels in Europe](https://raw.githubusercontent.com/Aluxian/Tweeather/master/docs/happiness.gif)

The change in the number of data points seems to depend more on the time of day than on weather conditions. In order to draw a pertinent conclusion about the correlation between weather conditions and sentiment, a larger dataset of tweets is required (spread across more than just a week).

### 3. *Fire* scripts

I used these scripts to train an [artificial neural network][5] that predicted the sentiment polarity given 3 weather variables: temperature, pressure and humidity.

#### Collection

Tweets were collected using Twitter's Streaming API, filtered by location (Europe) and language (English).

#### Processing

After they were collected, tweets were ran through the sentiment analyser to get their polarity. The parser script used a [NOAA][4]-provided [weather dataset][17] to extract the temperature, pressure and humidity for each tweet's location.

#### Training

After processing the tweets, I used them to train a [multilayer perceptron][9]. The 3 weather variables were the input nodes and the polarity was the output node. 90% of the dataset was used for training and the remaining 10% for testing.

#### Running

To run the experiment:

```
# Collect tweets; leave this running for a few hours
$ sbt submit-TwitterHoseFireCollector

# Parse the collected tweets
# This parser uses the "emo" sentiment analyser, make sure
#   you've trained it first or edit the script to use the other model
$ sbt submit-TwitterHoseFireParser

# Train the sentiment analyser
$ sbt submit-TwitterHoseFireTrainer

# Test the analyser
$ sbt submit-TwitterHoseFireRepl
```

#### Screenshot

Here's a screenshot of my collector running for almost 9 days.

![Fire Collector](https://raw.githubusercontent.com/Aluxian/Tweeather/master/docs/ss-fire-collector.png)

## Conclusion

> TODO

## Tips

Keep these in mind:

- use a powerful machine (otherwise, a cluster might be required)
- use the Spark UI to watch your script's progress on [http://localhost:4040](http://localhost:4040)
- you can download my trained models from the [downloads section](#downloads); just place them where the trainers would save them (see the scripts' source code) and you're good to go

## Suggestions

A few suggestions to improve the project:

- The sentiment analyser doesn't recognise negated words. Sentences like "i am not happy" are incorrectly classified as positive. Use a POS-tagger to merge words with their negations before using them for training (e.g. that sentence would become "i am not_happy")
- Use fuzzy matching with an English dictionary to correct spelling mistakes and further reduce the feature space
- *Other suggestions are welcome!*

## Downloads

I uploaded some files from my project on the [releases page][13]:

- [happiness.csv][10] – a csv with `lat`, `lon` and `polarity` extracted from tweets
- [weather.csv][16] – a csv with `lat`, `lon`, `created_at`, `temperature (K)`, `pressure (Pa)` and `humidity (%)` extracted from tweets and their locations' forecast
- [model-sentiment-140.tar.gz][11] – my Sentiment140 sentiment analyser model
- [model-emo-140.tar.gz][12] – my emo sentiment analyser model
- [examples-sentiment-140.txt][14] – prediction examples done with the Sentiment140 analyser
- [examples-emo-140.txt][15] – prediction examples done with the emo analyser


[1]: http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0138717
[2]: http://help.sentiment140.com/for-students/
[3]: https://dev.twitter.com/streaming/overview
[4]: http://www.noaa.gov/
[5]: https://www.wikiwand.com/en/Artificial_neural_network
[6]: http://spark.apache.org/
[7]: http://www.scala-sbt.org/
[8]: https://hadoop.apache.org/
[9]: https://en.wikipedia.org/wiki/Multilayer_perceptron
[10]: https://github.com/Aluxian/Tweeather/releases/download/v1/happiness.csv
[11]: https://github.com/Aluxian/Tweeather/releases/download/v1/model-sentiment-140.tar.gz
[12]: https://github.com/Aluxian/Tweeather/releases/download/v1/model-emo-140.tar.gz
[13]: https://github.com/Aluxian/Tweeather/releases/latest
[14]: https://github.com/Aluxian/Tweeather/releases/download/v1/examples-sentiment-140.txt
[15]: https://github.com/Aluxian/Tweeather/releases/download/v1/examples-emo-140.txt
[16]: https://github.com/Aluxian/Tweeather/releases/download/v1/weather.csv
[17]: https://www.ncdc.noaa.gov/data-access/model-data/model-datasets/global-forcast-system-gfs
