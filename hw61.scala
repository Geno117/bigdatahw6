val textFile = sc.textFile("hdfs:///tmp/gutenberg/books.txt") val words = textFile.flatMap(line => line.split(" "))
val pairs = words.map(word => (word, 1))
val counts = pairs.reduceByKey(_ + _) counts.saveAsTextFile("hdfs:///tmp/spertus/spark_wc")