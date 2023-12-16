// Databricks notebook source
// MAGIC %md
// MAGIC # Data-Intensive Programming - Assignment
// MAGIC
// MAGIC This is the **Scala** version of the assignment. Switch to the Python version, if you want to do the assignment in Python.
// MAGIC
// MAGIC In all tasks, add your solutions to the cells following the task instructions. You are free to add new cells if you want.
// MAGIC
// MAGIC Don't forget to **submit your solutions to Moodle** once your group is finished with the assignment.
// MAGIC
// MAGIC ## Basic tasks (compulsory)
// MAGIC
// MAGIC There are in total seven basic tasks that every group must implement in order to have an accepted assignment.
// MAGIC
// MAGIC The basic task 1 is a warming up task and it deals with some video game sales data. The task asks you to do some basic aggregation operations with Spark data frames.
// MAGIC
// MAGIC The other basic tasks (basic tasks 2-7) are all related and deal with data from [https://moneypuck.com/data.htm](https://moneypuck.com/data.htm) that contains information about every shot in all National Hockey League ([NHL](https://en.wikipedia.org/wiki/National_Hockey_League), [ice hockey](https://en.wikipedia.org/wiki/Ice_hockey)) matches starting from season 2011-12 and ending with the last completed season, 2022-23. The tasks ask you to calculate the results of the matches based on the given data as well as do some further calculations. Knowledge about ice hockey or NHL is not required, and the task instructions should be sufficient in order to gain enough context for the tasks.
// MAGIC
// MAGIC ## Additional tasks (optional, can provide course points)
// MAGIC
// MAGIC There are in total of three additional tasks that can be done to gain some course points.
// MAGIC
// MAGIC The first additional task asks you to do all the basic tasks in an optimized way. It is possible that you can some points from this without directly trying by just implementing the basic tasks in an efficient manner.
// MAGIC
// MAGIC The other two additional tasks are separate tasks and do not relate to any other basic or additional tasks. One of them asks you to load in unstructured text data and do some calculations based on the words found from the data. The other asks you to utilize the K-Means algorithm to partition the given building data.
// MAGIC
// MAGIC It is possible to gain partial points from the additional tasks. I.e., if you have not completed the task fully but have implemented some part of the task, you might gain some appropriate portion of the points from the task.
// MAGIC

// COMMAND ----------

// import statements for the entire notebook
// add anything that is required here

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window




// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 1 - Sales data
// MAGIC
// MAGIC The CSV file `assignment/sales/video_game_sales.csv` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains video game sales data (from [https://www.kaggle.com/datasets/ashaheedq/video-games-sales-2019/data](https://www.kaggle.com/datasets/ashaheedq/video-games-sales-2019/data)). The direct address for the dataset is: `abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/sales/video_game_sales.csv`
// MAGIC
// MAGIC Load the data from the CSV file into a data frame. The column headers and the first few data lines should give sufficient information about the source dataset.
// MAGIC
// MAGIC Only data for sales in the first ten years of the 21st century should be considered in this task, i.e. years 2000-2009.
// MAGIC
// MAGIC Using the data, find answers to the following:
// MAGIC
// MAGIC - Which publisher had the highest total sales in video games in European Union in years 2000-2009?
// MAGIC - What were the total yearly sales, in European Union and globally, for this publisher in year 2000-2009
// MAGIC

// COMMAND ----------

val salesDataFrame: DataFrame = spark.read
                                .format("csv")
                                .option("header","true")
                                .option("inferSchema", "true")
                                .load("abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/sales/video_game_sales.csv")

//salesDataFrame.show(10)
//salesDataFrame.printSchema()



// Filter data for the years 2000-2009
val filteredDF = salesDataFrame
  .filter(col("Year").between(2000, 2009))
  .filter(col("EU_Sales").isNotNull)


val totalSalesByPublisher = filteredDF
                            .groupBy("Publisher")
                            .agg(sum("EU_Sales").alias("Total_EU_Sales"))
                            .sort(desc("Total_EU_Sales"))



val bestEUPublisher: String = totalSalesByPublisher.first().getString(0)

val bestEUPublisherSales: DataFrame = filteredDF
                                      .filter(col("Publisher") === bestEUPublisher)
                                      .groupBy("Year")
                                      .agg(
                                            round(sum("EU_Sales"),2).alias("EU_Total"), 
                                            round(sum("Global_Sales"),2).alias("Global_Total")
                                         )
                                      .sort("Year")


println(s"The publisher with the highest total video game sales in European Union is: '${bestEUPublisher}'")
println("Sales data for the publisher:")
bestEUPublisherSales.show(10)



// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 2 - Shot data from NHL matches
// MAGIC
// MAGIC A parquet file in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) at folder `assignment/nhl_shots.parquet` from [https://moneypuck.com/data.htm](https://moneypuck.com/data.htm) contains information about every shot in all National Hockey League ([NHL](https://en.wikipedia.org/wiki/National_Hockey_League), [ice hockey](https://en.wikipedia.org/wiki/Ice_hockey)) matches starting from season 2011-12 and ending with the last completed season, 2022-23.
// MAGIC
// MAGIC In this task you should load the data with all of the rows into a data frame. This data frame object will then be used in the following basic tasks 3-7.
// MAGIC
// MAGIC ### Background information
// MAGIC
// MAGIC Each NHL season is divided into regular season and playoff season. In the regular season the teams play up to 82 games with the best teams continuing to the playoff season. During the playoff season the remaining teams are paired and each pair play best-of-seven series of games to determine which team will advance to the next phase.
// MAGIC
// MAGIC In ice hockey each game has a home team and an away team. The regular length of a game is three 20 minute periods, i.e. 60 minutes or 3600 seconds. The team that scores more goals in the regulation time is the winner of the game.
// MAGIC
// MAGIC If the scoreline is even after this regulation time:
// MAGIC
// MAGIC - In playoff games, the game will be continued until one of the teams score a goal with the scoring team being the winner.
// MAGIC - In regular season games, there is an extra time that can last a maximum of 5 minutes (300 seconds). If one of the teams score, the game ends with the scoring team being the winner. If there is no goals in the extra time, there would be a shootout competition to determine the winner. These shootout competitions are not considered in this assignment, and the shots from those are not included in the raw data.
// MAGIC
// MAGIC **Columns in the data**
// MAGIC
// MAGIC Each row in the given data represents one shot in a game.
// MAGIC
// MAGIC The column description from the source website. Not all of these will be needed in this assignment.
// MAGIC
// MAGIC | column name | column type | description |
// MAGIC | ----------- | ----------- | ----------- |
// MAGIC | shotID      | integer | Unique id for each shot |
// MAGIC | homeTeamCode | string | The home team in the game. For example: TOR, MTL, NYR, etc. |
// MAGIC | awayTeamCode | string | The away team in the game |
// MAGIC | season | integer | Season the shot took place in. Example: 2009 for the 2009-2010 season |
// MAGIC | isPlayOffGame | integer | Set to 1 if a playoff game, otherwise 0 |
// MAGIC | game_id | integer | The NHL Game_id of the game the shot took place in |
// MAGIC | time | integer | Seconds into the game of the shot |
// MAGIC | period | integer | Period of the game |
// MAGIC | team | string | The team taking the shot. HOME or AWAY |
// MAGIC | location | string | The zone the shot took place in. HOMEZONE, AWAYZONE, or Neu. Zone |
// MAGIC | event | string | Whether the shot was a shot on goal (SHOT), goal, (GOAL), or missed the net (MISS) |
// MAGIC | homeTeamGoals | integer | Home team goals before the shot took place |
// MAGIC | awayTeamGoals | integer | Away team goals before the shot took place |
// MAGIC | homeTeamWon | integer | Set to 1 if the home team won the game. Otherwise 0. |
// MAGIC | shotType | string | Type of the shot. (Slap, Wrist, etc) |
// MAGIC

// COMMAND ----------

val shotsDF: DataFrame = spark.read.parquet("abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/nhl_shots.parquet")

println("Number of rows:", shotsDF.count())





// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 3 - Game data frame
// MAGIC
// MAGIC Create a match data frame for all the game included in the shots data frame created in basic task 2.
// MAGIC
// MAGIC The output should contain one row for each game.
// MAGIC
// MAGIC The following columns should be included in the final data frame for this task:
// MAGIC
// MAGIC | column name    | column type | description |
// MAGIC | -------------- | ----------- | ----------- |
// MAGIC | season         | integer     | Season the game took place in. Example: 2009 for the 2009-2010 season |
// MAGIC | game_id        | integer     | The NHL Game_id of the game |
// MAGIC | homeTeamCode   | string      | The home team in the game. For example: TOR, MTL, NYR, etc. |
// MAGIC | awayTeamCode   | string      | The away team in the game |
// MAGIC | isPlayOffGame  | integer     | Set to 1 if a playoff game, otherwise 0 |
// MAGIC | homeTeamGoals  | integer     | Number of goals scored by the home team |
// MAGIC | awayTeamGoals  | integer     | Number of goals scored by the away team |
// MAGIC | lastGoalTime   | integer     | The time in seconds for the last goal in the game. 0 if there was no goals in the game. |
// MAGIC
// MAGIC All games had at least some shots but there are some games that did not have any goals either in the regulation 60 minutes or in the extra time.
// MAGIC
// MAGIC Note, that for a couple of games there might be some shots, including goal-scoring ones, that are missing from the original dataset. For example, there might be a game with a final scoreline of 3-4 but only 6 of the goal-scoring shots are included in the dataset. Your solution does not have to try to take these rare occasions of missing data into account. I.e., you can do all the tasks with the assumption that there are no missing or invalid data.
// MAGIC

// COMMAND ----------



val gamesDF: DataFrame = shotsDF
                        .groupBy("season", "game_id", "homeTeamCode", "awayTeamCode", "isPlayOffGame")
                        .agg(
                              sum(when(col("event") === "GOAL" && col("team") === "HOME", 1).otherwise(0)).alias("homeGoals"),

                              sum(when(col("event") === "GOAL" && col("team") === "AWAY", 1).otherwise(0)).alias("awayGoals"),

                              max(when(col("event") === "GOAL", coalesce(col("time"), lit(0)))).alias("lastGoalTime")



                            )
                        .orderBy("season", "game_id")
                        .select("season", "game_id", "homeTeamCode", "awayTeamCode", "isPlayOffGame", "homeGoals", "awayGoals", "lastGoalTime")


  gamesDF.na.fill(0, Array("lastGoalTime"))

println("Number of rows",gamesDF.count())

gamesDF.show()

gamesDF.filter(col("game_id") === 20425).show()



// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 4 - Game wins during playoff seasons
// MAGIC
// MAGIC Create a data frame that uses the game data frame from the basic task 3 and contains aggregated number of wins and losses for each team and for each playoff season, i.e. for games which have been marked as playoff games. Only teams that have played in at least one playoff game in the considered season should be included in the final data frame.
// MAGIC
// MAGIC The following columns should be included in the final data frame:
// MAGIC
// MAGIC | column name    | column type | description |
// MAGIC | -------------- | ----------- | ----------- |
// MAGIC | season         | integer     | The season for the data. Example: 2009 for the 2009-2010 season |
// MAGIC | teamCode       | string      | The code for the team. For example: TOR, MTL, NYR, etc. |
// MAGIC | games          | integer     | Number of playoff games the team played in the given season |
// MAGIC | wins           | integer     | Number of wins in playoff games the team had in the given season |
// MAGIC | losses         | integer     | Number of losses in playoff games the team had in the given season |
// MAGIC
// MAGIC Playoff games where a team scored more goals than their opponent are considered winning games. And playoff games where a team scored less goals than the opponent are considered losing games.
// MAGIC
// MAGIC In real life there should not be any playoff games where the final score line was even but due to some missing shot data you might end up with a couple of playoff games that seems to have ended up in a draw. These possible "drawn" playoff games can be left out from the win/loss calculations.
// MAGIC

// COMMAND ----------

val filteredData:DataFrame = gamesDF.filter(col("isPlayOffGame") === 1)




val playoffGamesDF: DataFrame = filteredData
                              .selectExpr("season", "homeTeamCode as teamCode", "homeGoals", "awayGoals")
                              .union(
                              filteredData.selectExpr("season", "awayTeamCode as teamCode", "awayGoals", "homeGoals")
                                    )
                              .groupBy("season", "teamCode")
                              .agg(
                                    count("teamCode").alias("games"),
                                    sum(when(col("homeGoals") > col("awayGoals"), 1).otherwise(0)).alias("wins"),
                                    sum(when(col("homeGoals") <= col("awayGoals"), 1).otherwise(0)).alias("losses")
                                    
                                  )
                              .orderBy("season")


println("Row count", playoffGamesDF.count())
playoffGamesDF.show(50)


// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 5 - Best playoff teams
// MAGIC
// MAGIC Using the playoff data frame created in basic task 4 create a data frame containing the win-loss record for best playoff team, i.e. the team with the most wins, for each season. You can assume that there are no ties for the highest amount of wins in each season.
// MAGIC
// MAGIC The following columns should be included in the final data frame:
// MAGIC
// MAGIC | column name    | column type | description |
// MAGIC | -------------- | ----------- | ----------- |
// MAGIC | season         | integer     | The season for the data. Example: 2009 for the 2009-2010 season |
// MAGIC | teamCode       | string      | The team code for the best performing playoff team in the given season. For example: TOR, MTL, NYR, etc. |
// MAGIC | games          | integer     | Number of playoff games the best performing playoff team played in the given season |
// MAGIC | wins           | integer     | Number of wins in playoff games the best performing playoff team had in the given season |
// MAGIC | losses         | integer     | Number of losses in playoff games the best performing playoff team had in the given season |
// MAGIC
// MAGIC Finally, fetch the details for the best playoff team in season 2022.
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.expressions.Window

val partition =  Window.partitionBy("season").orderBy(col("wins").desc)

val bestPlayoffTeams: DataFrame = playoffGamesDF
                                  .withColumn("rank", row_number().over(partition))
                                  .filter(col("rank") === 1)
                                  .drop("rank")

bestPlayoffTeams.show()


// COMMAND ----------

val bestPlayoffTeam2022: Row = bestPlayoffTeams.filter(col("season")=== 2022).head()

println("Best playoff team in 2022:")
println(s"    Team: ${bestPlayoffTeam2022.getAs[String]("teamCode")}")
println(s"    Games: ${bestPlayoffTeam2022.getAs[Long]("games")}")
println(s"    Wins: ${bestPlayoffTeam2022.getAs[Long]("wins")}")
println(s"    Losses: ${bestPlayoffTeam2022.getAs[Long]("losses")}")
println("=========================================================")


// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 6 - Regular season points
// MAGIC
// MAGIC Create a data frame that uses the game data frame from the basic task 3 and contains aggregated data for each team and for each season for the regular season matches, i.e. the non-playoff matches.
// MAGIC
// MAGIC The following columns should be included in the final data frame:
// MAGIC
// MAGIC | column name    | column type | description |
// MAGIC | -------------- | ----------- | ----------- |
// MAGIC | season         | integer     | The season for the data. Example: 2009 for the 2009-2010 season |
// MAGIC | teamCode       | string      | The code for the team. For example: TOR, MTL, NYR, etc. |
// MAGIC | games          | integer     | Number of non-playoff games the team played in the given season |
// MAGIC | wins           | integer     | Number of wins in non-playoff games the team had in the given season |
// MAGIC | losses         | integer     | Number of losses in non-playoff games the team had in the given season |
// MAGIC | goalsScored    | integer     | Total number goals scored by the team in non-playoff games in the given season |
// MAGIC | goalsConceded  | integer     | Total number goals scored against the team in non-playoff games in the given season |
// MAGIC | points         | integer     | Total number of points gathered by the team in non-playoff games in the given season |
// MAGIC
// MAGIC Points from each match are received as follows (in the context of this assignment, these do not exactly match the NHL rules):
// MAGIC
// MAGIC | points | situation |
// MAGIC | ------ | --------- |
// MAGIC | 3      | team scored more goals than the opponent during the regular 60 minutes |
// MAGIC | 2      | the score line was even after 60 minutes but the team scored a winning goal during the extra time |
// MAGIC | 1      | the score line was even after 60 minutes but the opponent scored a winning goal during the extra time or there were no goals in the extra time |
// MAGIC | 0      | the opponent scored more goals than the team during the regular 60 minutes |
// MAGIC
// MAGIC In the regular season the following table shows how wins and losses should be considered (in the context of this assignment):
// MAGIC
// MAGIC | win | loss | situation |
// MAGIC | --- | ---- | --------- |
// MAGIC | Yes | No   | team gained at least 2 points from the match |
// MAGIC | No  | Yes  | team gain at most 1 point from the match |
// MAGIC

// COMMAND ----------


val filteredDF = gamesDF.filter(col("isPlayOffGame") === 0)

val pointsDF = filteredDF.select(
  "season", "homeTeamCode", "awayTeamCode", "homeGoals", "awayGoals", "lastGoalTime"
)
.withColumn("teamCode", expr("CASE WHEN homeGoals > awayGoals THEN homeTeamCode ELSE awayTeamCode END"))
.withColumn("home_points", 
  when(col("homeGoals") > col("awayGoals") && col("lastGoalTime") <= 3600, 3)
  .when(col("homeGoals") > col("awayGoals") && col("lastGoalTime") > 3600, 2)
  .when(col("homeGoals") < col("awayGoals") && col("lastGoalTime") > 3600, 1)
  .when(col("homeGoals") === col("awayGoals") && col("lastGoalTime") <= 3600, 1)
  .when(col("homeGoals") === col("awayGoals") && col("lastGoalTime").isNull, 1)

  .otherwise(0)
)
.withColumn("away_points", 
  when(col("awayGoals") > col("homeGoals") && col("lastGoalTime") <= 3600, 3)
  .when(col("awayGoals") > col("homeGoals") && col("lastGoalTime") > 3600, 2)
  .when(col("awayGoals") < col("homeGoals") && col("lastGoalTime") > 3600, 1)
  .when(col("homeGoals") === col("awayGoals") && col("lastGoalTime") <= 3600, 1)
  .otherwise(0)
)

/*
pointsDF.filter(
  (col("homeTeamCode") === "CBJ" || col("awayTeamCode") === "CBJ") &&
  col("season") === 2021 &&
  (col("home_points") =!= 3 && col("away_points") =!= 3)
).show(500)
*/

  

// Calculate total games played, goals scored, goals conceded and total points by each team

  val gamesPlayedByTeamDF = pointsDF
  .select("season", "homeTeamCode", "home_points" , "homeGoals" , "awayGoals")
  .union(pointsDF.select("season", "awayTeamCode", "away_points" , "awayGoals", "homeGoals"))
  .groupBy("season", "homeTeamCode")
  .agg(

    count("*").alias("gamesPlayed"),
    sum("homeGoals").alias("goalsScored"),
    sum("awayGoals").alias("goalsConceded"),
    sum("home_points").alias("points")
  )
  .orderBy("season")

 // println(gamesPlayedByTeamDF.count())
  

// Calculate number of games won by each team
val gamesWonDF = pointsDF
  .filter(col("teamCode") === col("homeTeamCode") && col("homeGoals") > col("awayGoals") ||
          col("teamCode") === col("awayTeamCode") && col("awayGoals") > col("homeGoals"))
  .groupBy("season", "teamCode")
  .agg(count("teamCode").alias("gamesWon"))
  
//gamesWonDF.show(368)
  //println(gamesWonDF.count())

// Calculate number of games lost by each team
val totalLossesDF = gamesPlayedByTeamDF.as("a")
  .join(gamesWonDF.as("b"), col("a.homeTeamCode") === col("b.teamCode") && col("a.season") === col("b.season"), "left_outer")
  .withColumn("totalLosses", col("gamesPlayed") - coalesce(col("gamesWon"), lit(0)))
  .select("a.season", "teamCode", "totalLosses")

  //println(totalLossesDF.count())
  //totalLossesDF.show(368) 



val regularSeasonDF: DataFrame = gamesPlayedByTeamDF.as("A")
  .join(gamesWonDF.as("B"), col("A.homeTeamCode") === col("B.teamCode") && col("A.season") === col("B.season"))
  .join(totalLossesDF.as("C"), col("A.homeTeamCode") === col("C.teamCode") && col("A.season") === col("C.season") )
  .select(
    col("A.season"),
    col("B.teamCode"),
    col("A.gamesPlayed").alias("games"),
    col("B.gamesWon").alias("wins"),
    col("C.totalLosses").alias("losses"),
    col("A.goalsScored"),
    col("A.goalsConceded"),
    col("A.points")
  )
  .orderBy("A.season")


println("Total Rows",regularSeasonDF.count())

regularSeasonDF.show()



// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 7 - The worst regular season teams
// MAGIC
// MAGIC Using the regular season data frame created in the basic task 6, create a data frame containing the regular season records for the worst regular season team, i.e. the team with the least amount of points, for each season. You can assume that there are no ties for the lowest amount of points in each season.
// MAGIC
// MAGIC Finally, fetch the details for the worst regular season team in season 2022.
// MAGIC

// COMMAND ----------

val windowSpec = Window.partitionBy("season").orderBy("points")

val worstRegularTeams: DataFrame = regularSeasonDF
  .withColumn("rank", rank().over(windowSpec))
  .filter(col("rank") === 1)
  .drop("rank")


worstRegularTeams.show()


// COMMAND ----------

val worstRegularTeam2022: Row = worstRegularTeams
                                .filter(col("season") === 2022)
                                .orderBy("season")  
                                .first()

println("Worst regular season team in 2022:")
println(s"    Team: ${worstRegularTeam2022.getAs[String]("teamCode")}")
println(s"    Games: ${worstRegularTeam2022.getAs[Long]("games")}")
println(s"    Wins: ${worstRegularTeam2022.getAs[Long]("wins")}")
println(s"    Losses: ${worstRegularTeam2022.getAs[Long]("losses")}")
println(s"    Goals scored: ${worstRegularTeam2022.getAs[Long]("goalsScored")}")
println(s"    Goals conceded: ${worstRegularTeam2022.getAs[Long]("goalsConceded")}")
println(s"    Points: ${worstRegularTeam2022.getAs[Long]("points")}")

