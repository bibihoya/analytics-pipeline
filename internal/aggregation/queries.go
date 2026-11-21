package aggregation

const dailySQL = `WITH per_movie AS (
 SELECT movie_id, count() AS processed, uniqExactState(user_id) AS users,
 sumIf(progress_seconds, event_type = 'VIEW_FINISHED') AS watch_seconds,
 countIf(event_type = 'VIEW_STARTED') AS started,
 countIf(event_type = 'VIEW_FINISHED') AS finished
 FROM cinema.raw_events FINAL
 WHERE timestamp >= toDateTime({day:Date}, 'UTC')
 AND timestamp < toDateTime(addDays({day:Date}, 1), 'UTC')
 GROUP BY movie_id
), daily AS (
 SELECT sum(per_movie.processed) AS processed, uniqExactMerge(per_movie.users) AS dau,
 sum(per_movie.watch_seconds) / nullIf(sum(per_movie.finished), 0) AS average,
 sum(per_movie.started) AS started, sum(per_movie.finished) AS finished,
 arraySlice(arraySort(x -> (-toInt64(x.2), x.1),
 groupArrayIf((per_movie.movie_id, per_movie.started), per_movie.started > 0)), 1, 10) AS top
 FROM per_movie
)
SELECT processed, dau, average, started, finished,
arrayMap(x -> x.1, top) AS top_movie_ids,
arrayMap(x -> x.2, top) AS top_movie_views
FROM daily FORMAT JSONEachRow`

const cohortSQL = `WITH cohorts AS (
 SELECT user_id, min(toDate(timestamp, 'UTC')) AS first_activity,
 minIf(toDate(timestamp, 'UTC'), event_type = 'VIEW_STARTED') AS first_view,
 countIf(event_type = 'VIEW_STARTED') AS view_starts,
 groupUniqArray(toDate(timestamp, 'UTC')) AS days
 FROM cinema.raw_events FINAL GROUP BY user_id
)
SELECT countIf(view_starts > 0 AND first_view = {day:Date}) AS view_size,
countIf(view_starts > 0 AND first_view = {day:Date} AND has(days, addDays({day:Date}, 1))) AS d1,
countIf(view_starts > 0 AND first_view = {day:Date} AND has(days, addDays({day:Date}, 7))) AS d7,
countIf(first_activity = {day:Date}) AS size,
countIf(first_activity = {day:Date} AND has(days, addDays({day:Date}, 0))) AS day0,
countIf(first_activity = {day:Date} AND has(days, addDays({day:Date}, 1))) AS day1,
countIf(first_activity = {day:Date} AND has(days, addDays({day:Date}, 2))) AS day2,
countIf(first_activity = {day:Date} AND has(days, addDays({day:Date}, 3))) AS day3,
countIf(first_activity = {day:Date} AND has(days, addDays({day:Date}, 4))) AS day4,
countIf(first_activity = {day:Date} AND has(days, addDays({day:Date}, 5))) AS day5,
countIf(first_activity = {day:Date} AND has(days, addDays({day:Date}, 6))) AS day6,
countIf(first_activity = {day:Date} AND has(days, addDays({day:Date}, 7))) AS day7
FROM cohorts FORMAT JSONEachRow`
