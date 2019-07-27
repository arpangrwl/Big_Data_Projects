// p1.1
db.getCollection("famous-people").mapReduce(
    // Map
    function() {
        for(var i in this.awards){
            emit(this.awards[i].award, 1)
        }
    },
    // Reduce
    function(key, values){
        return Array.sum(values)
    },
    // Query
    {out: "awardCount"}
)
db.awardCount.find()



// p1.2
db.getCollection("famous-people")
  .aggregate([
    // stage 1
    {$unwind: "$awards"},
    // stage 2
    {$group: {_id: "$awards.award", yearArray:  {"$addToSet" : "$awards.year"}}}
  ])



// p1.3
db.getCollection("famous-people")
  .aggregate([
    // stage 1
    {$group: {_id: {$year: "$birth"}, count: { $sum: 1 }, idArray: {"$addToSet" : "$_id"}}},
    // stage 2
    {$project:{_id: 0, birthYear: "$_id", count: 1, idArray:1}},
    // stage 3
    {$sort: {birthYear: 1}}
  ])



// p1.4
var minmaxID = []
db.getCollection("famous-people")
  .aggregate([
    {$group: {_id: 1,minID: {$min: "$_id"},maxID: {$max: "$_id"}}}
  ]).map(function(row) {
     minmaxID.push(row.minID),
     minmaxID.push(row.maxID)
  })
db.getCollection("famous-people").find(
    {_id: {$in: minmaxID}}
)



// p1.5
// create text index
db.getCollection("famous-people").ensureIndex({"awards.award":"text"})

// search 2 string
db.getCollection("famous-people")
  .find( { $text:{ $search: "Turing"}} )

// drop index
db.getCollection("famous-people").dropIndex("awards.award_text")



// p1.6 (either or)
// create text index
db.getCollection("famous-people").ensureIndex({"awards.award":"text"})

// search 2 string
db.getCollection("famous-people")
  .find( { $text: { $search: "Turing, National Medal"} } )

// drop index
db.getCollection("famous-people").dropIndex("awards.award_text")



// p1.6 (both)
// create text index
db.getCollection("famous-people").ensureIndex({"awards.award":"text"})

// search 2 string
db.getCollection("famous-people")
  .find( { $text: { $search: "\"Turing\" \"National Medal\""} } )

// drop index
db.getCollection("famous-people").dropIndex("awards.award_text")
