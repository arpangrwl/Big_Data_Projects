// p1.1
var doc = db.getCollection("famous-people").findOne({$and: [{"name.first": "John"}, {"name.last": "McCarthy"}]})
doc._id = 100;
db.getCollection("famous-people").remove({$and: [{"name.first": "John"}, {"name.last": "McCarthy"}]})
db.getCollection("famous-people").insert(doc)
// db.getCollection("famous-people").find({$and: [{"name.first": "John"}, {"name.last": "McCarthy"}]})


// p1.2
db.getCollection("famous-people").insertMany([
{
"_id" : 20, "name" : {
"first" : "Mary",
"last" : "Sally" },
"birth" : ISODate("1933-08-27T04:00:00Z"), "death" : ISODate("1984-11-07T04:00:00Z"), "contribs" : [
"C++",
"Simula"
],
"awards" : [
{
"award" : "WPI Award", "year" : 1999,
"by" : "WPI"
} ]
},

{
"_id" : 30, "name" : {
"first" : "Ming",
"last" : "Zhang" },
"birth" : ISODate("1911-04-12T04:00:00Z"), "death" : ISODate("2000-11-07T04:00:00Z"), "contribs" : [
"C++", "FP", "Python",
],
"awards" : [
{
"award" : "WPI Award", "year" : 1960,
"by" : "WPI"
}, {
} ]
}
])

// p1.3
db.getCollection("famous-people").find({$and:[
    {"awards.award": "Turing Award"},
    {"awards.year": {"$gt": 1960}}
    ]})


// p1.4
db.getCollection("famous-people")
  .find({ "awards.2":  {$exists: true}}, {_id:0, name: 1})


// p1.5
db.getCollection("famous-people").update(
    {$and: [{"name.first": "Guido"}, {"name.last": "van Rossum"}]},
    {$addToSet: { "contribs": "Python"}}
)

// p1.6
db.getCollection("famous-people").update(
{$and: [{"name.first": "Mary"}, {"name.last": "Sally"}]},
{$set: {"comments": ["taught in 3 universities","was an amazing pioneer","lived in Worcester."]}}
)

// p1.7
db.createCollection("answer7")

var contribsMary  = db.getCollection('famous-people')
  .findOne({$and: [{"name.first": "Mary"}, {"name.last": "Sally"}]}, {_id: 0, contribs:1})
  .contribs

for(var i in contribsMary){
    var doc = {}
    doc['Contribution'] = contribsMary[i]
    doc['People'] = []
    db.getCollection('answer7').insertOne(doc)
}

var docs = db.getCollection('famous-people').find()
docs.forEach(function (x) {
    for(var i in contribsMary){
        if (x.contribs.includes(contribsMary[i])){
            db.getCollection('answer7').update(
                {Contribution: contribsMary[i]},
                {$push:{People: x.name}}
            )
            }
    }
})

db.getCollection("answer7").find({},{_id:0})

// p1.8
db.getCollection("famous-people").find({"name.first": { $regex: "Jo*" }}).sort( { "name.last": 1 } )

// p1.9
db.getCollection('famous-people').update(
    {_id: 30, "awards.by": "WPI"},
    {$set: {"awards.$.year": 1965}}
)

// p1.10
var contribs3
db.getCollection('famous-people').find({_id: 3}).forEach(function(u) { contribs3 = u.contribs})

db.getCollection("famous-people").update(
    {_id: 30},
    {$push: {contribs:{$each: contribs3}}}
)
