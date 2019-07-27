// model figure1 using parent-referecing model
db.createCollection("categories")

db.categories.insert( { _id: "MongoDB", parent: "Databases" } )
db.categories.insert( { _id: "dbm", parent: "Databases" } )
db.categories.insert( { _id: "Databases", parent: "Programming" } )
db.categories.insert( { _id: "Languages", parent: "Programming" } )
db.categories.insert( { _id: "Programming", parent: "Books" } )
db.categories.insert( { _id: "Books", parent: null } )


// p2.1 height of tree (parent-referecing)
// breadth first search
var height = 1
var queue = []
// given root node
var item = db.categories.findOne({"_id": "Books"})
queue.push(item)
var nextLayerSize = queue.length

while(queue.length > 0) {

    var current = queue.shift();
    var children = db.categories.find( {parent: current._id});

    // level order traversal
    while (children.hasNext() == true) {
        var child = children.next();
        queue.push(child);

    }

    nextLayerSize -= 1
    // When traverse to last node of current layer
    // Set new nextLayerSize and update the height variable
    if(nextLayerSize == 0 && queue.length != 0){
        nextLayerSize = queue.length
        height += 1
     }

}
print(height)



// p2.2 ancesters of "DBM"
// Given dbm node
var item = db.categories.findOne({"_id": "dbm"})
var level = 1
var result = []
// iteratively go up and reference parent
while(item.parent) {
    var item = db.categories.findOne({"_id": item.parent})
    result.push({
        "Name": item._id,
        "Level": level
    })
    level += 1
}
result


// model figure1 using child-referecing model
db.categories.drop() // drop parent-referecing model
db.createCollection("categories")
db.categories.insert( { _id: "MongoDB", children: [] } )
db.categories.insert( { _id: "dbm", children: [] } )
db.categories.insert( { _id: "Databases", children: [ "MongoDB", "dbm" ] } )
db.categories.insert( { _id: "Languages", children: [] } )
db.categories.insert( { _id: "Programming", children: [ "Databases", "Languages" ] } )
db.categories.insert( { _id: "Books", children: [ "Programming" ] } )



// p2.3 direct parent of MongoDB
db.categories.find({"children": "MongoDB"})




// p2.4
// breadth first search
var queue = []
var result = []
// given root node
var item = db.categories.findOne({"_id": "Programming"})
queue.push(item)

while(queue.length > 0) {
    var current = queue.shift();

    for(var i = 0; i < current.children.length; i++){
        var child = db.categories.findOne({"_id": current.children[i]})
        queue.push(child);
        result.push(child._id);
    }
}
result


// p2.5 siblings of Languages
var item = db.categories.findOne({"children": "Languages"})
var siblings = []
for(var i = 0; i < item.children.length; i++){
    var childName = item.children[i]
    if(childName!="Languages"){
        siblings.push(db.categories.findOne({"_id": childName}))
    }
}
siblings
