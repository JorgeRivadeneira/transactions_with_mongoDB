const {MongoClient} = require('mongodb');
const strConn = require('./strConnection');

async function main(){
    const uri = strConn;
    const client = new MongoClient(uri, { 
        useNewUrlParser: true,
        useUnifiedTopology: true
     });
    
    try{
        console.log("Start connection...");
        await client.connect();

        // await listDatabases(client);

        // await createListing(client, {
        //     name: 'Hotel Ibarrita',
        //     summary: 'Un lugar bello en Ibarra',
        //     bedrooms: 1,
        //     bathrooms: 1
        // });
        
        /*await createMultipleListings(client, [
            {
                name: 'Hotel Quito',
                summary: 'Hotel viejo',
                bedrooms: 1,
                bathrooms: 1
            },
            {
                name: 'Hotel Guayaquil',
                summary: 'Hotel viejo en GYE',
                bedrooms: 1,
                bathrooms: 1
            },   
            {
                name: 'Hotel Oro Verde',
                summary: 'Hotel en Manta',
                bedrooms: 1,
                bathrooms: 1
            }, 
            {
                name: 'Hotel Hillton Colon',
                summary: 'Hotel lujoso en Quito',
                bedrooms: 1,
                bathrooms: 1
            },                                
        ])*/

        //await findOneListingByName(client, "Hotel Quito");

        // await findListingsWithMinimumBedroomsBathroomsAndMostRecentReviews(client, {
        //     minimumNumberOfBathrooms: 2,
        //     minimumNumberOfBedrooms: 4,
        //     maximumNumberOfResults: 5
        // });

        /*await updateListingByName(client, "Hotel Ibarrita", {
            bathrooms: 6,
            bedrooms: 8
        });*/

        /*await upsertListingByName(client, "Hotel Transilvania", {
            name: "Hotel Transilvania",
            bedrooms: 1,
            bathrooms: 1
        });*/

        //await updateListingByNameToHavePropertyType(client);

        //await deleteListingByName(client, "Hotel Ibarrita");

        await deleteListingsScrapedBeforeDate(client, new Date("2019-02-15"));
    }catch(e){
        console.error(e);
    }finally{
        await client.close();
        console.log("...Connection Closed");
    }
}

main().catch(console.error);

async function deleteListingsScrapedBeforeDate(client, date){
    const result = await client.db("sample_airbnb")
        .collection("listingsAndReviews")
        .deleteMany({"last_scraped" : {$lt: date}});
    console.log(`${result.deletedCount} document(s) was/were deleted`);
    
}

async function deleteListingByName(client, nameOfListing){
    const result = await client.db("sample_airbnb")
                        .collection("listingsAndReviews")
                        .deleteOne({name: nameOfListing});

    console.log(`${result.deletedCount} document(s) was/were deleted`);
    
}

async function updateListingByNameToHavePropertyType(client){
    const result = await client.db("sample_airbnb")
        .collection("listingsAndReviews")
        .updateMany({
            property_type: {$exists: false}},
            {$set: {property_type: "Desconocida"}});
    console.log(`${result.matchedCount} document(s) matched the query criteria`);
    console.log(`${result.modifiedCount} document(s) was/were updated`);
    
}

async function upsertListingByName(client, nameOfListing, updatedListing){
    const result = await client.db("sample_airbnb")
        .collection("listingsAndReviews")
        .updateOne(
            {name: nameOfListing},
            {$set: updatedListing},
            {upsert: true});

    console.log(`${result.matchedCount} document(s) matched the query crieria`);
    if(result.upsertedCount > 0){
        console.log(`One document was inserted with the id: ${result.upsertedId}`);
        
    }else{
        console.log(`${result.modifiedCount} documents was/were updated`);
    }
}

async function updateListingByName(client, nameOfListing, updatedListing){
    const result = await client.db("sample_airbnb")
        .collection("listingsAndReviews")
        .updateOne(
            {name: nameOfListing},
            {$set: updatedListing});

    console.log(`${result.matchedCount} document(s) matched the query crieria`);
    console.log(`${result.modifiedCount} documents was/were updated`);
}

async function findListingsWithMinimumBedroomsBathroomsAndMostRecentReviews(client, {
    minimumNumberOfBedrooms = 0,
    minimumNumberOfBathrooms = 0,
    maximumNumberOfResults = Number.MAX_SAFE_INTEGER
} = {} )
{
    const cursor = client.db("sample_airbnb")
        .collection("listingsAndReviews")
        .find({
            bedrooms: {$gte: minimumNumberOfBedrooms},
            bathrooms: {$gte: minimumNumberOfBathrooms}
        }).sort({last_review: -1}).limit(maximumNumberOfResults);
    const results = await cursor.toArray();
    console.log(results);
    
}

async function findOneListingByName(client, nameOfListing){
    const result = await client.db("sample_airbnb")
        .collection("listingsAndReviews")
        .findOne({name: nameOfListing});
    if(result){
        console.log(`Found a listing with the name: ${nameOfListing}`);
        console.log(result);
        
    }else{
        console.log(`No listings found with the name: ${nameOfListing}`);
        
    }
}

async function createMultipleListings(client, newListings){
    const result = await client.db("sample_airbnb")
        .collection("listingsAndReviews")
        .insertMany(newListings);
    console.log(`${result.insertedCount} new listings created with the following id(s):`);
    console.log(result.insertedIds);
    
    
}

async function createListing(client, newListing){
    const result = await client.db("sample_airbnb")
        .collection("listingsAndReviews")
        .insertOne(newListing);

    console.log(`New listing created with the following id: ${result.insertedId}`);
    
}

async function listDatabases(client){
    const databasesList = await client.db().admin().listDatabases();
    console.log("Databases:");
    databasesList.databases.forEach(db => {
        console.log(`-${db.name}`);
        
    });
    
}
