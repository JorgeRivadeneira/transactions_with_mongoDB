const { MongoClient } = require('mongodb');
const strConn = require('./strConnection');

async function main() {
    /**
     * Connection URI. Update <username>, <password>, and <your-cluster-url> to reflect your cluster.
     * See https://docs.mongodb.com/drivers/node/ for more details
     */
    const uri = strConn;
    const client = new MongoClient(uri, { 
        useNewUrlParser: true,
        useUnifiedTopology: true
     });

    try {
        // Connect to the MongoDB cluster
        await client.connect();

        // Make the appropriate DB calls

        await printCheapestSuburbs(client, "Australia", "Sydney", 10);

    } finally {
        // Close the connection to the MongoDB cluster
        await client.close();
    }
}

main().catch(console.error);

// Add functions that make DB calls here

async function printCheapestSuburbs(client, country, market, maxNumberToPrint){
    const papeline = [
        {
          '$match': {
            'bedrooms': 1, 
            'address.country': country, 
            'address.market': market, 
            'address.suburb': {
              '$exists': 1, 
              '$ne': ''
            }, 
            'room_type': 'Entire home/apt'
          }
        }, {
          '$group': {
            '_id': '$address.suburb', 
            'averagePrice': {
              '$avg': '$price'
            }
          }
        }, {
          '$limit': maxNumberToPrint
        }
      ];
      const aggCursor = await client.db("sample_airbnb")
        .collection("listingsAndReviews")
        .aggregate(papeline);

      await aggCursor.forEach(airbnbListing => {
        console.log(`${airbnbListing._id} : ${airbnbListing.averagePrice}`);
        
      });
}