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
        console.log(createReservationDocument(client,
            "leslie@example.com",
            "Infinite Views", [
            new Date("2021-12-31"), new Date("2022-01-01")],
            {pricePerNight: 180, specialRequests: "Late Checkout",
            breakfastIncluded: true}
            ));
    } finally {
        // Close the connection to the MongoDB cluster
        await client.close();
    }
}

main().catch(console.error);

// Add functions that make DB calls here

async function createReservation(client, userEmail, nameOfListing, reservationDates, reservationDetails){
    const usersCollection = client.db("sample_airbnb")
    .collection("users");

    const listingAndReviewsCollection = client.db("sample_airbnb")
    .collection("listingsAndReviews");    

    const reservation = createReservation(nameOfListing, reservationDates, reservationDetails);

    //begin transaction
    const session = client.startSession();

    const transactionOptions = {
        readPreference: 'primary',
        readConcern: {level: 'local'},
        writeConcern: {w: 'majority'}
    };

    try{
        const transactionResults = await session.withTransaction(async() => {
            const userUpdateResults = await usersCollection.updateOne({email: userEmail}, 
                {$addToSet: {reservations: reservation}},
                {session});
                console.log(`${userUpdateResults.matchedCount} document(s) found in the users collection with the email address ${userEmail}`);

                console.log(`${userUpdateResults.modifiedCount} document(s) was/were updated to include the reservation`);

                const isListingReservedResults = await listingAndReviewsCollection.findOne(
                    {name: nameOfListing,
                    datesReserved: {$in: reservationDates}},
                    {session}
                );
                if(isListingReservedResults){
                    await session.abortTransaction();
                    console.error('This listing is already reserved for at least one of the given dates. The reservation could not created');

                    console.error('Any operations that already ocurred as part of this transaction will be rolled back.');

                    return;
                }

                const listingAndReviewsUpdateResults = await listingAndReviewsCollection.updateOne(
                    {name: nameOfListing},
                    {$addToSet: {datesReserved: {$each: reservationDates}}},
                    {session}
                );

                console.log(`${listingAndReviewsUpdateResults.matchedCount} document(s) found in the listingAndReviewsCollection with the name ${nameOfListing}`);

                console.log(`${listingAndReviewsUpdateResults.modifiedCount} document(s) was/were updated to include the reservation dates.`);
                
                
                
        }, transactionOptions);

        if(transactionResults){
            console.log('The reservation was successfully created');
            
        }else{
            console.log('The transaction was intentionally aborted');
            
        }

    }catch(e){
        console.log('The transaction was aborted due to an unexpected error: ' + e);
        
    }finally{
        await session.endSession();
    }
}

function createReservationDocument(nameOfListing, reservationDates, reservationDetails){
    let reservation = {
        name: nameOfListing,
        dates: reservationDates
    }

    for(let detail in reservationDetails){
        reservation[detail] = reservationDetails[detail];
    }

    return reservation;
}
