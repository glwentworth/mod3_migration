/**
 * 
 * 
 */

 /** stringify  for printing objects
  * 
  */
const stringify = require('stringify-object');
const fs = require('fs');
/** mongo db driver
 * 
 */
const mongodb = require('mongodb');

/** stream json parser
 *  breaks the file data into single objects
 */
const parser = require('dummy-streaming-array-parser');

/** code to maintain a queue of objects
 * 
 */
const Queue = require('queue-fifo');

/* data files with name info and address info
*/
const custfilename = 'm3-customer-data.json';
const adrfilename = 'm3-customer-address-data.json';

// 
var count = 1;

/* create a parser for each of the data files
 */
cp = parser()
ap = parser();

/* a queue for the data drom the files the name data and
   name data
*/
var adrq = new Queue();
var custq = new Queue();

/* control data for processing the incoming data
*/
var done = 0;
var ToDoCnt = 60;

//get the count to send to the data base each time from the command line...
var x = Number(process.argv[2]);
if (isNaN(x)) {
    console.log('Improper parameter....  ',process.argv[2]);
    return process.exit(1);;
}
ToDoCnt = x;

// Use connect method to connect to the Server
const url = 'mongodb://localhost:27017';  // Connection URI
const dbname = 'edx-course-db';  // database to use
const collectioname = 'bitcoin-owners';  // collection for this data
var database;  // will contain the database info
var written  = 0;  //count total objects written

/* get a client object for database use
*/
const MongoClient = mongodb.MongoClient;

MongoClient.connect(url, (err, client) => {
    if (err) {
        console.log("connection error"+err);
        return process.exit(2);
    } else {
        console.log('Connected successfully to server');
        database = client.db(dbname);
        // remove the existing data...
        database.collection(collectioname).deleteMany({});
        // process the new data
        getDataObjects();  
        //console.log('set timer for process q...');
        setTimeout(processqueued);
    }}
)

/** db continue
 *  callback for the insert determines if there is more todo and resets
 *  the timed call to process data
 */
function dbcontinue(result) {
    if (adrq.size() > 0) {
        // we have more to do...
        setTimeout(processqueued);
    } else {
        process.exit(0);
    }
}

/** insertDocument 
 *  function that inserts the data intothe database.
 *  database: the client object from connectiong to the data base
 *  collectioname:  the name of the collection to insert into
 *  documents: array of objects to write to the db
 *  callback: code that determines the continuation of the process
 */
function insertDocuments(database, collectioname, documents, callback) {
    database.collection(collectioname).insert(documents, (error, result) => {
        if (error) return process.exit(1);

        console.log('Inserted '+result.result.n+' items into the '+collectioname+' collection')
        callback(result);
    } );
}


/** processedqueued
 *  looks at the queues if there is data buillds a set of objects and 
 *  passes it to the db driver 
 */
function processqueued() {
    var item = new Object;
    var outputcnt = 0;
    var dbstuff = new Array(ToDoCnt);
    var dbitem = 0;
 
    var acnt = adrq.size();
    var ccnt = custq.size();

    console.log ('customer q is:',ccnt, " adress q is: ", acnt);

    outputcnt = Math.min(acnt, ccnt);

    if (outputcnt < ToDoCnt) {
        if (done < 2){
            setTimeout(processqueued);
        } else {
            ToDoCnt = outputcnt;
        }
    }
    if (outputcnt >= ToDoCnt) {
        outputcnt = ToDoCnt;

        dbitem = 0;
        while (dbitem < outputcnt) {
            // clear any old data
            dbstuff[dbitem] = {};

            Object.assign(dbstuff[dbitem], custq.peek());
            custq.dequeue();

            Object.assign(dbstuff[dbitem] , adrq.peek());
            adrq.dequeue();
            
            dbitem += 1;
            written += 1;
        }
        // this writes to the db ....
        insertDocuments(database, collectioname, dbstuff, dbcontinue);
   }
}

/** functions to parse the data failes into objects
 * 
 */
function getDataObjects() {
    // stream parser for customer info
    fs.createReadStream(custfilename, {highWaterMark:2*1024})
        .pipe(cp)
        .on('data', (element) => {
            custq.enqueue(element);
            //console.log('customer q contains: ', custq.size());
        })
        .on('end', ()=> {
            done += 1;  // finished this data
            //console.log('\t\t\tcustomers complete');
        })
    // stream parser for address data
    fs.createReadStream(adrfilename, {highWaterMark:2*1024})
        .pipe(ap)
        .on('data', (element) => {
            adrq.enqueue(element);
            //console.log('adress q contains: ', adrq.size());
        })
        .on('end', ()=> {
            done += 1;  // finished this file
            //console.log('\t\t\taddresses complete');
        })
}

/** done */