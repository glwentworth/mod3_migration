/**
 * test reading file and parsing json in a stream
 * 
 */

const stringify = require('stringify-object');
const fs = require('fs');
const mongodb = require('mongodb');

const custfilename = 'm3-customer-data.json';
const adrfilename = 'm3-customer-address-data.json';

var count = 1;

/** 
const jp = require('json-parse-stream');

fs.createReadStream(filename)
    .pipe(jp())
    .on('data', (element) => {
        console.log('next element ('+count+'): ');
        count += 1;
        console.log(stringify(element, {indent: '  ',
                    singleQuotes: false}))
    })
    .on('end', ()=> {
        console.log('complete');
    })
*/

/** stream json parser
 *  breaks the file data into single objects
 */
const parser = require('dummy-streaming-array-parser');

/** code to maintain a queue of objects
 * 
 */
const Queue = require('queue-fifo');

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

/** processedqueued
 *  looks at the queues if there is data buillds a set of objects and 
 *  passes it to the db driver 
 */
var written  = 0;  //count total objects written

function processqueued() {
    var item = new Object;
    var outputcnt = 0;
    var dbstuff = new Array(ToDoCnt);
    var dbitem = 0;
    var doccnt = 0;

    //console.log ('customer q is:',csize, " adress q is: ", csize);
    var acnt = adrq.size();
    var ccnt = custq.size();
    outputcnt = Math.min(acnt, ccnt);
    while (outputcnt >= ToDoCnt) {
        outputcnt = ToDoCnt;

        doccnt += 1;
        console.log('document ',doccnt);
            
        //if ( 0 < outputcnt) {
        //    console.log('process ',outputcnt, ' items...');
        //}
        dbitem = 0;
        while (dbitem < outputcnt) {

            dbstuff[dbitem] = {};

            //var adritem = adrq.peek();
            Object.assign(dbstuff[dbitem] , adrq.peek());
            adrq.dequeue();
            
            //var custitem = custq.peek();
            Object.assign(dbstuff[dbitem], custq.peek());
            custq.dequeue();

            //dbstuff[dbitem] = item;
            //console.log('add item (',dbitem,') to documents: ',dbstuff[dbitem]['ssn']);

            dbitem += 1;
            written += 1;
            //Object.assign(custitem, adritem);
            //Object.assign(custitem, adritem);

        }
        // this writes to the db ....
        insertDocuments(database, collectioname, dbstuff, dbcontinue);

        /*
        console.log("db document contains: ", dbitem, 'parts.');
            //stringify(dbstuff, 
            //        {indent: '  ', singleQuotes: false, inlineCharacterLimit: 96}));

        console.log('\t\t total items out: ', written);
        // should we go again...
        outputcnt = Math.min(adrq.size(), custq.size());
        */


        break;
    }
    /*
    if ( (done >= 2) ) {
        if ( (custq.size() > 0) && (custq.size() < ToDoCnt) ) {
            ToDoCnt = custq.size();
            setTimeout(getaddress);

            console.log('tick try...  done cnt: ',done, '  custq: ',custq.size());
        }
     } else {
        setTimeout(processqueued);
     }
     */ 
}

// Use connect method to connect to the Server

const url = 'mongodb://localhost:27017';  // Connection URI
const dbname = 'edx-course-db';  // database to use
const collectioname = 'bitcoin-owners';  // collection for this data
var database;  // will contain the database info

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

        setTimeout(processqueued);

    }}
)

/** insertDocument 
 *  function that inserts the data intothe database.
 *  database: the client object from connectiong to the data base
 *  collectioname:  the name of the collection to insert into
 *  documents: array of objects to write to the db
 *  callback: code that determines the continuation of the process
 */
function insertDocuments(database, collectioname, documents, callback) {
    console.log('Insert operation....');

    database.collection(collectioname).insert(documents, (error, result) => {
        if (error) return process.exit(1);

        //console.log(result.result.n) // will be 3
        //console.log(result.ops.length) // will be 3
        console.log('Inserted '+result.result.n+' items into the '+collectioname+' collection')
        console.log('result of insert: ',
                        +stringify(result, {indent: '  ', singleQuotes: false}));

        callback(result);
    } );
}

function dbcontinue(result) {
    if (adrq.size() > 0) {
        setTimeout(processqueued);
    }
}
// start processing the two input files...

fs.createReadStream(custfilename, {highWaterMark:2*1024})
    .pipe(cp)
    .on('data', (element) => {

        //console.log(stringify(element, {indent: '  ',
        //singleQuotes: false}));

        custq.enqueue(element);
        //console.log('customer q contains: ', custq.size());
    })
    .on('end', ()=> {
        done += 1;

        console.log('\t\t\tcustomers complete');
    })

fs.createReadStream(adrfilename, {highWaterMark:2*1024})
    .pipe(ap)
    .on('data', (element) => {
        adrq.enqueue(element);
        //console.log('adress q contains: ', adrq.size());
    })
    .on('end', ()=> {
        done += 1;

        console.log('\t\t\taddresses complete');
    })



//setTimeout(processqueued);

/** done */