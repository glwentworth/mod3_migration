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

const parser = require('dummy-streaming-array-parser');
const Queue = require('queue-fifo');

cp = parser()
ap = parser();

var adrq = new Queue();
var custq = new Queue();

var done = 0;

fs.createReadStream(custfilename, {highWaterMark:16*1024})
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

fs.createReadStream(adrfilename, {highWaterMark:16*1024})
    .pipe(ap)
    .on('data', (element) => {
        adrq.enqueue(element);
        //console.log('adress q contains: ', adrq.size());
    })
    .on('end', ()=> {
        done += 1;

        console.log('\t\t\taddresses complete');
    })


var ToDoCnt = 100;

var written  = 0;

function getaddress() {
    var item = new Object;
    var outputcnt = 0;
    var dbstuff = new Array(ToDoCnt);
    var dbitem = 0;
    var doccnt = 0;

    //console.log ('customer q is:',csize, " adress q is: ", csize);
   
    outputcnt = Math.min(adrq.size(), custq.size());
    while (outputcnt >= ToDoCnt) {
        outputcnt = ToDoCnt;

        doccnt += 1;
        console.log('document ',doccnt);
            
        //if ( 0 < outputcnt) {
        //    console.log('process ',outputcnt, ' items...');
        //}

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
        // this is the write to the db ....
        //console.log(
            //stringify(dbstuff, 
            //        {indent: '  ', singleQuotes: false, inlineCharacterLimit: 96}));

        console.log('\t\t total items out: ', written);
        // should we go again...
        outputcnt = Math.min(adrq.size(), custq.size());
    }
    
    if ( (done < 2) | (custq.size() > 0) ) {

        console.log('tick try');
        setTimeout(getaddress);
    }
}

setTimeout(getaddress);

/** done */