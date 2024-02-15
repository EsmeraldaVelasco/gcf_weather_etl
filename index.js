const {Storage}=require('@google-cloud/storage');
const {BigQuery}= require('@google-cloud/bigquery');
const csv=require('csv-parser');

const bq=new BigQuery();
const datasetId='weather_dev';
const tableId='weather';


exports.readObservation = (file, context) => {
    // console.log(`  Event: ${context.eventId}`);
    // console.log(`  Event Type: ${context.eventType}`);
    // console.log(`  Bucket: ${file.bucket}`);
    // console.log(`  File: ${file.name}`);

    
   
    const gcs=new Storage();
    const dataFile=gcs.bucket(file.bucket).file(file.name);

    const fileName=file.name;
    const print=[];



    dataFile.createReadStream()
    .on('error', ()=>{
        //Handle an error
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row)=>{
        //Log row data
        //console.log(row);
        //printDict(row)
        print.station=fileName;
        console.log(`${print.station}`);
        printDict(row);
        writeToBq(row);

    })
    .on('end', ()=>{
        //Handle the end of CSV
        console.log('End!');
    
    })

}




//Helper function
function printDict(row){
    for (let key in row){

        if(row[key] =='-9999.0'){
            row[key]=null;
        }

        if (['airtemp', 'dewpoint', 'pressure', 'windspeed', 'precip1hour', 'precip6hour'].includes(key) && !null) {
             row[key] = parseFloat(row[key]) / 10;
        }


        console.log(key + ':' + row[key]);
        //console.log(`${key} : ${row[key]}`)
        
    }


    
}


//Create a helper function that writes to BQ
//Function must be asynchronous 
async function writeToBq(row) {

    await bq
    .dataset(datasetId)
    .table(tableId)
    .insert(row)
    .then( ()=>{
        console.log('added')
    })
    .catch( (err)=>{console.error(`ERROR: ${err}`)})
}

