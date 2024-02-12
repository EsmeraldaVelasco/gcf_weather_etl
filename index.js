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
    dataFile.createReadStream()
    .on('error', ()=>{
        //Handle an error
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row)=>{
        //Log row data
        //console.log(row);
        printDict(row);
    })
    .on('end', ()=>{
        //Handle the end of CSV
        console.log('End!');
        try{
           writeToBq(row);
        }
        catch(error){
            console.error('Error')
        }
    })



}

//Helper function
function printDict(row){
    for (let key in row){
        if(row[key] == '-9999'){
            row[key]=null;
        }
        if(key=='airtemp'|| key=='dewpoint' ||key=='pressure' || key=='windspeed'|| key=='precip1hour' || key=='precip6hour'){
            row[key]/10;
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
        row.forEach( (row) =>{console.log(`Inserted: ${row}`)})
    })
    .catch( (err)=>{console.error(`ERROR: ${err}`)})
}

