const AWS = require('aws-sdk');
const csv = require('csvtojson');

const s3 = new AWS.S3();
const docClient = new AWS.DynamoDB({apiVersion: '2012-08-10'});

exports.handler = (event, context) => {
    const bucketName = process.env.S3_BUCKET_NAME;
    const keyName = event.Records[0].s3.object.key;
    console.log(`**********key is ${keyName}**********`);
    const params = { Bucket: bucketName, Key: keyName };
    
//grab the csv file from s3        
    const s3Stream = s3.getObject(params).createReadStream()
    
    console.log(`**********BUCKET NAME is "${bucketName}"**********`);
    console.log("**********S3 is fetched!**********");
    csv().fromStream(s3Stream)
         .on('data', (row) => {
            //read each row 
                let jsonContent = JSON.parse(row);
                console.log(`**********jsonContent**********`);
                console.log(JSON.stringify(jsonContent));
                         
            //push each row into DynamoDB
                // let paramsToPush = {
                //     TableName:process.env.DYNAMODB_TABLE_NAME,
                //     Item:{
                //         "csv-to-json" : {S: "key1"},
                //         "Series_reference" : {S: jsonContent.Series_reference},
                //         "Subject": {S: jsonContent.Subject},
                //         "Group": {S: jsonContent.Group},
                //         "Series_title_4": {S: jsonContent.Series_title_4}
                //         }
                // };
                let paramsToPush = {
                  RequestItems: {
                    "dynamo1": [
                       {
                         PutRequest: {
                           Item: {
                             "search-key": { "S": "hi" },
                             "ATTRIBUTE_1": { "S": "ATTRIBUTE_1_VALUE" },
                             "ATTRIBUTE_1": { "S": "ATTRIBUTE_1_VALUE" }
                           }
                         }
                       },
                       {
                         PutRequest: {
                           Item: {
                             "search-key": { "S": "hello" },
                             "ATTRIBUTE_1": { "S": "ATTRIBUTE_1_VALUE" },
                             "ATTRIBUTE_1": { "S": "ATTRIBUTE_1_VALUE" }
                           }
                         }
                       }
                    ]
                  }
                };
                addData(paramsToPush);
    });
      
};


function addData(params) {
    console.log("Adding a new item based on: ");
    docClient.batchWriteItem(params, function(err, data) {
        if (err) {
            console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            console.log("Added item:", JSON.stringify(params.Item, null, 2));
        }
    });
}