const path = require('path');

const AWS = require('aws-sdk');

//const credentials = new AWS.Credentials(process.env.AWS_ACCESS_KEY_ID, process.env.AWS_SECRET_ACCESS_KEY, process.env.AWS_SESSION_TOKEN)

var credentials = new AWS.EnvironmentCredentials('AWS');

AWS.config.update({
    credentials: credentials,
    region: process.env.AWS_REGION
});


const esDomain = {
    region: process.env.AWS_REGION,
    endpoint: process.env.ES_ENDPOINT,
    index: 'test_notification',
    doctype: 'offer'
};

const endpoint = new AWS.Endpoint(esDomain.endpoint);


exports.handler = (event, context, callback) => {
    console.log("--> AWS_REGION: ", process.env.AWS_REGION);

    console.log("--> event: ", event);
    //console.log("--> context: ", context);
    //event.memberId = null; // test

    let checked = check(event, context, callback);
    console.log("--> check: ", checked);

    if (checked) {
        let id = event.id;
        post(id, context);

        get(id, context);

    }
    else {
        let message = {
            "isBase64Encoded": false,
            "statusCode": 500,
            "headers": {},
            "body": {
                "errorMessage": "Lambda UnreadNumber failed with error."
            }
        };
        context.fail(JSON.stringify(message));
    }

    //

};

function test(id, context) {
    let data = {
        "_id": 1,
        "index": esDomain.index,
        "alias": "esDomain.index",
        "id": 1
    };
    postToES(id, data, context);
}

function check(event, context, callback) {
    console.log("--> event: ", event);
    let message = {
        "isBase64Encoded": false,
        "statusCode": 400,
        "headers": {},
        "body": { "errorMessage": "Parameters error" }
    };
    if (!event.id) {
        message.body.errorMessage = "Parameter 'id' is null."
        context.fail(JSON.stringify(message));
        return false;
    }
    console.log("--> id: ", event.id);
    return true;
}

function get(id, context) {
    let query = {
        "query": {
            "bool": {
                "must": [
                    { "match": { "id": id } }
                ]
            }
        }
    };
    console.log('query: ' + JSON.stringify(query));
    getFromES(id, query, context);
}

function postToES(id, data, context) {
    let req = new AWS.HttpRequest(endpoint);

    req.method = 'POST';
    req.path = path.join('/', esDomain.index, esDomain.doctype);
    req.region = esDomain.region;
    req.headers['presigned-expires'] = false;
    req.headers['Host'] = endpoint.host;
    req.headers['Content-Type'] = "application/json; charset=UTF-8";

    req.body = JSON.stringify(data);

    let signer = new AWS.Signers.V4(req, 'es'); // es: service code
    signer.addAuthorization(credentials, new Date());

    let send = new AWS.NodeHttpClient();
    send.handleRequest(req, null, function(httpResp) {
            let respBody = '';
            httpResp.on('data', function(chunk) {
                respBody += chunk;
            });
            httpResp.on('end', function(chunk) {
                console.log('POST Response: ' + JSON.stringify(respBody));
                let message = {
                    "isBase64Encoded": false,
                    "statusCode": 200,
                    "headers": {},
                    "body": respBody
                };
                context.succeed(JSON.stringify(message));
            });
        },
        function(err) {
            console.log('POST Error: ' + err);
            let message = {
                "isBase64Encoded": false,
                "statusCode": 500,
                "headers": {},
                "body": err
            };
            context.fail(JSON.stringify(message));
        });
}


function getFromES(id, query, context) {

    console.log('--> path: ', '/' + esDomain.index + "/" + esDomain.doctype);

    let req = new AWS.HttpRequest(endpoint);

    req.method = 'POST';
    req.path = path.join('/', esDomain.index, esDomain.doctype, "_search");
    req.region = esDomain.region;
    req.headers['presigned-expires'] = false;
    req.headers['Host'] = endpoint.host;
    req.headers['Content-Type'] = "application/json; charset=UTF-8";
    req.body = JSON.stringify(query);

    let signer = new AWS.Signers.V4(req, 'es'); // es: service code
    signer.addAuthorization(credentials, new Date());

    let send = new AWS.NodeHttpClient();
    send.handleRequest(req, null, function(httpResp) {
            let respBody = '';
            httpResp.on('data', function(chunk) {
                respBody += chunk;
            });
            httpResp.on('end', function(chunk) {
                console.log('GET Response: ' + JSON.stringify(respBody));
                let message = {
                    "isBase64Encoded": false,
                    "statusCode": 200,
                    "headers": {},
                    "body": respBody
                };
                //context.succeed(JSON.stringify(message));
                context.succeed(message);
            });
        },
        function(err) {
            console.log('GET Error: ' + err);
            let message = {
                "isBase64Encoded": false,
                "statusCode": 500,
                "headers": {},
                "body": err
            };
            context.fail(JSON.stringify(message));
        });
}
