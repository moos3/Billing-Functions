// Dependencies
const { BigQuery } = require('@google-cloud/bigquery');
const moment = require('moment');
const axios = require('axios');

// Environment variables
const PROJECT = process.env.GCP_PROJECT;
const DATASET = process.env.DATASET;
const DATASET_LOCATION = process.env.DATASET_LOCATION;
const TABLE = process.env.TABLE;
const BILLING_ID = process.env.BILLING_ID;
const bigquery = new BigQuery();

exports.diffBilling = async (req, res) => {
  // Slack initialization message
  let messageInit = {
    text: "Billing difference is being calculated :computer:",
  };

  try {
    // Different cases processing
    if (req.body.text.split(' ')[0].length < 10 || req.body.text.split(' ').length > 2) {
      throw new Error();
    } else if (req.body.text.split(' ').length == 1) {
      date1 = req.body.text.split(' ')[0];
      date2 = moment().format('YYYY-MM-DD');
    } else {
      date1 = req.body.text.split(' ')[0];
      date2 = req.body.text.split(' ')[1];
    }

    if (date1 > date2) {
      throw new Error();
    }

    // Send to Slack the initialization message as JSON if there is no error thrown
    res.json(messageInit);

    // Run the BigQuery request
    const result = await getBigQuery(date1, date2);

    // Retrieve the response data
    const cost01 = result[0].minCost;
    const cost02 = result[0].maxCost;
    const cost11 = result[1].minCost;
    const cost12 = result[1].maxCost;

    // Compute some figures
    const absoluteDiff = Math.round(((cost12 - cost11) - (cost02 - cost01)) * 100) / 100;
    const relativeDiff = Math.round(Math.abs(absoluteDiff) / (cost02 - cost01) * 100);

    // Some textual customization
    const evolution = absoluteDiff > 0 ? ['bigger', 'more', ':neutral_face:', ':no_mouth:'] : ['smaller', 'less', ':clap:', ':money_mouth_face:'];

    // Run the post request with the final message inside
    axios.post(req.body.response_url, {
      response_type: 'ephemeral',
      text: `The ${date2} billing increase is ${relativeDiff}% ${evolution[0]} than the ${date1} one ${evolution[2]}, that is to say ${absoluteDiff}€. ${evolution[3]}`,
      attachments: {},
    }, {
      headers: {
        'Content-Type': 'application/json',
      }
    });
  } catch (e) {
    const errorMessage = {
      text: "Syntax error: 1st date < 2nd date, at least 1 date as input (max 2)",
    };
    // If any error is thrown, return a generic error message to the Slack channel to notify the user and run no query
    res.json(errorMessage)
  }
};

async function getBigQuery(date1, date2) {
  let rows;
  const options = {
    query: `SELECT CAST(MIN(costAmount) AS FLOAT64) as minCost, CAST(MAX(costAmount) AS FLOAT64) as maxCost FROM \`${PROJECT}.${DATASET}.${TABLE}\` WHERE createdAt BETWEEN '${date1}' AND '${date1} 23:59:59.999' AND budgetId = '${BILLING_ID}' UNION ALL SELECT CAST(MIN(costAmount) AS FLOAT64), CAST(MAX(costAmount) AS FLOAT64) FROM \`${PROJECT}.${DATASET}.${TABLE}\` WHERE createdAt BETWEEN '${date2}' AND '${date2} 23:59:59.999' AND budgetId = '${BUDGET_ID}' ORDER BY minCost ASC`,
    useLegacySql: false, // Use standard SQL syntax for queries.
    location: DATASET_LOCATION,
  };
  console.log(options.query);
  let job;
  try {
    const result = await new Promise((resolve, reject) => {
      bigquery
        .createQueryJob(options)
        .then(results => {
          job = results[0];
          console.log(`Job ${job.id} started.`);
          return job.promise();
        })
        .then(() =>
          // Get the job's status
          job.getMetadata()
        )
        .then(metadata => {
          // Check the job's status for errors
          const errors = metadata[0].status.errors;
          if (errors && errors.length > 0) {
            throw errors;
          }
        })
        .then(() => {
          console.log(`Job ${job.id} completed.`);
          return job.getQueryResults();
        })
        .then(results => {
          rows = results[0];
          console.log('Rows:');
          rows.forEach(row => console.log(row));
          resolve(rows);
        })
        .catch(err => {
          reject(err);
        });
    });
    return result;
  } catch (error) {
    throw new Error(error);
  }
}
