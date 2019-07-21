// Dependency
const { BigQuery } = require('@google-cloud/bigquery');

// Environment variables
const PROJECT = process.env.GCP_PROJECT;
const DATASET = process.env.DATASET;
const DATASET_LOCATION = process.env.DATASET_LOCATION;
const TABLE = process.env.TABLE;
const BILLING_ID = process.env.BILLING_ID;
const bigquery = new BigQuery();

exports.getCurrentBilling = async (req, res) => {
  // Run the BigQuery request and wait for the results
  const result = await getBigQuery();

  // Retrieve the data from the results
  const costAmount = result[0].cost;
  const budgetAmount = result[0].budget;
  const rate = costAmount/budgetAmount*100;

  const emoticon = rate >= 90 ? ':scream: :fire: :scream:' : '';

  // Create the Slack message structure
  const message = {
    response_type: "ephemeral",
    text: `${emoticon} \nYour billing account has exceeded ${rate}% of the monthly budget of ${budgetAmount}.\nThe billing account has accrued ${':moneybag:'} ${costAmount}€ ${':moneybag:'} in costs so far for the month.`,
    attachments: [],
  };

  // Format the Slack message as JSON
  res.json(message);
}

async function getBigQuery() {
    let rows;
    const options = {
      query: `SELECT CAST(costAmount AS FLOAT64) as cost, CAST(budgetAmount AS FLOAT64) as budget FROM \`${PROJECT}.${DATASET}.${TABLE}\` WHERE BudgetId = '${BILLING_ID}' ORDER BY createdAt DESC LIMIT 1`,
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