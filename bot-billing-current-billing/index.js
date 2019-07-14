const {BigQuery} = require('@google-cloud/bigquery');
const slack = require('slack');

const CHANNEL = 'internal';
const DATASET = 'audit';
const TABLE = 'budget';
const PROJECT = process.env.GCP_PROJECT;
const bigquery = new BigQuery();
const BUDGET_ID = process.env.BUDGET_ID;

exports.getCurrentBilling = async (req, res) => {
  async function getBigQuery() {
    let rows;
    const options = {
      query: `SELECT CAST(costAmount AS FLOAT64), CAST(budgetAmount AS FLOAT64) FROM \`${PROJECT}.${DATASET}.${TABLE}\` WHERE BudgetId = '${BUDGET_ID}' ORDER BY createdAt DESC LIMIT 1`,
      useLegacySql: false, // Use standard SQL syntax for queries.
      location: 'EU'
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
  const result = await getBigQuery();

  const costAmount = result[0].f0_;
  const budgetAmount = result[0].f1_;
  const rate = costAmount/budgetAmount*100;

  const emoticon = rate >= 90 ? ':scream: :fire: :scream:' : '';

  const message = {
    response_type: "ephemeral",
    text: `${emoticon} \nYour billing account has exceeded ${rate}% of the monthly budget of ${budgetAmount}.\nThe billing account has accrued ${':moneybag:'} ${costAmount}€ ${':moneybag:'} in costs so far for the month.`,
    attachments: [],
  };

  res.json(message);
}
