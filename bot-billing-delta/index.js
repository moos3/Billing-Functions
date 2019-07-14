const {BigQuery} = require('@google-cloud/bigquery');
const moment = require('moment');

const CHANNEL = 'internal';
const DATASET = 'audit';
const TABLE = 'budget';
const PROJECT = process.env.GCP_PROJECT;
const bigquery = new BigQuery();
const BUDGET_ID = process.env.BUDGET_ID;

exports.diffBilling = async (req, res) => {
  console.log("Request body text = ");
  console.log(req.body.text);
  let date1 = '';
  let date2 = '';
  let err = false;
  let message = {};

  if(req.body.text.split(' ').length == 1) {
    date1 = req.body.text.split(' ')[0];
    date2 = moment().format('YYYY-MM-DD');
    console.log("Premier cas");
  } else if(req.body.text.split(' ').length == 2) {
    date1 = req.body.text.split(' ')[0];
    date2 = req.body.text.split(' ')[1];
    console.log("Deuxième cas");
  } else {
    message = {
      response_type: "ephemeral",
      text: `Check the function format first. :+1:`,
      attachments: [],
    };
    res.json(message);
  };

  if(!err) {
    console.log("date1 = " + date1);
    console.log("date2 = " + date2);

    const result = await getBigQuery(date1, date2);

    const costAmount01 = Math.min(result[1].f0_, result[0].f0_);
    const costAmount02 = Math.min(result[1].f1_, result[0].f1_);
    const costAmount11 = Math.max(result[1].f0_, result[0].f0_);
    const costAmount12 = Math.max(result[1].f1_, result[0].f1_);

    const absoluteDiff = Math.round(((costAmount12 - costAmount11) - (costAmount02 - costAmount01))*100)/100;
    const relativeDiff = Math.round(Math.abs(absoluteDiff)/(costAmount02 - costAmount01)*100);

    const evolution = absoluteDiff > 0 ? ['bigger', 'more', ':neutral_face:', ':no_mouth:'] : ['smaller', 'less', ':clap:', ':money_mouth_face:'];

    const message = {
      response_type: "ephemeral",
      text: `The ${date2} billing increase is ${relativeDiff}% ${evolution[0]} than the ${date1} one ${evolution[2]}, that is to say ${absoluteDiff}€. ${evolution[3]}`,
      attachments: [],
    };
    res.json(message);
  };
};

async function getBigQuery(date1, date2) {
      let rows;
      const options = {
        query: `(SELECT CAST(MIN(costAmount) AS FLOAT64), CAST(MAX(costAmount) AS FLOAT64) FROM \`${PROJECT}.${DATASET}.${TABLE}\` WHERE createdAt BETWEEN '${date1}' AND '${date1} 23:59:59.999' AND budgetId = '${BUDGET_ID}') UNION ALL (SELECT CAST(MIN(costAmount) AS FLOAT64), CAST(MAX(costAmount) AS FLOAT64) FROM \`${PROJECT}.${DATASET}.${TABLE}\` WHERE createdAt BETWEEN '${date2}' AND '${date2} 23:59:59.999' AND budgetId = '${BUDGET_ID}')`,
        useLegacySql: false, // Use standard SQL syntax for queries.
        location: 'EU',
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
      };
    };