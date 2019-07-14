const slack = require('slack');
const {BigQuery} = require('@google-cloud/bigquery');

const CHANNEL = 'internal';
const DATASET = 'audit'
const TABLE = 'budget';
const PROJECT = process.env.GCP_PROJECT;
const DATASET_LOCATION = 'EU';
const bigquery = new BigQuery();
const BOT_ACCESS_TOKEN = process.env.BOTTOKEN;

exports.notifySlack = async (data, context) => {

    const pubsubMessage = data;
    const pubsubData = JSON.parse(Buffer.from(pubsubMessage.data, 'base64').toString());
    const formatter = new Intl.NumberFormat('en-US', {style: 'currency', currency: 'USD', minimumFractionDigits: 2})
    const budgetId = pubsubMessage.attributes.budgetId;
    const costAmount = formatter.format(pubsubData.costAmount);
    const budgetAmount = formatter.format(pubsubData.budgetAmount);
    const budgetName = pubsubData.budgetDisplayName;
    const createdAt = new Date().toISOString();
    let threshold = (pubsubData.alertThresholdExceeded*100).toFixed(0);

    if (!isFinite(threshold)){
        threshold = 0;
    }
    //save data
    const rows = [{createdAt: createdAt,
                    costAmount: pubsubData.costAmount,
                    budgetAmount:pubsubData.budgetAmount,
                    budgetId: budgetId,
                    budgetName: budgetName,
                    threshold: threshold}]

    await bigquery.dataset(DATASET).table(TABLE).insert(rows);

    async function getBigQuery() {
      let rows;
      const options = {
        query: `SELECT count(*) cnt FROM \`${PROJECT}.${DATASET}.${TABLE}\` WHERE createdAt >  TIMESTAMP( DATE(EXTRACT(YEAR FROM CURRENT_DATE()) , EXTRACT(MONTH FROM CURRENT_DATE()), 1)) AND Threshold = ${threshold} and BudgetId = '${budgetId}'`,
        useLegacySql: false, // Use standard SQL syntax for queries.
        location: 'EU',
      };
      // Runs the query
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

    results = await getBigQuery();

    console.log(results)
    console.log(results.length)
    console.log(results[0].cnt)


    if (results.length > 0 && results[0].cnt > 1 ){
     console.log('No notifications to slack will be sent')
     return;
    }

    console.log('Notification will be sent')

    const emoticon = threshold >= 90 ? ':fire:' : '';

    notification = `${emoticon} ${budgetName} \nThis is an automated notification to inform you that your billing account has exceeded ${threshold}% of the monthly budget of ${budgetAmount}.\nThe billing account has accrued ${costAmount} in costs so far for the month.`

    const res = await slack.chat.postMessage({
        token: BOT_ACCESS_TOKEN,
        channel: CHANNEL,
        text: notification
    });
};