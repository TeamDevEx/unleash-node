const axios = require('axios');
require('dotenv').config();
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const moment = require('moment-timezone');

let config = {
  method: 'get',
  maxBodyLength: Infinity,
  url: process.env.URL_GET_METRIC_RPS,
  headers: {
    Accept: 'application/json',
    Authorization: process.env.ENTERPRISE,
  },
};

axios(config)
  .then(async (response) => {
    const metricRps = [];
    for (const i of response.data.data.result) {
      const obj = {
        metricName: i.metric.appName,
      };
      const mappedValues = i.values.map(([time, rps]) => {
        return {
          time: new Date(time * 1000),
          rps,
        };
      });
      obj['values'] = mappedValues;
      metricRps.push(obj);
    }
    metricRps.forEach(async (e) => {
      const insert = `INSERT INTO dreamlogic.metric (ingestion_time,metric) VALUES(TIMESTAMP('${moment
        .utc()
        .format('YYYY-MM-DD HH:mm:ss.SSSSSS')}'),PARSE_JSON('${JSON.stringify(
        e
      )}'))`;

      await bigquery.query({ query: insert });
    });
  })
  .catch((error) => {
    console.log(error.message);
  });
