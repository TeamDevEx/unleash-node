const axios = require('axios');
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
require('dotenv').config();
const moment = require('moment-timezone');

getApplication = async () => {
  const [rows] = await bigquery.dataset(`dreamlogic`).table(`app`).getRows();
  const arrOfApp = rows.map((i) => i.appName);

  let config = {
    method: 'get',
    maxBodyLength: Infinity,
    url: process.env.URL_GET_APPLICATION,
    headers: {
      Accept: 'application/json',
      Authorization: process.env.ENTERPRISE,
    },
  };

  // const dummy = [
  //   {
  //     appName: 'jimbit-app',
  //     createdAt: '2022-01-29T19:54:55.559Z',
  //     updatedAt: '2023-01-20T11:20:43.857Z',
  //     description: 'rrrrrrrrrrrrrrr',
  //     strategies: 2,
  //     createdBy: '35.203.103.197ooooooooooooo',
  //   },
  // ];

  axios(config)
    .then((response) => {
      response.data.applications.forEach(async (i) => {
        if (arrOfApp.includes(i.appName)) {
          const project = `off-net-dev.dreamlogic.app`;

          const query = `UPDATE ${project}  
                    SET ingestion_time = '${moment
                      .utc()
                      .format('YYYY-MM-DD HH:mm:ss.SSSSSS')}',
                        updatedAt = '${i.updatedAt}',
                        description = '${i.description}',
                        strategies = ${i.strategies},
                        createdBy = '${i.createdBy}'               
                    WHERE appName = '${i.appName}';`;

          const [job] = await bigquery.createQueryJob({
            query: query,
          });

          await job.getMetadata();
        } else {
          const payload = {
            ingestion_time: moment.utc().format('YYYY-MM-DD HH:mm:ss.SSSSSS'),
            appName: i.appName,
            createdAt: i.createdAt,
            updatedAt: i.updatedAt,
            description: i.description,
            strategies: i.strategies.length,
            createdBy: i.createdBy,
          };
          await bigquery.dataset(`dreamlogic`).table(`app`).insert([payload]);
        }
      });
    })
    .catch((error) => {
      console.log(error.message);
    });
};

getApplication();
