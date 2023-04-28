const axios = require('axios');
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
require('dotenv').config();
const moment = require('moment-timezone');

getUsers = async () => {
  const [rows] = await bigquery.dataset(`dreamlogic`).table(`users`).getRows();
  const arrayOfEmail = rows.map((i) => i.email);
  let config = {
    method: 'get',
    maxBodyLength: Infinity,
    url: process.env.URL_ENTERPRISE,
    headers: {
      Accept: 'application/json',
      Authorization: process.env.ENTERPRISE,
    },
  };

  axios(config)
    .then(async (response) => {
      response.data.users.forEach(async (e) => {
        if (arrayOfEmail.includes(e.email)) {
          const datasetId = `dreamlogic`;
          const tableId = `users`;

          const query = `UPDATE ${datasetId}.${tableId}
          SET ingestion_time = '${moment
            .utc()
            .format('YYYY-MM-DD HH:mm:ss.SSSSSS')}'
              id = ${e.id},
              name = '${e.name}', 
              seenAt = TIMESTAMP('${e.seenAt}'),
              loginAttempts = ${e.loginAttempts},
              createdAt = TIMESTAMP('${e.createdAt}'),
              rootRole = ${e.rootRole}              
          WHERE email = '${e.email}'`;

          const [job] = await bigquery.createQueryJob({
            query: query,
          });

          await job.getMetadata();
        } else {
          const payload = {
            ingestion_time: moment.utc().format('YYYY-MM-DD HH:mm:ss.SSSSSS'),
            id: e.id,
            name: e.name,
            email: e.email,
            seenAt: e.seenAt,
            loginAttempts: e.loginAttempts,
            createdAt: e.createdAt,
            rootRole: e.rootRole,
          };
          await bigquery.dataset(`dreamlogic`).table(`users`).insert([payload]);
        }
      });
    })
    .catch((error) => {
      console.log(error);
    });
};

getUsers();
