const axios = require('axios');
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
require('dotenv').config();

getUsers = async () => {
  const [rows] = await bigquery
    .dataset(`dreamlogic`)
    .table(`get_users_enterprise`)
    .getRows();
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
          const tableId = `get_users_enterprise`;

          const query = `UPDATE ${datasetId}.${tableId}
          SET id = ${e.id},
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
            id: element.id,
            name: element.name,
            email: element.email,
            seenAt: element.seenAt,
            loginAttempts: element.loginAttempts,
            createdAt: element.createdAt,
            rootRole: element.rootRole,
          };
          await bigquery
            .dataset(`dreamlogic`)
            .table(`get_users_enterprise`)
            .insert([payload]);
        }
      });
    })
    .catch((error) => {
      console.log(error);
    });
};

getUsers();
