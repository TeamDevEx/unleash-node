const axios = require('axios');
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
require('dotenv').config();

getProject = async () => {
  let config = {
    method: 'get',
    maxBodyLength: Infinity,
    url: process.env.URL_GET_PROJECTS,
    headers: {
      Accept: 'application/json',
      Authorization: process.env.ENTERPRISE,
    },
  };

  const arrayOfProjectName = [];
  const [rows] = await bigquery
    .dataset(`dreamlogic`)
    .table(`get_projects`)
    .getRows();
  for (const i of rows) {
    arrayOfProjectName.push(i.name);
  }
  console.log(arrayOfProjectName);
  axios(config)
    .then((response) => {
      response.data.projects.forEach(async (e) => {
        if (arrayOfProjectName.includes(e.name)) {
          const datasetId = 'dreamlogic';
          const tableId = 'get_projects';

          const query = `UPDATE ${datasetId}.${tableId}
          SET id = '${e.id}',
              description = '${e.description}',
              health = ${e.health},
              featureCount = ${e.featureCount},
              memberCount = ${e.memberCount},
              updatedAt = TIMESTAMP('${e.updatedAt}'),
              mode = '${e.mode}',
              defaultStickiness = '${e.defaultStickiness}'
          WHERE name = '${e.name}'`;

          const [job] = await bigquery.createQueryJob({
            query: query,
          });

          await job.getMetadata();
          console.log(`Updated row with name '${e.name}' successfully.`);
        } else {
          const payload = [
            {
              name: e.name,
              id: e.id,
              description: e.description,
              health: e.health,
              featureCount: e.featureCount,
              memberCount: e.memberCount,
              updatedAt: e.updatedAt,
              mode: e.mode,
              defaultStickiness: e.defaultStickiness,
            },
          ];
          await bigquery
            .dataset(`dreamlogic`)
            .table(`get_projects`)
            .insert(payload);
        }
      });
    })
    .catch((error) => {
      console.log(error);
    });
};

getProject();
