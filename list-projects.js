const axios = require('axios');
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const { getProjectUserMore } = require('./get-project-user-with-more');
require('dotenv').config();

getArrayProjects = async () => {
  let config = {
    method: 'get',
    maxBodyLength: Infinity,
    url: process.env.URL_GET_PROJECTS,
    headers: {
      Accept: 'application/json',
      Authorization: process.env.ENTERPRISE,
    },
  };

  axios(config)
    .then(async (response) => {
      const arrayProjects = response.data.projects.map((i) => i.id);

      const listProject = await getProjectUserMore(arrayProjects);
      listProject.forEach(async (element) => {
        await bigquery
          .dataset(`dreamlogic`)
          .table(`list-projects`)
          .insert([element]);
      });
    })
    .catch((error) => {
      console.log(error);
    });
};

getArrayProjects();
