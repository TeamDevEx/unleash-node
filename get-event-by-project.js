const axios = require('axios');
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
require('dotenv').config();
const moment = require('moment-timezone');

getProjectList = () => {
  let config = {
    method: 'get',
    maxBodyLength: Infinity,
    url: process.env.URL_GET_PROJECTS,
    headers: {
      Accept: 'application/json',
      Authorization: process.env.ENTERPRISE,
    },
  };

  return axios(config)
    .then(async (response) => {
      return response.data.projects.map((i) => i.id);
    })
    .catch((error) => {
      console.log(error);
    });
};

getEventsByProject = async () => {
  const arrayOfProjectId = await getProjectList();

  arrayOfProjectId.forEach((element) => {
    let config = {
      method: 'get',
      maxBodyLength: Infinity,
      url: process.env.URL_GET_PROJECT_EVENTS + element,
      headers: {
        Accept: 'application/json',
        Authorization: process.env.ENTERPRISE,
      },
    };
    axios(config)
      .then(async (response) => {
        response.data.events.forEach(async (i) => {
          const payload = {
            ingestion_time: moment.utc().format('YYYY-MM-DD HH:mm:ss.SSSSSS'),
            id: i.id,
            type: i.type,
            createdBy: i.createdBy,
            createdAt: i.createdAt,
            featureName: i.featureName,
            project: i.project,
            environment: i.environment,
          };

          await bigquery
            .dataset(`dreamlogic`)
            .table(`project_events`)
            .insert([payload]);
        });
      })
      .catch((error) => {
        console.log(error.message);
      });
  });
};
getEventsByProject();
