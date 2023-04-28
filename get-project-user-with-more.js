const axios = require('axios');
require('dotenv').config();
const moment = require('moment-timezone');

const getProjectUserMore = async (projects) => {
  const matchProjectFromProjectUsers = [];

  for (const id of projects) {
    let config = {
      method: 'get',
      maxBodyLength: Infinity,
      url: process.env.URL_GET_PROJECT_USER + id + '/users',
      headers: {
        Accept: 'application/json',
        Authorization: process.env.ENTERPRISE,
      },
    };

    await axios(config)
      .then((response) => {
        if (response.data.users.length != 0) {
          for (const e of response.data.users) {
            matchProjectFromProjectUsers.push({
              ingestion_time: moment.utc().format('YYYY-MM-DD HH:mm:ss.SSSSSS'),
              project: id,
              userType: 'users',
              id: e.id,
              name: e.name,
              email: e.email,
              roleId: e.roleId,
            });
          }
        }
        if (response.data.groups.length != 0) {
          for (const e of response.data.groups[0].users) {
            matchProjectFromProjectUsers.push({
              ingestion_time: moment.utc().format('YYYY-MM-DD HH:mm:ss.SSSSSS'),
              project: id,
              userType: 'groups',
              id: e.user.id,
              name: e.user.name,
              email: e.user.email,
              roleId: response.data.groups[0].roleId,
            });
          }
        }
      })
      .catch((error) => {
        console.log(error);
      });
  }

  return matchProjectFromProjectUsers;
};

module.exports = { getProjectUserMore };
