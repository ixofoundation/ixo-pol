require('dotenv').config();

const MessageQ = require('./MessageQ')

MessageQ.default.connect()
  .then((conn: any) => {
    setInterval(
      MessageQ.default.subscribe(),
      parseInt(process.env.pollTimer || '3000')
    );
  }).catch(() => {
});

process.on('SIGTERM', function () {
  console.log('Shut down');
  MessageQ.default.connection.close();
});
