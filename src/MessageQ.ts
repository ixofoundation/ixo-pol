import axios from 'axios';

const amqplib = require('amqplib');
const dateFormat = require('dateformat');

const BLOCKCHAIN_URI_SYNC = (process.env.BLOCKCHAIN_URI_SYNC || '');
const BLOCKCHAIN_URI_COMMIT = (process.env.BLOCKCHAIN_URI_COMMIT || '');
const BLOCKCHAIN_URI_VALIDATE = (process.env.BLOCKCHAIN_URI_VALIDATE || '');
const ETHEREUM_API = (process.env.ETHEREUM_API || 'https://mainnet.infura.io/');

class MessageQ {

  connection: any;
  private queue: string;
  private readonly lookupBlockChainURI: any;

  constructor(queue: string) {
    this.lookupBlockChainURI = {
      'SYNC': BLOCKCHAIN_URI_SYNC,
      'COMMIT': BLOCKCHAIN_URI_COMMIT,
      'VALIDATE': BLOCKCHAIN_URI_VALIDATE
    };
    this.queue = queue;
  }

  dateTimeLogger() {
    return dateFormat(new Date(), "yyyy-mm-dd hh:mm:ss:l");
  }

  public connect(): Promise<any> {
    const inst = this;
    return new Promise(function (resolve, reject) {
      amqplib.connect(process.env.RABITMQ_URI || '')
        .then((conn: any) => {
          inst.connection = conn;
          console.log(inst.dateTimeLogger() + ' RabbitMQ connected');
          resolve(conn);
        }, () => {
          throw new Error("Cannot connect to RabbitMQ Server");
        });
    });
  }

  public subscribe(): Promise<any> {
    const inst = this;
    return new Promise(async function (resolve: Function, reject: Function) {
      try {
        const channel = await inst.connection.createChannel();
        channel.assertExchange("pds.ex", "direct", {durable: true});
        channel.assertQueue(inst.queue, {
          durable: true
        })
          .then(() => {
            channel.bindQueue(inst.queue, 'pds.ex');
          })
          .then(() => {
            channel.prefetch(50);
            channel.consume(inst.queue, (messageData: any) => {
              if (messageData === null) {
                return;
              }
              const message = JSON.parse(messageData.content.toString());
              inst.handleMessage(message.data)
                .then((response) => {
                  const msgResponse = {
                    msgType: message.data.msgType,
                    txHash: message.txHash,
                    data: response.result
                  };
                  console.log(inst.dateTimeLogger() + ' return blockchain response message ' + message.txHash);
                  channel.sendToQueue('pds.res', Buffer.from(JSON.stringify(msgResponse)), {
                    persistent: false,
                    contentType: 'application/json'
                  });
                  return channel.ack(messageData);
                }, (error) => {
                  channel.sendToQueue('pds.res', Buffer.from(JSON.stringify({
                    msgType: "error",
                    data: error,
                    txHash: message.txHash
                  })), {
                    persistent: false,
                    contentType: 'application/json'
                  });
                  return channel.ack(messageData);
                });
            });
          }, (error: any) => {
            throw error;
          });
      } catch (error) {
        throw new Error(error.message);
      }
    })
  }

  private handleMessage(message: any): Promise<any> {
    return new Promise((resolve, reject) => {
      console.log(this.dateTimeLogger() + ' consume from queue' + JSON.stringify(message));
      if (message.msgType === 'eth') {
        const txnId = message.data;
        axios.request({
          method: 'post',
          url: ETHEREUM_API,
          data: {jsonrpc: "2.0", method: "eth_getTransactionByHash", params: [txnId], id: 1}
        })
          .then((response) => {
            console.log(this.dateTimeLogger() + ' received response from ethereum ' + response.data.hash);
            resolve(response);
          })
          .catch((reason) => {
            console.log(this.dateTimeLogger() + ' no response from ethereum ' + reason);
            reject(reason);
          });
      } else {
        const blockchainUrl = this.lookupBlockChainURI[message.uri];
        console.log(this.dateTimeLogger() + ' sending message to' + blockchainUrl);
        axios.get(blockchainUrl + message.data).then((response) => {
          if (response.data && response.data.result) {
            console.log(this.dateTimeLogger() + ' received response from blockchain ' + response.data.result.hash);
            resolve(response.data);
          } else {
            console.log(this.dateTimeLogger() + ' received error response from blockchain ' + JSON.stringify(response.data));
            reject(response.data.error.data || "Unknown error");
          }
        })
          .catch((reason) => {
            console.log(this.dateTimeLogger() + ' no response from blockchain ' + reason.response || reason);
            reject(reason);
          });
      }
    });
  }
}

export default new MessageQ('pds')
