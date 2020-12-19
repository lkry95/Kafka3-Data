from kafka import KafkaConsumer, TopicPartition
from json import loads
import statistics

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}

        self.totalDeposit = 0
        self.depositCounter = 0
        self.totalWithdrawl = 0
        self.withdrawlCounter = 0
        self.averageDeposit = 0
        self.averageWithdrawl = 0
        self.deposit = []
        self.withdrawl = []

    def handleMessages(self):

        stddevDeposit = 0
        stddevWithdrawl = 0

        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']

                self.custBalances[message['custid']] += message['amt']

                self.totalDeposit += message['amt']
                self.depositCounter += 1
                self.averageDeposit = round(self.totalDeposit / self.depositCounter, 2)

                self.deposit.append(message['amt'])
                if len(self.deposit) > 1:
                    stddevDeposit = round(statistics.stdev(self.deposit), 2)

            else:
                self.custBalances[message['custid']] -= message['amt']
                self.totalWithdrawl += message['amt']
                self.withdrawlCounter += 1
                self.averageWithdrawl = round(self.totalWithdrawl / self.withdrawlCounter, 2)

                self.withdrawl.append(message['amt'])
                if len(self.withdrawl) > 1:
                    stddevWithdrawl = round(statistics.stdev(self.withdrawl), 2)

            print('total amount of deposits: ', self.totalDeposit, '\n', 'total # of deposits: ', self.depositCounter,
                  '\n', 'average deposit: ', self.averageDeposit, '\n' ' std dev of deposits: ', stddevDeposit, '\n',
                  '\n', 'total amount of withdrawls: ', self.totalWithdrawl,
                  '\n', 'total # of withdrawls: ', self.withdrawlCounter, '\n', 'average withdrawl: ', self.averageWithdrawl,
                  '\n', 'std dev of withdrawls: ', stddevWithdrawl)


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()