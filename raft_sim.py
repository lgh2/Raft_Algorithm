# this is the good one

from time import time, sleep
from random import randint
from uuid import uuid4
from raft_messages import AddEntry, Vote, Terminate, ClientEntry
import curio
from logging import getLogger, basicConfig, DEBUG
logger = getLogger(__name__)
basicConfig(level=DEBUG)


class RaftNode:
    """a node for the Raft distributed consensus algorithm"""
    def __init__(self, name, config, q):
        server_states = ["Follower", "Candidate", "Leader"]
        self.status = server_states[0] #always start as a follower
        self.id = name
        self.q = q
        self.config = config
        self.provisional_log = [] #deque maxlen
        self.log = []
        self.term = 0
        self.match_index = 0
        self.term_limit = randint(5,10)
        self.time_contacts = [time()]
        self.leader_address = None
        self.votes = 0
        self.voters = []
        self.voted_for = {}
        self.responses = {}
        self.match_index = 0
        self.alive = True

    async def __call__(self):
        while self.alive:
            if self.status == "Leader":
                await self.heartbeat()
                await curio.sleep(0)
        #     if self.time_contacts[-1] + self.term_limit == time():
        #         self.start_election()
        #         self.time_contacts.append(time())

            message = await self.q.get()
            logger.debug("{} got {}".format(self.id, type(message)))

            if self.id not in message.recipient_id:
                logger.debug("{} is putting {}".format(self.id, type(message)))
                await self.q.put(message)
                await self.q.join()

            else:
                await self.process_message(message)
                # logger.debug("{} is processing {}".format(self.id, type(message)))
                logger.debug("QSIZE {}".format(self.q.qsize()))
                await curio.sleep(2)
            await self.q.task_done()


    async def send(self, message):
        logger.debug("{} is sending {}".format(self.id, type(message)))
        await self.q.put(message)
        await self.q.join()

    async def process_message(self, message):
        logger.debug("{} is processing {}".format(self.id, type(message)))
        if isinstance(message, ClientEntry) and self.status == "Leader":
            await self.update_log()

        else:
            # message = self.create_message(AddEntry, "Client", leader_id = self.leader_address, success=False) #reject
            # await self.send(message)
            curio.sleep(0)
            pass
            return
        if not message.sender_id == self.id:
            self.time_contacts.append(time())
        if message.term < self.term:
            #do something here or just ignore?
            pass
            return
        if message.term > self.term:
            self.term = message.term
            if self.status == "Leader" or self.status =="Candidate":
                self.become_follower()


        # self.message_table = {
        # ("Leader", ClientEntry): await self.update_log(message),
        # ("Leader" ,Terminate): self.terminate(),
        # ("Leader", Response): await self.gather_responses(message),
        # ("Follower", AddEntry): await self.add_data(message),
        # ("Follower", CommitEntry): await self.commit_data(message),
        # ("Follower", ClientEntry): await self.reject(),
        # ("Follower", Terminate):self.terminate(),
        # ("Follower", Heartbeat):await self.heartbeat(),
        # ("Candidate", Terminate):self.terminate()
        # }

        # try:
        #     self.message_table[(self.status, type(message))]
        # except KeyError:
        #     self.ignore(message)

    def ignore(self, message):
        logger.debug("{} ignored {}".format(self.id, type(message)))
        pass

    def terminate(self):
        self.alive = False
        logger.debug ("{} log: {}".format(self.id, self.log))
        logger.debug("{} terminated".format(self.id))

    async def heartbeat(self):
        for recipient in self.config:
            message = self.create_message(AddEntry,recipient,commit=None, success=None, new_log_entry = None)
            await self.send(message)

    async def reject(self):
        for message in self.create_messages(RejectEntry):
            await self.send(message)

    def create_message(
            self,
            message_type,
            recipient,
            commit=None,
            success=None,
            new_log_entry = None,
            vote=None,
            leader_id = None
            ):
        try:
            self.new_log_entry = self.log[-1]
        except IndexError:
            self.new_log_entry = None
        try:
            self.last_log_entry = self.log[-2]
        except IndexError:
            self.last_log_entry = None
        try:
            self.last_log_index = self.log.index(self.log[-1])
        except IndexError:
            self.last_log_index = None

        if message_type == AddEntry:
            message = AddEntry(
            status = self.status,
            sender_id = self.id,
            recipient_id=recipient,
            term = self.term,
            commit = None,
            success = None,
            leader_id = None,
            last_log_index = self.last_log_index,
            last_log_entry = self.last_log_entry,
            new_log_entry = self.new_log_entry)


        if message_type == Vote:
            message = Vote(
            status = self.status,
            sender_id = self.id,
            sender_log_length = len(self.log),
            recipient_id=recipient,
            term = self.term,
            vote=None)

        elif message_type == Terminate:
            message = Terminate(
            status = self.status,
            sender_id = self.id,
            recipient_id = recipient,
            term = self.term,
            )

        # logger.debug("{} created message {}".format(self.id, type(message)))
        logger.debug("{} create_messages is returning {}".format(self.id, message))
        return message


    async def add_data(self, message):
        self.provisional_log.append(message.new_log_entry)
        logger.debug("{} appended {} to provisional log".format(self.id, self.provisional_log[-1]))
        for message in self.create_messages(Response):
            await self.send(message)

    async def gather_responses(self, message):
        logger.debug("{} is gathering responses {}".format(self.id, type(message)))
        if message.provisional == self.log[-1]:
            self.responses[message.sender_id] = 1
            if len(self.responses) >= (len(self.config)//2)+1:
                for message in self.create_messages(CommitEntry):
                    await self.send(message)
                logger.debug("received response from {}".format(message.sender_id))
        else:
            pass
            #TODO: reject entry

    def commit_data(self,data):
        try:
            self.log.append(self.provisional_log.pop())
            logger.debug("{} appending to log {}".format(self.id, self.provisional_log.pop()))
        except IndexError:
            logger.debug("{} had no provisional log entry".format(self.id))
        self.provisional_log = []
        self.commit_index += 1
        self.match_index += 1

    async def update_log(self, data):
        self.log.append(data.log_entry)
        logger.debug("{} is updating logs {}".format(self.id, self.log))
        for recipient in self.config:
            message = self.create_message(AddEntry, commit=False, new_log_entry=self.log[-1])
            await self.send(message)

    def become_leader(self):
        self.leader_address = self.id
        self.status = 'Leader'

    def become_follower(self):
        self.time_contacts.append(time())
        self.status = 'Follower'
        self.votes = 0

    def start_election(self):
        self.status = "Candidate"
        self.votes += 1
        self.voted_for[self.term] = self.id
        self.term += 1
        self.time_contacts.append(time())
        #send vote requests
        print(self.status, self.votes)
        if self.votes >= (len(self.config)//2)+1:
            self.become_leader()

    def vote_response(self, message):
        if self.term in self.voted_for:
            self.create_messages(VoteNo) #create vote yes and no messages
            #send msg

        else:
            self.create_messages(VoteYes)
            self.voted_for[self.term] = message.sender_id
            #send msg


    def show_logs(self):
        print ("status: ", self.status)
        print("log: ", self.log)
        print("provisional log: ", self.provisional_log)
        print("time contacts: ", self.time_contacts)

class RaftClient:
    def __init__(self, config, q):
        self.status = "Client"
        self.id = "Client"
        self.log = []
        self.leader_address = (None, None)
        self.leader = None
        self.config = config
        self.q = q

    async def __call__(self):
        for recipient in self.config:
            message = self.create_message(recipient)
            await self.q.put(message)
            logger.debug("{} put {}".format(self.id, type(message)))
        await self.q.task_done()


    async def recieve(self, message):
        if self.id not in message.recipient_id:
            await self.q.put(message)
            await self.q.join()
                        # logger.debug("{} returned {}".format(self.id, type(message)))
        else:
            self.process_message(message)
            # logger.debug("{} got {}".format(self.id, type(message)))

    def create_message(self, recipient):
        self.log.append(randint(1,100))
        message = ClientEntry(
        message_uuid = uuid4(),
        sender_id=self.id,
        recipient_id = recipient,
        log_entry = self.log[-1],
        status=self.status)
        return message

    def process_message(self, message):
        if isinstance(message, AddEntry):
            self.leader_address = message.leader_id

    def show_logs(self):
        print("Client log: ", self.log)

async def main():
    config = ["Alice", "Bob"]
    q = curio.Queue()
    alice = RaftNode("Alice", config, q)
    alice.status = "Leader"
    bob = RaftNode("Bob", config, q)
    client = RaftClient(config, q)
    tasks = []
    nodes = [client, bob, alice]
    for n in nodes:
        t = await curio.spawn(n)
        tasks.append(t)

    for t in tasks:
        await t.join()

if __name__=='__main__':
    curio.run(main, with_monitor=True)
