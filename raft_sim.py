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
        #creates single messages
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
            #reject entry

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


    #if not voted this term, have not already voted for server, vote
    # else deny vote
    # voting as bool vs check term number


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
        # print(self.id)

    async def __call__(self):
        while True:
            for recipient in self.config:
                message = self.create_message(recipient)
                await self.q.put(message)
                logger.debug("{} put {}".format(self.id, type(message)))
            await self.q.task_done()
            await curio.sleep(7)

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
    #check this against prod_cons.py
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


"""
#--------------------------------------------------------------

# cf error messages in raft.go

# only 2 forms of communication for basic algo - request vote and append entries - servers retry requests if they do not recieve timely replies
# Heartbeats are append entries requests with no data
# observer object
# client object
# logging

# match index - index of highest known match on leader

# find peers - adding/removing peers can only be run on the leader (must fail otherwise); if leader is removed it triggers a new election
# channel
#leaders inform peers when elected and when they become followers (cf split election)
# followers track last time of contact from a leader

# if no heartbeat/haven't heard from a leader in time, revert to candidate state
# when leader steps down - cf cleanup steps in go - reset last contact time
# if a client sends data to a follower, follower redirects request to last leader they heard from

# all communications between servers have term numbers included
# if a server recieves a message with a larger term number, it updates its own term number
#if a leader or candidate recieves a message with a larger term number it
reverts to a follower
# servers reject requests with stale term numbers (smaller than current)

#requests for votes are sent in parallel (as are heartbeats/add log entries)
#data cannot be sent during election (no leader)

#VOTING
# new leader is chosen when existing leader fails - leaders typically operate until they fail
# if a follower has not heard from a leader for the election timeout, it
# increments its term, becomes a candidate and initiates an election
# if awaiting votes, a candidate recieves an append entries message from a server with a term as high as or higher than its own, it reverts to follower status
# if term number is lower than its own, rejects append entries message and continues election
# Raft determines which of two logs is more up-to-date
# by comparing the index and term of the last entries in the
# logs. If the logs have last entries with different terms, then
# the log with the later term is more up-to-date. If the logs
# end with the same term, then whichever log is longer is
# more up-to-date.
#if a server recieves a request vote message from another server but the minimum election timeout for the current leader has not passed since the last heartbeat, it does not change its term or send its vote

#LOGGING
# leader sends commit logs message when a majority of the servers have acknowledged they have added the data to their logs
# logs are applied in order
# When sending # an AppendEntries RPC, the leader includes the index
# and term of the entry in its log that immediately precedes
# the new entries. If the follower does not find an entry in
# its log with the same index and term, then it refuses the
# new entries
# logs only flow from leader to follower (leaders never delete logs)

# To bring a follower’s log into consistency with its own,
# the leader must find the latest log entry where the two
# logs agree, delete any entries in the follower’s log after
# that point, and send the follower all of the leader’s entries
# after that point.

# The leader maintains a nextIndex for each follower,
# which is the index of the next log entry the leader will
# send to that follower. When a leader first comes to power,
# it initializes all nextIndex values to the index just after the
# last one in its log (11 in Figure 7). If a follower’s log is
# inconsistent with the leader’s, the AppendEntries consistency
# check will fail in the next AppendEntries RPC. After
# a rejection, the leader decrements nextIndex and retries
# the AppendEntries RPC. Eventually nextIndex will reach
# a point where the leader and follower logs match. When
# this happens, AppendEntries will succeed, which removes
# any conflicting entries in the follower’s log

# leader never changes its log if follower's log is inconsistent
#leaders commit a no-op message at the start of their terms to confirm that followers have the latest logs committed
# leaders must exchange heartbeats with majority of servers to confirm they are still leader before responding to a read request

# // Stats is used to return a map of various internal stats. This
# // should only be used for informative purposes or debugging.
# //
# // Keys are: "state", "term", "last_log_index", "last_log_term",
# // "commit_index", "applied_index", "fsm_pending",
# // "last_snapshot_index", "last_snapshot_term", "num_peers" and
# // "last_contact".

#client - chooses random server on startup, if server is not leader, it provides network address of last leader it heard from
# if no leader, client retries
#client commands have unique serial numbers
# if serial no is not confirmed, client retries
# if client re-sends a committed serial number, leader can confirm without contacting other servers
"""
