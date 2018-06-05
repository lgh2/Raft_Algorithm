#!/usr/bin/env python

"""
# Heartbeat
# commit entry
# Vote request
# Vote response yes/no
#add server state - leader, follower, candidate 

"""

#create raft-specific exceptions per go
from collections import namedtuple 

shared = 'status sender_id recipient_id term'

AddEntry = namedtuple("AddEntry",
"commit success last_log_index last_log_entry new_log_entry leader_id {}".format(shared)
)

Vote = namedtuple("Vote",
"vote log_length {}".format(shared)
)

Terminate = namedtuple("Terminate", "{}".format(shared))

ClientEntry = namedtuple("ClientEntry", "message_uuid sender_id recipient_id log_entry status")

