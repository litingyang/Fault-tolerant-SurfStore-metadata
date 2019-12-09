from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
from socketserver import ThreadingMixIn
import hashlib
import argparse
import datetime, threading
import time
from time import sleep
import random
from threading import Timer
import sys, os



#state 0 : crash, 1: follower, 2:candidate, 3:leaser
_state = 1


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

# A simple ping, returns true
def ping():
    """A simple ping method"""
    print("Ping()")
    return True

# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    print(h)
    blockData = Blocks[h]
    print(blockData)
    return blockData

# Puts a block
def putblock(h, b):
    """Puts a block"""
    print("PutBlock()")
    print(h)
    print(b)
    print(type(b))
    Blocks[h] = b
    server_hashlist.append(h)
    return True

# Given a list of blocks, return the subset that are on this server
def hasblocks(client_hashlist):
    """Determines which blocks are on this server"""
    print("HasBlocks()")
    intersection_list = [value for value in client_hashlist if value in server_hashlist]
    return intersection_list


# Retrieves the server's FileInfoMap
# [NEW] Returns metadata from the filesystem
# [NEW] If the node is the leader, and if a majority of the nodes are working, should return the correct answer
# [NEW] If a majority of the nodes are crashed, should block until a majority recover
# [NEW] If not the leader, should indicate an error back to the client
def getfileinfomap():
    global _isFollower
    global _isCrash
    global _isCandidate
    global _isLeader
    global currentTerm
    global _state
    global voteFor
    global myTimer
    global s_connect
    global servernum
    global log
    global matchIndex
    global nextIndex
    global commitIndex
    global serverlist


    """Gets the fileinfo map"""
    print("GetFileInfoMap()")
    print("======== FILEMAP ========")
    for f in nameToVersion.keys():
        print("===== File : {0} =====".format(f))
        print("Version : {0}".format(nameToVersion[f]))
        print("Hashs : {0}".format(nameToHashs[f]))
    return nameToVersion, nameToHashs


# Update a file's fileinfo entry
# [NEW] Updates a file’s metadata
# [NEW] If the node is the leader, and if a majority of the nodes are working, should return the correct answer
# [NEW] If a majority of the nodes are crashed, should block until a majority recover
# [NEW] If not the leader, should indicate an error back to the client
def updatefile(filename, version, hashlist):
    global _isFollower
    global _isCrash
    global _isCandidate
    global _isLeader
    global currentTerm
    global _state
    global voteFor
    global myTimer
    global s_connect
    global servernum
    global log
    global matchIndex
    global nextIndex
    global commitIndex
    global serverlist
    global nameToVersion

    if _isCrash:
        raise Exception('Server is crashed.')
    elif _isFollower:
        raise Exception('Server is not a leader.')
    elif _isCandidate:
        raise Exception('Server is not a leader.')
    elif _isLeader:
        commit_ct = 0
        newLog = (currentTerm, filename, version)
        log.append(newLog)
        while(commit_ct <= maxnum//2):
            for i in range(len(s_connect)):
                if i != servernum:
                    s_connect[i] = xmlrpc.client.ServerProxy('http://' + serverlist[i])

            for i in range(len(s_connect)):
                if i == servernum:
                    continue
                s = s_connect[i]
                print("AppendEntries to ", s)
                prevLogIndex = matchIndex[i]
                prevLogTerm = log[prevLogIndex][0]
                print("prevLogIndex: ", prevLogIndex)
                print("prevLogTerm: ", prevLogTerm)
                t, midx, success, c = s.surfstore.appendEntries(currentTerm, servernum, prevLogIndex, prevLogTerm, [newLog], commitIndex)

                # return value: term, matchIndex, successOrNot
                print("Append Return | term: ", t, " matchIndex: ", midx, " Success: ", s)
                if not c:
                    matchIndex[i] = midx
                    nextIndex[i] = matchIndex[i]
                    if(success):
                        commit_ct += 1
                
                # [TODO] over majority --> commit
                
                if commit_ct > maxnum//2:
                    commitIndex += 1
                    nameToVersion[filename] = version
                    print("Commit")
                    return True
            commit_ct = 0       
        pass
    """Updates a file's fileinfo entry"""
    print("UpdateFile()")
    nameToVersion[filename] = version
    nameToHashs[filename] = hashlist

    print("===== Update File : {0} =====".format(filename))
    print("Version : {0}".format(nameToVersion[filename]))
    print("Hashs : {0}".format(nameToHashs[filename]))
    return True






# PROJECT 3 APIs below

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    global _isLeader
    """Is this metadata store a leader?"""
    print("IsLeader()")
    return _isLeader

# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    global _isFollower
    global _isCrash
    global _isCandidate
    global _isLeader
    global currentTerm
    global _state
    global voteFor
    global myTimer
    global s_connect
    global servernum
    global log
    global matchIndex
    global nextIndex
    global commitIndex
    global serverlist


    """Crashes this metadata store"""
    print("Crash()")
    _isCrash = True
    _isFollower = False
    _isCandidate = False
    _isLeader = False
    _state = 0
    myTimer.cancel()
    return True

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    global _isFollower
    global _isCrash
    global _isCandidate
    global _isLeader
    global currentTerm
    global _state
    global voteFor
    global myTimer
    global s_connect
    global servernum
    global log
    global matchIndex
    global nextIndex
    global commitIndex
    global serverlist


    if _isCrash:
        _isCrash = False
        _isFollower = True
        _isCandidate = False
        _isLeader = False
        _state = 1
        print("servernum:",servernum, "state","crash to follower")
        myTimer = Timer(E_interval,election_timeout)
        myTimer.start()
    elif _isFollower:
        pass
    elif _isCandidate:
        pass
    elif _isLeader:
        pass
    """Restores this metadata store"""
    print("Restore()")
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    global _isCrash
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    return _isCrash


# [NEW]
# Replicates log entries
# Serves as a heartbeat mechanism
# Should return an “isCrashed” error
# Procedure has no effect if server is crashed
def appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
    global _isFollower
    global _isCrash
    global _isCandidate
    global _isLeader
    global currentTerm
    global _state
    global voteFor
    global myTimer
    global s_connect
    global servernum
    global log
    global matchIndex
    global nextIndex
    global commitIndex
    global serverlist

    if _isCrash:
        print("I'm crashed. Don't appendEntries to:", servernum)
        return -1, -1, False, _isCrash
    elif _isFollower:
        print("Follower ",servernum,"receive a heartbeat")
        if entries == []:
            
            myTimer.cancel()
            myTimer = Timer(E_interval,election_timeout)
            myTimer.start()

            # [TODO] update term if needed
            if term > currentTerm:
                currentTerm = term
                voteFor = -1

            if leaderCommit > commitIndex:
                # Update FileInfoMap
                for c in range(commitIndex, leaderCommit):
                    filename = log[c][1]
                    version = log[c][2]
                    nameToVersion[filename] = version
                commitIndex = min(leaderCommit, len(log)-1)

            return currentTerm, len(log)-1, False, _isCrash

        # Reply false if term < currentTerm
        if term < currentTerm: 
            print("False 1")  
            return currentTerm, len(log)-1, False, _isCrash

        # Reply false if log doesn’t contain an entry at prevLogIndex 
        if len(log)-1 < prevLogIndex:
            print("[]", log)
            print("[]", prevLogIndex)
            print("False 2")
            return currentTerm,len(log)-1, False, _isCrash

        # Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        if log[prevLogIndex][0] != prevLogTerm:
            print("[]", log[prevLogIndex][0])
            print("[]", prevLogTerm)
            print("False 3")
            return currentTerm,len(log)-1, False, _isCrash
        
        # If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
        print("Old log: ", log)
        log = log[:prevLogIndex+1]
        print("Clean log: ", log)
        
        # Append any new entries not already in the log
        log.extend(entries)
        print("New log: ", log)
        

        # If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if leaderCommit > commitIndex:
            # Update FileInfoMap
            for c in range(commitIndex, leaderCommit):
                filename = log[c][1]
                version = log[c][2]
                nameToVersion[filename] = version
            commitIndex = min(leaderCommit, len(log)-1)
        
        print("True")
        return currentTerm,len(log)-1, True, _isCrash

    elif _isCandidate:
        print("Candidate ",servernum,"receive a heartbeat")
        if term >= currentTerm:
            currentTerm = term
            voteFor = -1
            _isCandidate = False
            _isFollower = True
            _isLeader = False
            _state = 1
            print("servernum:",servernum,"candidate to follower")

    elif _isLeader:
        print("Leader ",servernum,"receive a heartbeat")
        if term >= currentTerm:
            currentTerm = term
            voteFor = -1
            _isCandidate = False
            _isFollower = True
            _isLeader = False
            _state = 1
            print("servernum:",servernum,"leader to follower")
    
    return currentTerm, len(log), False, _isCrash


# [NEW]
# Used to implement leader election
# Should return an “isCrashed” error
# Procedure has no effect if server is crashed
def requestVote(term, candidateId, lastLogIndex, lastLogTerm):

    global _isFollower
    global _isCrash
    global _isCandidate
    global _isLeader
    global currentTerm
    global _state
    global voteFor
    global myTimer
    global s_connect
    global servernum
    global log
    global matchIndex
    global nextIndex
    global commitIndex
    global serverlist


    if _isCrash:
        print("I'm crashed. Don't requestvote to:", servernum)
        return -1 , False, _isCrash
    elif _isFollower:
        print("======= Vote Request to Follower =======")
        print("Candidate term: ", term)
        print("Candidate id:   ", candidateId)
        print("Current term:   ", currentTerm)
        print("Last log index: ", lastLogIndex)
        print("Last log term:  ", lastLogTerm)
        if term < currentTerm:
            return currentTerm, False, _isCrash
        
        if term > currentTerm:
            currentTerm = term
            voteFor = -1
            
        if voteFor == -1 or candidateId == voteFor:
            if lastLogTerm > log[-1][0]:
                voteFor = candidateId
                return currentTerm, True, _isCrash
            
            elif lastLogTerm == log[-1][0] and lastLogIndex >= len(log):
                voteFor = candidateId
                return currentTerm, True, _isCrash

            else: #not up to date
                return currentTerm, False, _isCrash

    elif _isCandidate:
        print("======= Vote Request to Candidate ======= candidatenum:",servernum)
        print("Candidate term: ", term)
        print("Candidate id:   ", candidateId)
        print("Current term:   ", currentTerm)
        print("Last log index: ", lastLogIndex)
        print("Last log term:  ", lastLogTerm)
        if term < currentTerm:
            return currentTerm, False, _isCrash
        
        if term > currentTerm:
            currentTerm = term
            voteFor = -1
            _isCandidate = False
            _isFollower = True
            _isLeader = False
            _state = 1
            print("servernum:",servernum,"candidate to follower")
            
        if voteFor == -1 or candidateId == voteFor:
            if lastLogTerm > log[-1][0]:
                voteFor = candidateId
                return currentTerm, True, _isCrash

            elif lastLogTerm == log[-1][0] and lastLogIndex >= len(log):
                voteFor = candidateId
                return currentTerm, True, _isCrash

            else: #not up to date
                return currentTerm, False, _isCrash
        else:
            return currentTerm, False, _isCrash
    elif _isLeader:
        print("======= Vote Request to Leader =======, leadernum:",servernum)
        print("Candidate term: ", term)
        print("Candidate id:   ", candidateId)
        print("Current term:   ", currentTerm)
        print("Last log index: ", lastLogIndex)
        print("Last log term:  ", lastLogTerm)
        if term < currentTerm:
            return currentTerm, False, _isCrash
        
        if term > currentTerm:
            currentTerm = term
            voteFor = -1
            _isCandidate = False
            _isFollower = True
            _isLeader = False
            _state = 1
            print("servernum:",servernum,"leader to follower")
            
        if voteFor == -1 or candidateId == voteFor:
            if lastLogTerm > log[-1][0]:
                voteFor = candidateId
                return currentTerm, True, _isCrash

            elif lastLogTerm == log[-1][0] and lastLogIndex >= len(log):
                voteFor = candidateId
                return currentTerm, True, _isCrash

            else: #not up to date
                return currentTerm, False, _isCrash
        else:
            return currentTerm, False, _isCrash

# [NEW] 
# Returns the version of the given file, even when the server is crashed
def tester_getversion(filename):
    global _isFollower
    global _isCrash
    global _isCandidate
    global _isLeader
    global currentTerm
    global _state
    global voteFor
    global myTimer
    global s_connect
    global servernum
    global log
    global matchIndex
    global nextIndex
    global commitIndex
    global serverlist 
    global nameToVersion

    if _isCrash:
        print("I'm crashed, don't ask me file version")
        if filename in nameToVersion.keys():
            return nameToVersion[filename]
        else:
            return 0
    elif _isFollower:
        print("I'm a follower, don't ask me file version")
        if filename in nameToVersion.keys():
            return nameToVersion[filename]
        else:
            return 0
    elif _isCandidate:
        if filename in nameToVersion.keys():
            return nameToVersion[filename]
        else:
            return 0
    elif _isLeader:
        
        if filename in nameToVersion.keys():
            print("I'm a leader. The file version is ", nameToVersion[filename])
            return nameToVersion[filename]
        else:
            return 0

# [NEW]
# Reads the config file and return host, port and store list of other servers
def readconfig(config, servernum, serverlist):
    """Reads cofig file"""
    fd = open(config, 'r')
    l = fd.readline()

    maxnum = int(l.strip().split(' ')[1])

    if servernum >= maxnum or servernum < 0:
        raise Exception('Server number out of range.')

    d = fd.read()
    d = d.splitlines()

    for i in range(len(d)):
        hostport = d[i].strip().split(' ')[1]
        if i == servernum:
            host = hostport.split(':')[0]
            port = int(hostport.split(':')[1])

        serverlist.append(hostport)
        print("serverlist:",serverlist)
    return maxnum, host, port


#not rpc function
def election_timeout():
    
    global _isFollower
    global _isCrash
    global _isCandidate
    global _isLeader
    global currentTerm
    global _state
    global voteFor
    global myTimer
    global s_connect
    global servernum
    global log
    global matchIndex
    global nextIndex
    global commitIndex
    global serverlist

    print("election_timeout")
    print("myterm",currentTerm)
    
    
    if _isCrash:
        pass
    elif _isFollower:
        
        _isCandidate = True
        _isFollower = False
        _state = 2
        print("servernum:",servernum,"follower to candidate")
        currentTerm += 1
        print("currentTerm:", currentTerm)
        myTimer = Timer(V_interval,vote_timeout)
        myTimer.start()
        voteFor = servernum
        vote_count = 1
        print(s_connect)
        for i in range(len(s_connect)):
            if i != servernum:
                s_connect[i] = xmlrpc.client.ServerProxy('http://' + serverlist[i])

        for i in range(len(s_connect)):
            if i!= servernum:
                s = s_connect[i]
                print("RequestVote to ", s)
                term, success, c = s.surfstore.requestVote(currentTerm, servernum, len(log), log[-1][0])
                print("Receive vote from ", s, "is ", success)
                if(success):
                    vote_count += 1
        if vote_count > maxnum//2:
            #change to leader
            _isCandidate = False
            _isLeader = True
            _state = 3
            print("servernum:",servernum,"candidate to leader")
            myTimer = Timer(H_interval,heartbeatTimeout)
            myTimer.start()
            


def vote_timeout():

    global _isFollower
    global _isCrash
    global _isCandidate
    global _isLeader
    global currentTerm
    global _state
    global voteFor
    global myTimer
    global s_connect
    global servernum
    global log
    global matchIndex
    global nextIndex
    global commitIndex
    global serverlist

    if _isCrash:
        pass
    elif _isCandidate:

        print("Vote timeout")
        print("I am ", servernum)
        currentTerm += 1
        myTimer = Timer(V_interval,vote_timeout)
        myTimer.start()
        voteFor = servernum
        vote_count = 1

        for i in range(len(s_connect)):
            if i != servernum:
                s_connect[i] = xmlrpc.client.ServerProxy('http://' + serverlist[i])

        for i in range(len(s_connect)):
            if i!= servernum:
                s = s_connect[i]
                print("RequestVote to ", s)
                term, success, c = s.surfstore.requestVote(currentTerm, servernum, len(log), log[-1][0])
                print("Receive vote from ", s, "is ", success)
                if(success):
                    vote_count += 1
        if vote_count > maxnum//2:
            #change to leader
            _isCandidate = False
            _isLeader = True
            _state = 2
            print("servernum:",servernum,"candidate to leader")
            myTimer = Timer(H_interval,heartbeatTimeout)
            myTimer.start()


def heartbeatTimeout():

    global _isFollower
    global _isCrash
    global _isCandidate
    global _isLeader
    global currentTerm
    global _state
    global voteFor
    global myTimer
    global s_connect
    global servernum
    global log
    global matchIndex
    global nextIndex
    global commitIndex
    global serverlist

    if _isCrash:
        pass
    elif _isLeader:

        myTimer = Timer(H_interval,heartbeatTimeout)
        myTimer.start()
        for i in range(len(s_connect)):
            if i != servernum:
                s_connect[i] = xmlrpc.client.ServerProxy('http://' + serverlist[i])

        for i in range(len(s_connect)):
            if i!= servernum:
                s = s_connect[i]
                print("appendEntries to ", s)
                #appendEntries(, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
                t, midx, success, c = s.surfstore.appendEntries(currentTerm,servernum, -1, -1, [], commitIndex)
                print("Heartbeat Return | term: ", t, " matchIndex: ", midx, " Success: ", s)
                if not c:
                    matchIndex[i] = midx
                    nextIndex[i] = matchIndex[i] + 1

    
if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="SurfStore server")
        parser.add_argument('config', help='path to config file')
        parser.add_argument('servernum', type=int, help='server number')

        args = parser.parse_args()

        config = args.config
        servernum = args.servernum
        '''
        global _isFollower
        global _isCrash
        global _isCandidate
        global _isLeader
        global currentTerm
        global _state
        global voteFor
        global myTimer
        global s_connect
        global servernum
        global log
        global matchIndex
        global nextIndex
        global commitIndex
        '''

        # server list has list of other servers
        serverlist = []

        # maxnum is maximum number of servers
        maxnum, host, port = readconfig(config, servernum, serverlist)
        print("all_sesrverlist:",serverlist)
        hashmap = dict()
        fileinfomap = dict()
        server_hashlist = []
       
        nameToHashs = dict()

        Blocks = dict()

        

        _state = 1
        _isCrash = False
        _isLeader = False
        _isFollower = True
        _isCandidate = False
        currentTerm = 1
        nameToVersion = dict()


        E_interval = (random.random()*400+600)/1000
        V_interval = (random.random()*400+600)/1000
        H_interval = 0.25

        

        currentTerm = 1
        voteFor = -1

        commitIndex = 0
        log = [[1, "", 0]]
        commitIndex = 0     # index of highest log entry known to be committed
        lastApplied = 0     # index of highest log entry applied to state machine 

        # Leader only
        nextIndex = []      # for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
        matchIndex = []     # for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
        nextIndex = [(len(log) + 1) for _ in range(maxnum)]
        matchIndex = [0 for _ in range(maxnum)]

        #sleep
        sleep(2)
        s_connect = [None for _ in serverlist]
        for i in range(len(s_connect)):
            if i != servernum:
                s_connect[i] = xmlrpc.client.ServerProxy('http://' + serverlist[i])
        my_connect = xmlrpc.client.ServerProxy('http://' + serverlist[servernum])
        print("conect:",s_connect)
        myTimer = Timer(E_interval,election_timeout)
        myTimer.start()


        print("Attempting to start XML-RPC Server...")
        print(host, port)
        server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping,"surfstore.ping")
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(updatefile,"surfstore.updatefile")
        # Project 3 APIs
        server.register_function(isLeader,"surfstore.isLeader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.isCrashed")
        server.register_function(requestVote,"surfstore.requestVote")
        server.register_function(appendEntries,"surfstore.appendEntries")
        server.register_function(tester_getversion,"surfstore.tester_getversion")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))