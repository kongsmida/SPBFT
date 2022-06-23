package consensus

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type State struct {
	ViewID         int64
	MsgLogs        *MsgLogs
	LastSequenceID int64
	CurrentStage   Stage
}

type MsgLogs struct {
	SingletxMsg         *SingletxMsg
	CrosstxMsg          *CrosstxMsg
	ProposeMsg          *ProposeMsg
	PrePrepareMsgs      map[string]*VoteMsg
	CommitMsgs          map[string]*VoteMsg
	CommitLocalMsgs     map[string]*CommitLocalMsg
	CommitFinalMsgs     map[string]*VoteMsg
	PreCommitMsgs       map[string]*VoteMsg
	ProofofProposalMsgs map[string]*VoteMsg
}

type Stage int

const (
	Idle        Stage = iota // Node is created successfully, but the consensus process is not started yet.
	PrePrepared              // The ReqMsgs is processed successfully. The node is ready to head to the Prepare stage.
	// Prepared
	PreCommitted
	CommittedLocal
	CommittedFinal
	Committed // Same with `committed-local` stage explained in the original paper.
	Proposed
)

// f: # of Byzantine faulty node  u: Byzantine nodes in AS
// f = (n­1) / 3
// n = 4, in this case.
const (
	f = 1
	u = 1
)

// lastSequenceID will be -1 if there is no last sequence ID.
func CreateState(viewID int64, lastSequenceID int64) *State {
	return &State{
		ViewID: viewID,
		MsgLogs: &MsgLogs{
			SingletxMsg:         nil,
			CrosstxMsg:          nil,
			ProposeMsg:          nil,
			CommitMsgs:          make(map[string]*VoteMsg, 0),
			CommitLocalMsgs:     make(map[string]*CommitLocalMsg, 0),
			CommitFinalMsgs:     make(map[string]*VoteMsg, 0),
			PreCommitMsgs:       make(map[string]*VoteMsg, 0),
			ProofofProposalMsgs: make(map[string]*VoteMsg, 0),
		},
		LastSequenceID: lastSequenceID,
		CurrentStage:   Idle,
	}
}

func (state *State) StartConsensus(request *SingletxMsg) (*PrePrepareMsg, error) {
	// `sequenceID` will be the index of this message.
	sequenceID := time.Now().UnixNano()

	// Find the unique and largest number for the sequence ID
	if state.LastSequenceID != -1 {
		for state.LastSequenceID >= sequenceID {
			sequenceID += 1
		}
	}

	// Assign a new sequence ID to the request message object.
	request.SequenceID = sequenceID

	// Save ReqMsgs to its logs.
	state.MsgLogs.SingletxMsg = request

	// Get the digest of the request message
	digest, err := digest(request)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	// Change the stage to pre-prepared.
	state.CurrentStage = PrePrepared

	return &PrePrepareMsg{
		ViewID:     state.ViewID,
		SequenceID: sequenceID,
		Digest:     digest,
		RequestMsg: request,
	}, nil
}
func (state *State) StartConsensusForAS(request *CrosstxMsg) (*ProposeMsg, error) {
	sequenceID := time.Now().UnixNano()

	if state.LastSequenceID != -1 {
		for state.LastSequenceID >= sequenceID {
			sequenceID += 1
		}
	}
	nodeID := request.ClientID
	subtx := request.Operation

	request.SequenceID = sequenceID

	state.MsgLogs.CrosstxMsg = request

	state.CurrentStage = Proposed

	return &ProposeMsg{
		ViewID:     state.ViewID,
		SequenceID: sequenceID,
		Subtx:      subtx,
		NodeID:     nodeID,
	}, nil
}

func (state *State) PrePrepare(prePrepareMsg *PrePrepareMsg) (*VoteMsg, error) {
	// Get ReqMsgs and save it to its logs like the primary.
	state.MsgLogs.SingletxMsg = prePrepareMsg.RequestMsg

	// Verify if v, n(a.k.a. sequenceID), d are correct.
	if !state.verifyMsg(prePrepareMsg.ViewID, prePrepareMsg.SequenceID, prePrepareMsg.Digest) {
		return nil, errors.New("pre-prepare message is corrupted")
	}

	// Change the stage to pre-prepared.
	state.CurrentStage = PrePrepared

	return &VoteMsg{
		ViewID:     state.ViewID,
		SequenceID: prePrepareMsg.SequenceID,
		Digest:     prePrepareMsg.Digest,
		MsgType:    PreCommitMsg,
	}, nil
}

func (state *State) PreCommit(preCommitMsg *VoteMsg) (*VoteMsg, error) {
	if !state.verifyMsg(preCommitMsg.ViewID, preCommitMsg.SequenceID, preCommitMsg.Digest) {
		return nil, errors.New("prepare message is corrupted")
	}

	// Append msg to its logs
	state.MsgLogs.PreCommitMsgs[preCommitMsg.NodeID] = preCommitMsg

	// Print current voting status
	fmt.Printf("[Prepare-Vote]: %d\n", len(state.MsgLogs.PreCommitMsgs))

	if state.preCommitted() {
		// Change the stage to prepared.
		state.CurrentStage = PreCommitted

		return &VoteMsg{
			ViewID:     state.ViewID,
			SequenceID: preCommitMsg.SequenceID,
			Digest:     preCommitMsg.Digest,
			MsgType:    CommitMsg,
		}, nil
	}

	return nil, nil
}

func (state *State) Commit(commitMsg *VoteMsg) (*ReplyMsg, *SingletxMsg, error) {
	if !state.verifyMsg(commitMsg.ViewID, commitMsg.SequenceID, commitMsg.Digest) {
		return nil, nil, errors.New("commit message is corrupted")
	}

	// Append msg to its logs
	state.MsgLogs.CommitMsgs[commitMsg.NodeID] = commitMsg

	// Print current voting status
	fmt.Printf("[Commit-Vote]: %d\n", len(state.MsgLogs.CommitMsgs))

	if state.committed() {
		// This node executes the requested operation locally and gets the result.
		result := "Executed"

		// Change the stage to prepared.
		state.CurrentStage = Committed

		return &ReplyMsg{
			ViewID:    state.ViewID,
			Timestamp: state.MsgLogs.SingletxMsg.Timestamp,
			ClientID:  state.MsgLogs.SingletxMsg.ClientID,
			Result:    result,
		}, state.MsgLogs.SingletxMsg, nil
	}

	return nil, nil, nil
}

func (state *State) verifyMsg(viewID int64, sequenceID int64, digestGot string) bool {
	// Wrong view. That is, wrong configurations of peers to start the consensus.
	if state.ViewID != viewID {
		return false
	}

	// Check if the Primary sent fault sequence number. => Faulty primary.
	// TODO: adopt upper/lower bound check.
	if state.LastSequenceID != -1 {
		if state.LastSequenceID >= sequenceID {
			return false
		}
	}

	digest, err := digest(state.MsgLogs.SingletxMsg)
	if err != nil {
		fmt.Println(err)
		return false
	}

	// Check digest.
	if digestGot != digest {
		return false
	}

	return true
}

func (state *State) proposed() bool {
	if state.MsgLogs.CrosstxMsg == nil {
		return false
	}

	if len(state.MsgLogs.ProofofProposalMsgs) < 2*f {
		return false
	}
	return true
}

func (state *State) preCommitted() bool {
	if state.MsgLogs.SingletxMsg == nil {
		return false
	}

	if len(state.MsgLogs.PrePrepareMsgs) < 2*f {
		return false
	}

	return true
}

func (state *State) CommittedLocal() bool {
	if !state.preCommitted() {
		return false
	}

	if len(state.MsgLogs.CommitLocalMsgs) < 2*f {
		return false
	}
	return true
}

func (state *State) CommittedFinal() bool {
	if !state.proposed() {
		return false
	}

	if len(state.MsgLogs.CommitFinalMsgs) < 2*f {
		return false
	}
	return true
}

func (state *State) committed() bool {
	if !state.CommittedLocal() {
		return false
	}

	if len(state.MsgLogs.CommitMsgs) < 2*f {
		return false
	}

	return true
}

func digest(object interface{}) (string, error) {
	msg, err := json.Marshal(object)

	if err != nil {
		return "", err
	}

	return Hash(msg), nil
}
