package network

import (
	"fmt"

	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
)

func LogMsg(msg interface{}) {
	switch msg.(type) {
	case *consensus.SingletxMsg:
		reqMsg := msg.(*consensus.SingletxMsg)
		fmt.Printf("[SINGLETX] ClientID: %s, Timestamp: %d, Operation: %s\n", reqMsg.ClientID, reqMsg.Timestamp, reqMsg.Operation)
	case *consensus.CrosstxMsg:
		crosstxMsg := msg.(*consensus.CrosstxMsg)
		fmt.Printf("[CROSSTX] ClientID: %s, Timestamp: %d, Operation: ", crosstxMsg.ClientID, crosstxMsg.Timestamp)
		fmt.Println(crosstxMsg.Operation)
	case *consensus.ProposeMsg:
		ProposeMsg := msg.(*consensus.ProposeMsg)
		subtx := ProposeMsg.Subtx
		fmt.Printf("[PROPOSE] SubTx: ")
		fmt.Print(subtx)
		fmt.Printf(", PrimaryID: %s\n", ProposeMsg.NodeID)

	case *consensus.PrePrepareMsg:
		prePrepareMsg := msg.(*consensus.PrePrepareMsg)
		fmt.Printf("[PREPREPARE] ClientID: %s, Operation: %s, SequenceID: %d\n", prePrepareMsg.RequestMsg.ClientID, prePrepareMsg.RequestMsg.Operation, prePrepareMsg.SequenceID)
	case *consensus.PreReplyMsg:
		preReplyMsg := msg.(*consensus.PreReplyMsg)
		fmt.Printf("[PREREPLY] SubTx: %s, NodeID: %s\n", preReplyMsg.Subtx, preReplyMsg.NodeID)
	case *consensus.VoteMsg:
		voteMsg := msg.(*consensus.VoteMsg)
		if voteMsg.MsgType == consensus.PreCommitMsg {
			fmt.Printf("[PRECOMMIT] NodeID: %s\n", voteMsg.NodeID)
		} else if voteMsg.MsgType == consensus.CommitMsg {
			fmt.Printf("[COMMIT] NodeID: %s\n", voteMsg.NodeID)
		} else if voteMsg.MsgType == consensus.CommitFinalMsg {
			fmt.Printf("[COMMIT-FINAL] NodeID: %s\n", voteMsg.NodeID)
		} else if voteMsg.MsgType == consensus.ProofofProposalMsg {
			fmt.Printf("[PROOFOFPROPOSAL] NodeID: %s\n", voteMsg.NodeID)
		}
	}
}

func LogStage(stage string, isDone bool) {
	if isDone {
		fmt.Printf("[STAGE-DONE] %s\n", stage)
	} else {
		fmt.Printf("[STAGE-BEGIN] %s\n", stage)
	}
}
