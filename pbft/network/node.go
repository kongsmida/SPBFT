package network

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bigpicturelabs/consensusPBFT/pbft/consensus"
)

type StateDB struct {
	Address          string
	Balance          float32
	PublicKey        string
	Proof_of_Primary string
}

type Node struct {
	NodeID        string
	ShardID       uint16 //加
	View          *View
	CurrentState  *consensus.State
	CommittedMsgs []*consensus.SingletxMsg // kinda block.
	MsgBuffer     *MsgBuffer               //一个包含所有类型消息的缓冲区
	MsgEntrance   chan interface{}
	MsgDelivery   chan interface{}
	Alarm         chan bool
	Balance       int         //加
	StateDB       *[4]StateDB //加
}

type Transaction struct { //加
	senders   []string
	receivers []string
	txType
}
type txType int

const (
	Singletx txType = 1
	Crosstx  txType = 0
)

type MsgBuffer struct {
	SingletxMsgs   []*consensus.SingletxMsg
	CrosstxMsgs    []*consensus.CrosstxMsg
	PrePrepareMsgs []*consensus.PrePrepareMsg
	PrepareMsgs    []*consensus.VoteMsg
	CommitMsgs     []*consensus.VoteMsg
	ProposeMsgs    []*consensus.ProposeMsg
	ProofMsgs      []*consensus.VoteMsg
	PreCommitMsgs  []*consensus.VoteMsg
}

type View struct {
	ID      int64
	Primary [4]string
}

var InitialStateDB_1 = [4]StateDB{{"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}}
var InitialStateDB_2 = [4]StateDB{{"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}}
var InitialStateDB_3 = [4]StateDB{{"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}}
var InitialStateDB_4 = [4]StateDB{{"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}}
var AssistantShard = [4]StateDB{{"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}, {"0000", 5.23, "xxxx", "prove"}}

const ResolvingTimeDuration = time.Millisecond * 1000 // 1 second.

func NewNode(nodeID string) *Node {
	const viewID = 0000 // temporary.
	var primary = [4]string{"0000", "0100", "1000", "1100"}
	var stateDB [4]StateDB
	str := nodeID[0:2]
	switch str {
	case "00":
		stateDB = InitialStateDB_1
	case "01":
		stateDB = InitialStateDB_2
	case "10":
		stateDB = InitialStateDB_3
	case "11":
		stateDB = InitialStateDB_4
	default:
		{
			fmt.Printf("找不到对应的分片\n")
		}
	}

	node := &Node{
		// Hard-coded for test.
		NodeID: nodeID,
		// NodeTable: map[string]string{
		// 	"Apple":  "localhost:1111",
		// 	"MS":     "localhost:1112",
		// 	"Google": "localhost:1113",
		// 	"IBM":    "localhost:1114",
		// },
		// NodeTable: map[string]int{
		// 	"0000": 1, "1000": 1,
		// 	"0001": 2, "1001": 2,
		// 	"0010": 3, "1010": 3,
		// 	"0011": 4, "1011": 4,
		// 	"0100": 1, "1100": 1,
		// 	"0101": 2, "1101": 2,
		// 	"0110": 3, "1110": 3,
		// 	"0111": 4, "1111": 4,
		// },

		View: &View{
			ID:      viewID,
			Primary: primary,
		},
		StateDB: &stateDB,

		// Consensus-related struct
		CurrentState:  nil,
		CommittedMsgs: make([]*consensus.SingletxMsg, 0), //make()创建map类型
		MsgBuffer: &MsgBuffer{
			SingletxMsgs:   make([]*consensus.SingletxMsg, 0),
			CrosstxMsgs:    make([]*consensus.CrosstxMsg, 0),
			PrePrepareMsgs: make([]*consensus.PrePrepareMsg, 0),
			PrepareMsgs:    make([]*consensus.VoteMsg, 0),
			CommitMsgs:     make([]*consensus.VoteMsg, 0),
			ProposeMsgs:    make([]*consensus.ProposeMsg, 0),
		},

		// Channels 代表并发管道
		MsgEntrance: make(chan interface{}),
		MsgDelivery: make(chan interface{}),
		Alarm:       make(chan bool),
	}

	// Start message dispatcher
	go node.dispatchMsg()

	// Start alarm trigger
	go node.alarmToDispatcher()

	// Start message resolver
	go node.resolveMsg()

	return node
}

func (node *Node) Broadcast(msg interface{}, path string) map[string]error {
	errorMap := make(map[string]error)

	for _, url := range AssistantShard {

		jsonMsg, err := json.Marshal(msg) //将msg编码为json格式存入jsonMsg
		if err != nil {
			errorMap[url.Address[2:4]] = err
			continue
		}

		// send(url+path, jsonMsg)

		send("localhost:"+url.Address+path, jsonMsg)
	}

	if len(errorMap) == 0 {
		return nil
	} else {
		return errorMap
	}
}

func (node *Node) Reply(msg *consensus.ReplyMsg) error { //只给主节点返回reply消息的json数据
	// Print all committed messages.
	for _, value := range node.CommittedMsgs {
		fmt.Printf("Committed value: %s, %d, %s, %d", value.ClientID, value.Timestamp, value.Operation, value.SequenceID)
	}
	fmt.Print("\n")

	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Client가 없으므로, 일단 Primary에게 보내는 걸로 처리.  这里要改成返回给client
	for i := range node.View.Primary {
		fmt.Println(i)
		send("localhost:"+"/reply", jsonMsg)
	}

	return nil
}

// GetReq can be called when the node's CurrentState is nil.
// Consensus start procedure for the Primary.
func (node *Node) GetReq(reqMsg *consensus.SingletxMsg) error {
	LogMsg(reqMsg) //打印消息

	// 在新一轮共识前，创建一个新的状态，即节点此时位于哪个视图、最新的一个requestID
	err := node.createStateForNewConsensus()
	if err != nil {
		return err
	}

	// 开始共识，创建一个pre-prepare消息，包含viewID，序列号，摘要，请求消息
	prePrepareMsg, err := node.CurrentState.StartConsensus(reqMsg)
	if err != nil {
		return err
	}

	LogStage(fmt.Sprintf("Consensus Process (ViewID:%d)", node.CurrentState.ViewID), false)

	// 广播pre-prepare消息并打印
	if prePrepareMsg != nil {
		node.Broadcast(prePrepareMsg, "/preprepare")
		LogStage("Pre-prepare", true)
	}

	return nil
}

// GetPrePrepare can be called when the node's CurrentState is nil.
// Consensus start procedure for normal participants.
func (node *Node) GetPrePrepare(prePrepareMsg *consensus.PrePrepareMsg) error {
	LogMsg(prePrepareMsg)

	// Create a new state for the new consensus.
	err := node.createStateForNewConsensus()
	if err != nil {
		return err
	}

	prePareMsg, err := node.CurrentState.PrePrepare(prePrepareMsg)
	if err != nil {
		return err
	}

	if prePareMsg != nil {
		// Attach node ID to the message
		prePareMsg.NodeID = node.NodeID

		LogStage("Pre-prepare", true)
		node.Broadcast(prePareMsg, "/prepare")
		LogStage("Prepare", false)
	}

	return nil
}

func (node *Node) GetPreCommit(preCommitMsg *consensus.VoteMsg) error {
	LogMsg(preCommitMsg)

	commitMsg, err := node.CurrentState.PreCommit(preCommitMsg)
	if err != nil {
		return err
	}

	if commitMsg != nil {
		// Attach node ID to the message
		commitMsg.NodeID = node.NodeID

		LogStage("Prepare", true)
		node.Broadcast(commitMsg, "/commit")
		LogStage("Commit", false)
	}

	return nil
}

func (node *Node) GetCommit(commitMsg *consensus.VoteMsg) error {
	LogMsg(commitMsg)

	replyMsg, committedMsg, err := node.CurrentState.Commit(commitMsg)
	if err != nil {
		return err
	}

	if replyMsg != nil {
		if committedMsg == nil {
			return errors.New("committed message is nil, even though the reply message is not nil")
		}

		// Attach node ID to the message
		replyMsg.NodeID = node.NodeID

		// Save the last version of committed messages to node.
		node.CommittedMsgs = append(node.CommittedMsgs, committedMsg)

		LogStage("Commit", true)
		node.Reply(replyMsg)
		LogStage("Reply", true)
	}

	return nil
}

func (node *Node) GetReply(msg *consensus.ReplyMsg) {
	fmt.Printf("Result: %s by %s\n", msg.Result, msg.NodeID)
}

func (node *Node) createStateForNewConsensus() error {
	// Check if there is an ongoing consensus process.
	if node.CurrentState != nil {
		return errors.New("another consensus is ongoing")
	}

	// Get the last sequence ID
	var lastSequenceID int64
	if len(node.CommittedMsgs) == 0 {
		lastSequenceID = -1
	} else {
		lastSequenceID = node.CommittedMsgs[len(node.CommittedMsgs)-1].SequenceID
	}

	// Create a new state for this new consensus process in the Primary
	node.CurrentState = consensus.CreateState(node.View.ID, lastSequenceID)

	LogStage("Create the replica status", true)

	return nil
}

func (node *Node) dispatchMsg() {
	for {
		select {
		case msg := <-node.MsgEntrance:
			err := node.routeMsg(msg)
			if err != nil {
				fmt.Println(err)
				// TODO: send err to ErrorChannel
			}
		case <-node.Alarm:
			err := node.routeMsgWhenAlarmed()
			if err != nil {
				fmt.Println(err)
				// TODO: send err to ErrorChannel
			}
		}
	}
}

func (node *Node) routeMsg(msg interface{}) []error {
	switch msg.(type) {
	case *consensus.SingletxMsg:
		if node.CurrentState == nil {
			// Copy buffered messages first.
			msgs := make([]*consensus.SingletxMsg, len(node.MsgBuffer.SingletxMsgs))
			copy(msgs, node.MsgBuffer.SingletxMsgs)

			// Append a newly arrived message.
			msgs = append(msgs, msg.(*consensus.SingletxMsg))

			// Empty the buffer.
			node.MsgBuffer.SingletxMsgs = make([]*consensus.SingletxMsg, 0)

			// Send messages to MsgDelivery通道(这边传出去后，resolveMsg goroutine会立马接收这个通道中的消息).
			node.MsgDelivery <- msgs

			//node.CrosstxDelivery <- msgs
		} else { //当前状态不为空，就得等执行完当前任务才能处理此msg，则此msg会先进入MsgBuffer中
			node.MsgBuffer.SingletxMsgs = append(node.MsgBuffer.SingletxMsgs, msg.(*consensus.SingletxMsg))
		}
	case *consensus.PrePrepareMsg:
		if node.CurrentState == nil {
			// Copy buffered messages first.
			msgs := make([]*consensus.PrePrepareMsg, len(node.MsgBuffer.PrePrepareMsgs))
			copy(msgs, node.MsgBuffer.PrePrepareMsgs)

			// Append a newly arrived message.
			msgs = append(msgs, msg.(*consensus.PrePrepareMsg))

			// Empty the buffer.
			node.MsgBuffer.PrePrepareMsgs = make([]*consensus.PrePrepareMsg, 0)

			// Send messages.
			node.MsgDelivery <- msgs
		} else {
			node.MsgBuffer.PrePrepareMsgs = append(node.MsgBuffer.PrePrepareMsgs, msg.(*consensus.PrePrepareMsg))
		}
	case *consensus.VoteMsg:
		if msg.(*consensus.VoteMsg).MsgType == consensus.PreCommitMsg {
			if node.CurrentState == nil || node.CurrentState.CurrentStage != consensus.Proposed {
				node.MsgBuffer.PreCommitMsgs = append(node.MsgBuffer.PreCommitMsgs, msg.(*consensus.VoteMsg))
			} else {
				// Copy buffered messages first.
				msgs := make([]*consensus.VoteMsg, len(node.MsgBuffer.PrepareMsgs))
				copy(msgs, node.MsgBuffer.PrepareMsgs)

				// Append a newly arrived message.
				msgs = append(msgs, msg.(*consensus.VoteMsg))

				// Empty the buffer.
				node.MsgBuffer.PreCommitMsgs = make([]*consensus.VoteMsg, 0)

				// Send messages.
				node.MsgDelivery <- msgs
			}
		} else if msg.(*consensus.VoteMsg).MsgType == consensus.CommitMsg {
			if node.CurrentState == nil || node.CurrentState.CurrentStage != consensus.CommittedLocal {
				node.MsgBuffer.CommitMsgs = append(node.MsgBuffer.CommitMsgs, msg.(*consensus.VoteMsg))
			} else {
				// Copy buffered messages first.
				msgs := make([]*consensus.VoteMsg, len(node.MsgBuffer.CommitMsgs))
				copy(msgs, node.MsgBuffer.CommitMsgs)

				// Append a newly arrived message.
				msgs = append(msgs, msg.(*consensus.VoteMsg))

				// Empty the buffer.
				node.MsgBuffer.CommitMsgs = make([]*consensus.VoteMsg, 0)

				// Send messages.
				node.MsgDelivery <- msgs
			}
		}
	}

	return nil
}

func (node *Node) routeMsgWhenAlarmed() []error {
	if node.CurrentState == nil {
		// Check ReqMsgs, send them.
		if len(node.MsgBuffer.SingletxMsgs) != 0 {
			msgs := make([]*consensus.SingletxMsg, len(node.MsgBuffer.SingletxMsgs))
			copy(msgs, node.MsgBuffer.SingletxMsgs)

			node.MsgDelivery <- msgs
		}

		// Check PrePrepareMsgs, send them.
		if len(node.MsgBuffer.PrePrepareMsgs) != 0 {
			msgs := make([]*consensus.PrePrepareMsg, len(node.MsgBuffer.PrePrepareMsgs))
			copy(msgs, node.MsgBuffer.PrePrepareMsgs)

			node.MsgDelivery <- msgs
		}
	} else {
		switch node.CurrentState.CurrentStage {
		case consensus.PrePrepared:
			// Check PrepareMsgs, send them.
			if len(node.MsgBuffer.PrepareMsgs) != 0 {
				msgs := make([]*consensus.VoteMsg, len(node.MsgBuffer.PrepareMsgs))
				copy(msgs, node.MsgBuffer.PrepareMsgs)

				node.MsgDelivery <- msgs
			}
		case consensus.PreCommitted:
			// Check CommitMsgs, send them.
			if len(node.MsgBuffer.PreCommitMsgs) != 0 {
				msgs := make([]*consensus.VoteMsg, len(node.MsgBuffer.PreCommitMsgs))
				copy(msgs, node.MsgBuffer.PreCommitMsgs)

				node.MsgDelivery <- msgs
			}
		}
	}

	return nil
}

func (node *Node) resolveMsg() {
	for {
		// Get buffered messages from the dispatcher.
		msgs := <-node.MsgDelivery
		switch msgs.(type) {
		case []*consensus.SingletxMsg:
			errs := node.resolveRequestMsg(msgs.([]*consensus.SingletxMsg))
			if len(errs) != 0 {
				for _, err := range errs {
					fmt.Println(err)
				}
				// TODO: send err to ErrorChannel
			}
		case []*consensus.PrePrepareMsg:
			errs := node.resolvePrePrepareMsg(msgs.([]*consensus.PrePrepareMsg))
			if len(errs) != 0 {
				for _, err := range errs {
					fmt.Println(err)
				}
				// TODO: send err to ErrorChannel
			}
		case []*consensus.VoteMsg:
			voteMsgs := msgs.([]*consensus.VoteMsg)
			if len(voteMsgs) == 0 {
				break
			}

			if voteMsgs[0].MsgType == consensus.PreCommitMsg {
				errs := node.resolvePreCommitMsg(voteMsgs)
				if len(errs) != 0 {
					for _, err := range errs {
						fmt.Println(err)
					}
					// TODO: send err to ErrorChannel
				}
			} else if voteMsgs[0].MsgType == consensus.CommitMsg {
				errs := node.resolveCommitMsg(voteMsgs)
				if len(errs) != 0 {
					for _, err := range errs {
						fmt.Println(err)
					}
					// TODO: send err to ErrorChannel
				}
			}
		}
	}
}

func (node *Node) alarmToDispatcher() {
	for {
		time.Sleep(ResolvingTimeDuration)
		node.Alarm <- true
	}
}

func (node *Node) resolveRequestMsg(msgs []*consensus.SingletxMsg) []error {
	errs := make([]error, 0)

	// Resolve messages
	for _, reqMsg := range msgs {
		err := node.GetReq(reqMsg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (node *Node) resolvePrePrepareMsg(msgs []*consensus.PrePrepareMsg) []error {
	errs := make([]error, 0)

	// Resolve messages
	for _, prePrepareMsg := range msgs {
		err := node.GetPrePrepare(prePrepareMsg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (node *Node) resolvePreCommitMsg(msgs []*consensus.VoteMsg) []error {
	errs := make([]error, 0)

	// Resolve messages
	for _, precommitMsg := range msgs {
		err := node.GetPreCommit(precommitMsg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (node *Node) resolveCommitMsg(msgs []*consensus.VoteMsg) []error {
	errs := make([]error, 0)

	// Resolve messages
	for _, commitMsg := range msgs {
		err := node.GetCommit(commitMsg)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}
