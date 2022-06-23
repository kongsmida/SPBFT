package consensus

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

type SingletxMsg struct {
	Timestamp  int64  `json:"timestamp"`
	ClientID   string `json:"clientID"`
	Operation  string `json:"operation"`
	SequenceID int64  `json:"sequenceID"`
}

type CrosstxMsg struct {
	Timestamp  int64       `json:"timestamp"`
	ClientID   string      `json:"clientID"`
	Operation  Transaction `json:"operation"`
	SequenceID int64       `json:"sequenceID"`
}

type ProposeMsg struct {
	ViewID     int64       `json:"viewID"`
	SequenceID int64       `json:"sequenceID"`
	Subtx      Transaction `json:"subtx"`
	NodeID     string      `json:"nodeID"`
}

type CommitLocalMsg struct {
	ViewID     int64  `json:"viewID"`
	SequenceID int64  `json:"sequenceID"`
	Digest     string `json:"digest"`
	NodeID     string `json:"nodeID"`
}

type ReplyMsg struct {
	ViewID    int64  `json:"viewID"`
	Timestamp int64  `json:"timestamp"`
	ClientID  string `json:"clientID"`
	NodeID    string `json:"nodeID"`
	Result    string `json:"result"`
}

type PreReplyMsg struct {
	ViewID    int64  `json:"viewID"`
	Timestamp int64  `json:"timestamp"`
	PrimaryID string `json:"primaryID"`
	NodeID    string `json:"nodeID"`
	Subtx     string `json:"subtx"`
}

type PrePrepareMsg struct {
	ViewID     int64        `json:"viewID"`
	SequenceID int64        `json:"sequenceID"`
	Digest     string       `json:"digest"`
	RequestMsg *SingletxMsg `json:"requestMsg"`
}

type VoteMsg struct {
	ViewID     int64  `json:"viewID"`
	SequenceID int64  `json:"sequenceID"`
	Digest     string `json:"digest"`
	NodeID     string `json:"nodeID"`
	MsgType    `json:"msgType"`
}

type MsgType int

const (
	// PrepareMsg
	CommitMsg MsgType = iota
	PreCommitMsg
	ProofofProposalMsg
	CommitFinalMsg
)
