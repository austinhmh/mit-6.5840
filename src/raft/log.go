package raft

// must lock outSize
func (rf *Raft) ApplyLog(command interface{}) int {
	rf.log = append(rf.log, command)
	return len(rf.log) - 1
}
