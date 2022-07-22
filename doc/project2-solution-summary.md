# Project1 StandaloneKV 

## EDITOR
Qilong Li

## GOAL

## SOLUTION

## TIPS
1. In Part A, you just need to implement leader election and log replication without storage service.
2. 一个non-nil的空切片和一个nil的切片并不是深等价的，比如[]byte{}和[]byte{nil}是非等价的。
3. RaftLog的stabled既可以比committed提前，也可以比committed落后。一个rawnode的entries可以在集群未committed时保存到本地，也可以在掉线的
情况下一直未接收entries从而落后集群committed。

# Reading Material
* https://raft.github.io/raft.pdf
* https://github.com/HennyNile/tinykv/blob/course/doc/project2-RaftKV.md
* https://github.com/HennyNile/tinykv/blob/course/raft/doc.go
* Metting Vedio: raft算法介绍和对应代码框架介绍.mp4