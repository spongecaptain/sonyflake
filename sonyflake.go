// Package sonyflake implements Sonyflake, a distributed unique ID generator inspired by Twitter's Snowflake.
//
// A Sonyflake ID is composed of
//
//	39 bits for time in units of 10 msec
//	 8 bits for a sequence number
//	16 bits for a machine id
package sonyflake

import (
	"errors"
	"net"
	"sync"
	"time"
)

// These constants are the bit lengths of Sonyflake ID parts.
const (
	// BitLenTime 39 位的时间戳，单位为 10ms
	BitLenTime = 39 // bit length of time
	// BitLenSequence 8 位，每 10 ms 内最多有 256 个记录
	BitLenSequence = 8 // bit length of sequence number
	// BitLenMachineID 机器 ID，这里也就是 16 位，共可以支持 65536 台机器
	BitLenMachineID = 63 - BitLenTime - BitLenSequence // bit length of machine id
)

// Settings configures Sonyflake:
//
// StartTime is the time since which the Sonyflake time is defined as the elapsed time.
// If StartTime is 0, the start time of the Sonyflake is set to "2014-09-01 00:00:00 +0000 UTC".
// If StartTime is ahead of the current time, Sonyflake is not created.
//
// MachineID returns the unique ID of the Sonyflake instance.
// If MachineID returns an error, Sonyflake is not created.
// If MachineID is nil, default MachineID is used.
// Default MachineID returns the lower 16 bits of the private IP address.
//
// CheckMachineID validates the uniqueness of the machine ID.
// If CheckMachineID returns false, Sonyflake is not created.
// If CheckMachineID is nil, no validation is done.
type Settings struct {
	// StartTime 开始时间
	StartTime time.Time
	// MachineID 机器 ID 获取方法，如果为 nil，那么默认取 private IP 后 16 位
	MachineID func() (uint16, error)
	// CheckMachineID 检查 ID 的合法性，默认为 nil
	// CheckMachineID 方法通常可以配合 MachineID 一起使用，先 MachineID 去中心化服务上获取 ID（例如 Redis 集群，或者本地生产），
	// 然后利用 CheckMachineID 去中心化的服务中检查此输入的 uint16 是否是真的唯一
	CheckMachineID func(uint16) bool
}

// Sonyflake is a distributed unique ID generator.
type Sonyflake struct {
	mutex       *sync.Mutex
	startTime   int64  // 开始时间
	elapsedTime int64  // 已经过去的时间，用于检查是否有时间回拨现象
	sequence    uint16 // 某一个时刻，可以总共生产 256 个序号（2^8），但是使用 uint16 位保存，是因为可能存在时间回拨现象
	machineID   uint16 // 机器号
}

// NewSonyflake returns a new Sonyflake configured with the given Settings.
// NewSonyflake returns nil in the following cases:
// - Settings.StartTime is ahead of the current time.
// - Settings.MachineID returns an error.
// - Settings.CheckMachineID returns false.
func NewSonyflake(st Settings) *Sonyflake {
	sf := new(Sonyflake)
	sf.mutex = new(sync.Mutex)
	sf.sequence = uint16(1<<BitLenSequence - 1)

	if st.StartTime.After(time.Now()) {
		return nil
	}
	if st.StartTime.IsZero() {
		sf.startTime = toSonyflakeTime(time.Date(2014, 9, 1, 0, 0, 0, 0, time.UTC))
	} else {
		sf.startTime = toSonyflakeTime(st.StartTime)
	}

	var err error
	if st.MachineID == nil {
		sf.machineID, err = lower16BitPrivateIP()
	} else {
		sf.machineID, err = st.MachineID()
	}
	if err != nil || (st.CheckMachineID != nil && !st.CheckMachineID(sf.machineID)) {
		return nil
	}

	return sf
}

// NextID generates a next unique ID.
// After the Sonyflake time overflows, NextID returns an error.
func (sf *Sonyflake) NextID() (uint64, error) {
	const maskSequence = uint16(1<<BitLenSequence - 1)
	// 上全局锁
	sf.mutex.Lock()
	defer sf.mutex.Unlock()
	// 获取当前时间到 startTime 的时间序列
	current := currentElapsedTime(sf.startTime)
	if sf.elapsedTime < current {
		// sf.elapsedTime < current 没有出现时间回拨现象，处于新的时间轮次，需要将 sequence 重新置为 0
		sf.elapsedTime = current
		sf.sequence = 0
	} else { // sf.elapsedTime >= current
		// 下面的逻辑意味着：
		// 1. 如果时间没有回拨，也就是当前时刻下 256 个 ID 分配完了，那么就等待一个时间单位 10ms，然后下一个时刻 sf.elapsedTim+1 开始从 sequence = 0 开始继续递增分配
		// 2. 如果时间回拨了，那么会在这个大于回拨时间的 sf.elapsedTime 继续分配（不是在回拨时间 current 时刻下）这个 256 个 ID
		// 在这个过程中，如果 ID 被分配完了，那么就 sleep 到 sf.elapsedTime+1 时刻，然后继续从 0 开始分配 sequence
		// 如果 ID 没有分配完，但是时间又恢复正常，即解决了时间回拨问题，那么就会按照正常逻辑，sf.elapsedTime = current，然后继续从 0 开始分配 sequence
		sf.sequence = (sf.sequence + 1) & maskSequence
		// sf.sequence == 0 有两种情况
		// 情况1：sf.elapsedTime == current && sf.sequence == 0，说明此 sf.elapsedTime 时刻的 256 个序号被消耗完毕了，因此等待一个时间单位（sf.elapsedTime++）
		// 情况2：sf.elapsedTime > current && sf.sequence == 0，说明出现了时间回拨现象，因此等待，直到机器时间等于 sf.elapsedTime + 1
		if sf.sequence == 0 {
			sf.elapsedTime++ // 两种情况都会进行 sf.elapsedTime++，这是最关键的
			overtime := sf.elapsedTime - current
			// 为了避免时间回拨，sleep 这些时间
			// NOTE：此时锁没有释放，当前协程 sleep 期间，其余调用 Sonyflake.NextID 的方法也会阻塞于锁，相当于继续等待
			time.Sleep(sleepTime((overtime)))
		}
	}

	return sf.toID()
}

const sonyflakeTimeUnit = 1e7 // nsec, i.e. 10 msec

func toSonyflakeTime(t time.Time) int64 {
	return t.UTC().UnixNano() / sonyflakeTimeUnit
}

func currentElapsedTime(startTime int64) int64 {
	return toSonyflakeTime(time.Now()) - startTime
}

func sleepTime(overtime int64) time.Duration {
	return time.Duration(overtime*sonyflakeTimeUnit) -
		time.Duration(time.Now().UTC().UnixNano()%sonyflakeTimeUnit)
}

func (sf *Sonyflake) toID() (uint64, error) {
	if sf.elapsedTime >= 1<<BitLenTime {
		return 0, errors.New("over the time limit")
	}

	return uint64(sf.elapsedTime)<<(BitLenSequence+BitLenMachineID) |
		uint64(sf.sequence)<<BitLenMachineID |
		uint64(sf.machineID), nil
}

func privateIPv4() (net.IP, error) {
	as, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, a := range as {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}

		ip := ipnet.IP.To4()
		if isPrivateIPv4(ip) {
			return ip, nil
		}
	}
	return nil, errors.New("no private ip address")
}
func isPrivateIPv4(ip net.IP) bool {
	return ip != nil &&
		(ip[0] == 10 || ip[0] == 172 && (ip[1] >= 16 && ip[1] < 32) || ip[0] == 192 && ip[1] == 168)
}

func lower16BitPrivateIP() (uint16, error) {
	ip, err := privateIPv4()
	if err != nil {
		return 0, err
	}

	return uint16(ip[2])<<8 + uint16(ip[3]), nil
}

// ElapsedTime returns the elapsed time when the given Sonyflake ID was generated.
// 得到此 ID 距 startTime 已经有多少时间了
func ElapsedTime(id uint64) time.Duration {
	return time.Duration(elapsedTime(id) * sonyflakeTimeUnit)
}

// 将 ID 向右移动 8 + 16 位，获取得到 ID 的前 40 位（首位一直为 0），因此相当于获取 39 位的时间戳数值
func elapsedTime(id uint64) uint64 {
	return id >> (BitLenSequence + BitLenMachineID)
}

// SequenceNumber returns the sequence number of a Sonyflake ID.
// 返回序列 ID
func SequenceNumber(id uint64) uint64 {
	const maskSequence = uint64((1<<BitLenSequence - 1) << BitLenMachineID)
	return id & maskSequence >> BitLenMachineID
}

// MachineID returns the machine ID of a Sonyflake ID.
// 返回机器 ID
func MachineID(id uint64) uint64 {
	const maskMachineID = uint64(1<<BitLenMachineID - 1)
	return id & maskMachineID
}

// Decompose returns a set of Sonyflake ID parts.
func Decompose(id uint64) map[string]uint64 {
	msb := id >> 63
	time := elapsedTime(id)
	sequence := SequenceNumber(id)
	machineID := MachineID(id)
	return map[string]uint64{
		"id":         id,        // 整个 ID
		"msb":        msb,       // 首位
		"time":       time,      // 时间戳
		"sequence":   sequence,  // 序列号
		"machine-id": machineID, // 机器 ID
	}
}
