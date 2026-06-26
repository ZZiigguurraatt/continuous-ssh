// Disk-cap policy. The total on-disk footprint of all sessions on this
// host is bounded by DiskBudget:
//
//	DiskBudget = min(2 GiB, 20% × free_disk)
//	           − min(100 MiB, 5% × 20% × free_disk) × N_growing
//
// where N_growing is the number of sessions in active or stale status
// (dead sessions don't grow, so they don't reserve). The reserve
// per session scales down on small disks so it can't dominate the
// raw budget. Each daemon's sweeper compares the global sum to
// DiskBudget and shuts itself down when both `sum > DiskBudget` and
// `my_size > DiskBudget/N_growing` — the second clause picks the
// fast-grower while sparing sessions already under their fair share.
package sessions

import (
	"os"
	"path/filepath"
	"syscall"
)

const (
	diskCapAbsolute      uint64 = 2 << 30   // 2 GiB ceiling regardless of free disk
	diskCapFreeRatio            = 0.20      // alternatively, 20% of free disk
	diskCapReserveMax    uint64 = 100 << 20 // upper bound on per-session reserve
	diskCapReserveSubRatio      = 0.05      // reserve = 5% of the raw budget…
	// …i.e. 5% × diskCapFreeRatio × free_disk = 1% × free_disk.
)

// DiskBudget is the global cap on the sum of every session's segment
// bytes. Returns 0 if the reserve eats the whole budget — at that
// point any session over fair share (also 0) shuts down.
func DiskBudget(sessions []Session, freeDisk uint64) uint64 {
	c := diskCapAbsolute
	fromFree := uint64(float64(freeDisk) * diskCapFreeRatio)
	if fromFree < c {
		c = fromFree
	}
	var growing uint64
	for _, s := range sessions {
		if s.Status == StatusActive || s.Status == StatusStale {
			growing++
		}
	}
	perSession := uint64(float64(freeDisk) * diskCapFreeRatio * diskCapReserveSubRatio)
	if perSession > diskCapReserveMax {
		perSession = diskCapReserveMax
	}
	reserve := perSession * growing
	if reserve >= c {
		return 0
	}
	return c - reserve
}

// TotalDiskUsage sums each session's disk bytes (segment-file totals).
// Includes dead sessions — they still occupy disk until xssh rm.
func TotalDiskUsage(sessions []Session) uint64 {
	var total uint64
	for _, s := range sessions {
		if s.DiskBytes > 0 {
			total += uint64(s.DiskBytes)
		}
	}
	return total
}

// GrowingCount returns N_growing (active + stale).
func GrowingCount(sessions []Session) int {
	var n int
	for _, s := range sessions {
		if s.Status == StatusActive || s.Status == StatusStale {
			n++
		}
	}
	return n
}

// DiskSpace reports (free, total) bytes on the filesystem holding
// ~/.continuous-ssh. The directory is created if missing so statfs
// always has a real path to work with.
func DiskSpace() (free, total uint64, err error) {
	home, herr := os.UserHomeDir()
	if herr != nil {
		return 0, 0, herr
	}
	root := filepath.Join(home, ".continuous-ssh")
	if merr := os.MkdirAll(root, 0o700); merr != nil {
		return 0, 0, merr
	}
	var st syscall.Statfs_t
	if serr := syscall.Statfs(root, &st); serr != nil {
		return 0, 0, serr
	}
	return uint64(st.Bavail) * uint64(st.Bsize),
		uint64(st.Blocks) * uint64(st.Bsize),
		nil
}

// FreeDisk reports bytes available on the filesystem holding
// ~/.continuous-ssh.
func FreeDisk() (uint64, error) {
	free, _, err := DiskSpace()
	return free, err
}

// HumanBytes is the exported version of humanBytes for callers that
// want consistent byte formatting in user-facing strings (e.g. the
// daemon's startup banner).
func HumanBytes(n int64) string { return humanBytes(n) }
