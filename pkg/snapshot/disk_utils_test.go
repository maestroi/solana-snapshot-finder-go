package snapshot

import "testing"

func TestFormatBytes(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{500, "500 B"},
		{2048, "2.00 KB"},
		{5 * 1024 * 1024, "5.00 MB"},
		{2 * 1024 * 1024 * 1024, "2.00 GB"},
	}
	for _, c := range cases {
		if got := FormatBytes(c.in); got != c.want {
			t.Errorf("FormatBytes(%d)=%q want %q", c.in, got, c.want)
		}
	}
}
