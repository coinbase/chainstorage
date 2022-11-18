package endpoints

import (
	"testing"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestGetCookieNames(t *testing.T) {
	tests := []struct {
		cookies  string
		expected string
	}{
		{
			cookies:  "",
			expected: "",
		},
		{
			cookies:  "cookies",
			expected: "",
		},
		{
			cookies:  "c1;c2",
			expected: "",
		},
		{
			cookies:  "c1=1;c2",
			expected: "c1",
		},
		{
			cookies:  "c1=1234567;c2=",
			expected: "c1;c2",
		},
	}
	for _, test := range tests {
		t.Run(test.cookies, func(t *testing.T) {
			require := testutil.Require(t)

			actual := getCookieNames(test.cookies)
			require.Equal(test.expected, actual)
		})
	}
}
