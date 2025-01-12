package beacon

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestParseBeaconQuantity(t *testing.T) {
	type Envelope struct {
		Value Quantity `json:"value"`
	}

	tests := []struct {
		name     string
		expected uint64
		input    string
	}{
		{
			name:     "number",
			expected: uint64(7891),
			input:    "7891",
		},
		{
			name:     "zero",
			expected: uint64(0),
			input:    "0",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			data := fmt.Sprintf(`{"value": "%v"}`, test.input)
			var envelope Envelope
			err := json.Unmarshal([]byte(data), &envelope)
			require.NoError(err)
			require.Equal(test.expected, envelope.Value.Value())
		})
	}
}

func TestParseBeaconQuantity_InvalidInput(t *testing.T) {
	type Envelope struct {
		Value Quantity `json:"value"`
	}

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "empty",
			input: ``,
		},
		{
			name:  "emptyString",
			input: `""`,
		},
		{
			name:  "negative",
			input: `"-1234"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			data := fmt.Sprintf(`{"value": %v}`, test.input)
			var envelope Envelope
			err := json.Unmarshal([]byte(data), &envelope)
			require.Error(err)
		})
	}
}

func TestParseBeaconExecutionTransaction(t *testing.T) {
	type Envelope struct {
		Value ExecutionTransaction `json:"value"`
	}

	transactionData := "0x02f904c18242688201cc85012a05f200852e90edd000831e848094b7fb99e86f93dc3047a12932052236d8530651738a0a968163f0a57b400000b90444cb222302000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000003e80a61a41a9f46074d7b67e2bd2ebf500234af33500a61a609ccac704133f6ce16ae6a214f6411a37d0a61c62bf09176badc442315df76f75c4019c8a80a61d636458654df92376f0461bec12df45d96c90a6210240471de2b5f23c34c9584353d97676b7d0a62131020f0e3886f153e75a6cb8cd27a6869d20a6235429039f3314898de27d29cb6542967e2700a627709b782089bd05fc06d4aa272f4ef185fe60a62a9ae5009463da7ca2752bb98ac3a886e725a0a62f2c835e131a536678c0a55d042713434e4c00a637e53ee819a3d9b122532705c4a242f3d4d650a6391f5c0ff83fa183fd4e3d9c1bb1a75175e020a63c5af1ea8aa538c9c3472c7caa4a8f18e9bbd0a6419834f45b85ffb02a254e31219e5d0ffd44f0a64619d16f62a3c31df66fc9c2e05041b16f4a60a646f6cb2ad35d8441b37791c25b5df454205500a64af74104ab36173af57640f25c880288210210a64bc73793faf399adb51ebad204acb11f0ae640a64cf084c35cbcda683b9b996c7e3802e1c07cb0a64cf66dfae3efafc97536544a46469a2a7a6370a64f114fed179e2118b4f29482fd51cc51ea0b70a64f4eb382e89e7ac8d79832bbdf54f69b6ff500a64f96716ee6b3d1a4508259e152b54211fd1ae0a6503781b5ef6e4c0613e71f9f99364f2e3daae0a651229d4a1612edb41852c4c6ad7a58874e3c40a655caa9a11b42200b538b708f6de243589d4130a6599f971c3d394a78274a29ed5d2c59b092b620a65b3aad3672ac3cd842d474851c121d67e81b30a65c3660771279fede36cc8ad304c3e9ad150e30a6600ae9d94a0cccc4f8b86c90f505ba99be0cd0a6608914dbb45c9dd82b409636b5f8bbf6a5c210a663680b7ee658783f53951d7df215fb1ec2bfc0a663aaca26d82de6430cf271c9fae22bca1f07a0a66624bc0e564e5e1b1a28922bd433cfbcee7740a668a6617dcbdd38625796938312d8c47c406a90a668fa07f4560542ff331c86f01b5f9ff87e7510a669dfc594db999ae469ff397899bbb9ee13b390a66a1a8159356eb60656e9e1ed14fac4c8b93300a67018a2390b68ab7857139d330a1219b700ba10a6740238b013e7a98bab6bc99045870bc98885f0a678c276fc3f1b86995b16233de6adc31a384030a67d0b7bc11c54ddc8c9f94f442434fb187523a0a680b1ea757a0faba2256603c2b3f5a296eac8a0a68b5d6ab7a7a1a80eb8fef0aff55d1bee47a210a68c758f0bde9f2daf586d68412a0825aa24ea30a68e71666bcab6603ec090db3eb8a9fab4dca4c0a690b298f84d12414f5c8db7de1ece5a46058770a6944e8a10ef1f9c6e1b2e14ecfa1aade162cfb0a694d1e3cbc3e6575f7d649ba2ce100031b43740a69502076d5411084f1013aa45ca6d628ef19b5000000000000000000000000000000000000000000000000c001a04d3bf4b1cf62d7f6fcf192b6f1575a14c171be1158731c7cd49b3935dd61dff1a06482b531f9c0cb30c310bb3d1b0c4dcd40c473515bf76c38fdb340fe91478066"

	tests := []struct {
		name     string
		expected []byte
		input    string
	}{
		{
			name:     "example",
			expected: []byte(transactionData),
			input:    transactionData,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			data := fmt.Sprintf(`{"value": "%v"}`, test.input)
			var envelope Envelope
			err := json.Unmarshal([]byte(data), &envelope)
			require.NoError(err)
			require.Equal(test.expected, []byte(envelope.Value))
		})
	}
}

func TestParseBeaconExecutionTransaction_InvalidInput(t *testing.T) {
	type Envelope struct {
		Value ExecutionTransaction `json:"value"`
	}

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "empty",
			input: ``,
		},
		{
			name:  "emptyString",
			input: `""`,
		},
		{
			name:  "miss0X",
			input: `"02f904c"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			data := fmt.Sprintf(`{"value": %v}`, test.input)
			var envelope Envelope
			err := json.Unmarshal([]byte(data), &envelope)
			require.Error(err)
		})
	}
}

func TestParseBeaconBlob(t *testing.T) {
	type Envelope struct {
		Value Blob `json:"value"`
	}

	blob := "0x02f904c18242688201cc85012a05f200852e90edd000831e848094b7fb99e86f93dc3"

	tests := []struct {
		name     string
		expected []byte
		input    string
	}{
		{
			name:     "example",
			expected: []byte(blob),
			input:    blob,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			data := fmt.Sprintf(`{"value": "%v"}`, test.input)
			var envelope Envelope
			err := json.Unmarshal([]byte(data), &envelope)
			require.NoError(err)
			require.Equal(test.expected, []byte(envelope.Value))
		})
	}
}

func TestParseBeaconBlob_InvalidInput(t *testing.T) {
	type Envelope struct {
		Value Blob `json:"value"`
	}

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "empty",
			input: ``,
		},
		{
			name:  "emptyString",
			input: `""`,
		},
		{
			name:  "miss0X",
			input: `"02f904c"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			data := fmt.Sprintf(`{"value": %v}`, test.input)
			var envelope Envelope
			err := json.Unmarshal([]byte(data), &envelope)
			require.Error(err)
		})
	}
}

func TestParseCalculateEpoch(t *testing.T) {
	tests := []struct {
		name     string
		expected uint64
		slot     uint64
	}{
		{
			name:     "0",
			expected: uint64(0),
			slot:     uint64(0),
		},
		{
			name:     "319",
			expected: uint64(9),
			slot:     uint64(319),
		},
		{
			name:     "320",
			expected: uint64(10),
			slot:     uint64(320),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			epoch, err := calculateEpoch(test.slot)
			require.NoError(err)
			require.Equal(test.expected, epoch)
		})
	}
}
