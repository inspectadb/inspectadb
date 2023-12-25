package config

import (
	"testing"
)

func TestParseDSN(t *testing.T) {
	type testCase struct {
		name     string
		dsn      string
		expected DBConfig
	}

	testCases := []testCase{
		testCase{
			name: "Without Database",
			dsn:  "mysql://root:password@localhost:3306/myschema",
			expected: DBConfig{
				Driver:   "mysql",
				User:     "root",
				Password: "password",
				Host:     "localhost",
				Port:     3306,
				Database: "",
				Schema:   "myschema",
			},
		},
		testCase{
			name: "With Database",
			dsn:  "mysql://root:password@localhost:3306/database:myschema",
			expected: DBConfig{
				Driver:   "mysql",
				User:     "root",
				Password: "password",
				Host:     "localhost",
				Port:     3306,
				Database: "database",
				Schema:   "myschema",
			},
		},
		testCase{
			name: "Without password",
			dsn:  "mysql://root:@localhost:3306/database:myschema",
			expected: DBConfig{
				Driver:   "mysql",
				User:     "root",
				Password: "",
				Host:     "localhost",
				Port:     3306,
				Database: "database",
				Schema:   "myschema",
			},
		},
	}

	for _, tc := range testCases {
		actual, err := parseDSN(tc.dsn)

		if err != nil || actual != tc.expected {
			t.Errorf("'%s' case failed", tc.name)
		}

		t.Logf("'%s' was successful", tc.name)
	}
}
