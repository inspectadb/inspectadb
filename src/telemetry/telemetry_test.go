package telemetry

import (
	"github.com/magiconair/properties/assert"
	"os"
	"testing"
)

func TestIsRunningInCI(t *testing.T) {
	t.Run("CI environment variable set", func(t *testing.T) {
		os.Setenv("CI", "true")
		defer os.Unsetenv("CI")
		assert.Equal(t, isRunningInCI(), true)
	})

	t.Run("JENKINS_HOME environment variable set", func(t *testing.T) {
		os.Setenv("JENKINS_HOME", "/some/path")
		defer os.Unsetenv("JENKINS_HOME")
		assert.Equal(t, isRunningInCI(), true)
	})

	t.Run("BUILD_REASON environment variable set", func(t *testing.T) {
		_ = os.Setenv("BUILD_REASON", "IndividualCI")
		assert.Equal(t, isRunningInCI(), true)
		_ = os.Setenv("BUILD_REASON", "BatchedCI")
		assert.Equal(t, isRunningInCI(), true)
	})

	t.Run("No CI-related environment variables set", func(t *testing.T) {
		os.Clearenv()
		assert.Equal(t, isRunningInCI(), false)
	})
}

func TestIsRunningInDocker(t *testing.T) {
	t.Run("IS_DOCKER environment variable set", func(t *testing.T) {
		_ = os.Setenv("IS_DOCKER", "true")
		assert.Equal(t, isRunningInDocker(), true)
	})

	t.Run("No Docker-related environment variable set", func(t *testing.T) {
		os.Clearenv()
		assert.Equal(t, isRunningInDocker(), false)
	})
}
