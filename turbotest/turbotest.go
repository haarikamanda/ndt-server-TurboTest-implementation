// turbotest.go
// TurboTest inference module

package turbotest

import (
	"fmt"

	"github.com/gomlx/gomlx/backends"
	"github.com/gomlx/gomlx/context"
	. "github.com/gomlx/gomlx/graph"
	"github.com/gomlx/onnx-gomlx/onnx"
	"github.com/m-lab/ndt-server/redis"
)

type TurboTest struct {
	// RedisClient is the Redis client for caching.
	RedisClient *redis.Client
}

// panics upon error
func must(err error) {
	if err != nil {
		panic(err)
	}
}

// panics upon error and returns value
func must1[T any](val T, err error) T {
	must(err)
	return val
}

func (tt *TurboTest) main() {
	// 1. Load the ONNX model
	modelPath := "path/to/your/model.onnx"
	model := must1(onnx.ReadFile(modelPath))

	// 2. Create a GoMLX context and load model weights
	ctx := context.New()
	must(model.VariablesToContext(ctx))

	// 3. Get our input data from the Redis cache
	// Replace with your actual input data
	// parse command line for model and tau
	// obtain speed test uuid
	// call infer()
	uuid := "test-uuid-123"
	inputData, err := tt.RedisClient.GetTCPInfo(ctx, uuid)

	// 4. Run inference
	output := runInference(ctx, model, inputData)

	fmt.Printf("Output: %v\n", output)
}

func runInference(ctx *context.Context, model *onnx.Model, inputData *redis.TCPInfo) interface{} {
	backend := backends.New() // Uses XLA backend by default

	results := context.MustExecOnceN(
		backend,
		ctx,
		func(ctx *context.Context, inputs []*Node) []*Node {
			g := inputs[0].Graph()

			// Map your input names to nodes
			// Check your ONNX model to find the actual input names
			inputMap := map[string]*Node{
				"input": inputs[0], // Replace "input" with your actual input name
			}

			// Call the ONNX model graph
			// If you have specific outputs, specify them; otherwise use nil
			outputs := model.CallGraph(ctx, g, inputMap, nil...)

			return outputs
		},
		inputData, // Your input tensors
	)

	return results
}
