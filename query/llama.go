//go:build llama
// +build llama

package query

import (
	"fmt"
	"os"
	"strconv"

	llama "github.com/go-skynet/go-llama.cpp"
)

var (
	LLamaModel      *llama.LLama = nil
	LLamaModelPath  string       = "./models/ggml-model-q4_0.bin"
	LLamaContextLen int          = 512
)

func init() {
	funcMap["embedding"] = &Function{
		"embedding",
		1,
		true,
		TLIST,
		funcEmbedding,
		nil,
	}
	var err error

	envLlamaPath := os.Getenv("LLAMA_PATH")
	if envLlamaPath != "" {
		LLamaModelPath = envLlamaPath
	}
	envLlamaCtxLen := os.Getenv("LLAMA_CTX_LEN")
	if envLlamaCtxLen != "" {
		val, err := strconv.Atoi(envLlamaCtxLen)
		if err == nil {
			LLamaContextLen = val
		}
	}

	LLamaModel, err = llama.New(LLamaModelPath, llama.EnableF16Memory, llama.SetContext(LLamaContextLen), llama.EnableEmbeddings)
	if err != nil {
		fmt.Println("Loading LLama model got error:", err)
	} else {
		fmt.Printf("Load LLama model from %s with context length %d", LLamaModelPath, LLamaContextLen)
	}
}

func funcEmbedding(kv KVPair, args []Expression, ctx *ExecuteCtx) (any, error) {
	if args[0].ReturnType() != TSTR {
		return nil, NewExecuteError(args[0].GetPos(), "embedding function first parameter require string type")
	}
	rarg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return nil, err
	}
	text := toString(rarg)
	if LLamaModel == nil {
		return nil, fmt.Errorf("llama model not loaded")
	}
	embeds, err := LLamaModel.Embeddings(text)
	if err != nil {
		return nil, err
	}
	return embeds, nil
}
