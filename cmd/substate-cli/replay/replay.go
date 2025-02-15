package replay

import (
	"bytes"
	"fmt"
	"math/big"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Fantom-foundation/go-opera/evmcore"
	"github.com/Fantom-foundation/go-opera/opera"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/core/vm/lfvm"
	_ "github.com/ethereum/go-ethereum/core/vm/lfvm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/substate"
	cli "gopkg.in/urfave/cli.v1"
)

var (
	gitCommit = "" // Git SHA1 commit hash of the release (set via linker flags)
	gitDate   = ""
)

// chain id
var chainID int
var ChainIDFlag = cli.IntFlag{
	Name:  "chainid",
	Usage: "ChainID for replayer",
	Value: 250,
}

var ProfileEVMCallFlag = cli.BoolFlag{
	Name:  "profiling-call",
	Usage: "enable profiling for EVM call",
}

var ProfileEVMOpCodeFlag = cli.BoolFlag{
	Name:  "profiling-opcode",
	Usage: "enable profiling for EVM opcodes",
}

var OnlySuccessfulFlag = cli.BoolFlag{
	Name:  "only-successful",
	Usage: "only runs transactions that have been successful",
}

var InterpreterImplFlag = cli.StringFlag{
	Name:  "interpreter",
	Usage: "select the interpreter version to be used",
}

var CpuProfilingFlag = cli.StringFlag{
	Name:  "cpuprofile",
	Usage: "the file name where to write a CPU profile of the evaluation step to",
}

var UseInMemoryStateDbFlag = cli.BoolFlag{
	Name:  "faststatedb",
	Usage: "enables a faster, yet still experimental StateDB implementation",
}

// record-replay: substate-cli replay command
var ReplayCommand = cli.Command{
	Action:    replayAction,
	Name:      "replay",
	Usage:     "executes full state transitions and check output consistency",
	ArgsUsage: "<blockNumFirst> <blockNumLast>",
	Flags: []cli.Flag{
		substate.WorkersFlag,
		substate.SkipTransferTxsFlag,
		substate.SkipCallTxsFlag,
		substate.SkipCreateTxsFlag,
		substate.SubstateDirFlag,
		ChainIDFlag,
		ProfileEVMCallFlag,
		ProfileEVMOpCodeFlag,
		InterpreterImplFlag,
		OnlySuccessfulFlag,
		CpuProfilingFlag,
		UseInMemoryStateDbFlag,
	},
	Description: `
The substate-cli replay command requires two arguments:
<blockNumFirst> <blockNumLast>

<blockNumFirst> and <blockNumLast> are the first and
last block of the inclusive range of blocks to replay transactions.`,
}

var vm_duration time.Duration

func resetVmDuration() {
	atomic.StoreInt64((*int64)(&vm_duration), 0)
}

func addVmDuration(delta time.Duration) {
	atomic.AddInt64((*int64)(&vm_duration), (int64)(delta))
}

func getVmDuration() time.Duration {
	return time.Duration(atomic.LoadInt64((*int64)(&vm_duration)))
}

type ReplayConfig struct {
	vm_impl          string
	only_successful  bool
	use_in_memory_db bool
}

// replayTask replays a transaction substate
func replayTask(config ReplayConfig, block uint64, tx int, recording *substate.Substate, taskPool *substate.SubstateTaskPool) error {

	// If requested, skip failed transactions.
	if config.only_successful && recording.Result.Status != types.ReceiptStatusSuccessful {
		return nil
	}

	inputAlloc := recording.InputAlloc
	inputEnv := recording.Env
	inputMessage := recording.Message

	outputAlloc := recording.OutputAlloc
	outputResult := recording.Result

	var (
		vmConfig    vm.Config
		chainConfig *params.ChainConfig
	)

	vmConfig = opera.DefaultVMConfig
	vmConfig.NoBaseFee = true

	chainConfig = params.AllEthashProtocolChanges
	chainConfig.ChainID = big.NewInt(int64(chainID))
	chainConfig.LondonBlock = new(big.Int).SetUint64(37534833)
	chainConfig.BerlinBlock = new(big.Int).SetUint64(37455223)

	var hashError error
	getHash := func(num uint64) common.Hash {
		if inputEnv.BlockHashes == nil {
			hashError = fmt.Errorf("getHash(%d) invoked, no blockhashes provided", num)
			return common.Hash{}
		}
		h, ok := inputEnv.BlockHashes[num]
		if !ok {
			hashError = fmt.Errorf("getHash(%d) invoked, blockhash for that block not provided", num)
		}
		return h
	}

	var statedb StateDB
	if config.use_in_memory_db {
		statedb = MakeInMemoryStateDB(&inputAlloc)
	} else {
		statedb = MakeOffTheChainStateDB(inputAlloc)
	}

	// Apply Message
	var (
		gaspool   = new(evmcore.GasPool)
		blockHash = common.Hash{0x01}
		txHash    = common.Hash{0x02}
		txIndex   = tx
	)

	gaspool.AddGas(inputEnv.GasLimit)
	blockCtx := vm.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		Coinbase:    inputEnv.Coinbase,
		BlockNumber: new(big.Int).SetUint64(inputEnv.Number),
		Time:        new(big.Int).SetUint64(inputEnv.Timestamp),
		Difficulty:  inputEnv.Difficulty,
		GasLimit:    inputEnv.GasLimit,
		GetHash:     getHash,
	}
	// If currentBaseFee is defined, add it to the vmContext.
	if inputEnv.BaseFee != nil {
		blockCtx.BaseFee = new(big.Int).Set(inputEnv.BaseFee)
	}

	msg := inputMessage.AsMessage()

	vmConfig.Tracer = nil
	vmConfig.Debug = false
	vmConfig.InterpreterImpl = config.vm_impl
	statedb.Prepare(txHash, txIndex)

	txCtx := evmcore.NewEVMTxContext(msg)

	evm := vm.NewEVM(blockCtx, txCtx, statedb, chainConfig, vmConfig)

	snapshot := statedb.Snapshot()
	start := time.Now()
	msgResult, err := evmcore.ApplyMessage(evm, msg, gaspool)
	addVmDuration(time.Since(start))

	if err != nil {
		statedb.RevertToSnapshot(snapshot)
		return err
	}

	if hashError != nil {
		return hashError
	}

	if chainConfig.IsByzantium(blockCtx.BlockNumber) {
		statedb.Finalise(true)
	} else {
		statedb.IntermediateRoot(chainConfig.IsEIP158(blockCtx.BlockNumber))
	}

	evmResult := &substate.SubstateResult{}
	if msgResult.Failed() {
		evmResult.Status = types.ReceiptStatusFailed
	} else {
		evmResult.Status = types.ReceiptStatusSuccessful
	}
	evmResult.Logs = statedb.GetLogs(txHash, blockHash)
	evmResult.Bloom = types.BytesToBloom(types.LogsBloom(evmResult.Logs))
	if to := msg.To(); to == nil {
		evmResult.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, msg.Nonce())
	}
	evmResult.GasUsed = msgResult.UsedGas

	evmAlloc := statedb.GetSubstatePostAlloc()

	r := outputResult.Equal(evmResult)
	a := outputAlloc.Equal(evmAlloc)
	if !(r && a) {
		fmt.Printf("block: %v Transaction: %v\n", block, tx)
		if !r {
			fmt.Printf("inconsistent output: result\n")
			printResultDiffSummary(outputResult, evmResult)
		}
		if !a {
			fmt.Printf("inconsistent output: alloc\n")
			printAllocationDiffSummary(&outputAlloc, &evmAlloc)
		}
		return fmt.Errorf("inconsistent output")
	}

	return nil
}

func printIfDifferent[T comparable](label string, want, have T) bool {
	if want != have {
		fmt.Printf("  Different %s:\n", label)
		fmt.Printf("    want: %v\n", want)
		fmt.Printf("    have: %v\n", have)
		return true
	}
	return false
}

func printIfDifferentBytes(label string, want, have []byte) bool {
	if !bytes.Equal(want, have) {
		fmt.Printf("  Different %s:\n", label)
		fmt.Printf("    want: %v\n", want)
		fmt.Printf("    have: %v\n", have)
		return true
	}
	return false
}

func printIfDifferentBigInt(label string, want, have *big.Int) bool {
	if want == nil && have == nil {
		return false
	}
	if want == nil || have == nil || want.Cmp(have) != 0 {
		fmt.Printf("  Different %s:\n", label)
		fmt.Printf("    want: %v\n", want)
		fmt.Printf("    have: %v\n", have)
		return true
	}
	return false
}

func printResultDiffSummary(want, have *substate.SubstateResult) {
	printIfDifferent("status", want.Status, have.Status)
	printIfDifferent("contract address", want.ContractAddress, have.ContractAddress)
	printIfDifferent("gas usage", want.GasUsed, have.GasUsed)
	printIfDifferent("log bloom filter", want.Bloom, have.Bloom)
	if !printIfDifferent("log size", len(want.Logs), len(have.Logs)) {
		for i := range want.Logs {
			printLogDiffSummary(fmt.Sprintf("log[%d]", i), want.Logs[i], have.Logs[i])
		}
	}
}

func printLogDiffSummary(label string, want, have *types.Log) {
	printIfDifferent(fmt.Sprintf("%s.address", label), want.Address, have.Address)
	if !printIfDifferent(fmt.Sprintf("%s.Topics size", label), len(want.Topics), len(have.Topics)) {
		for i := range want.Topics {
			printIfDifferent(fmt.Sprintf("%s.Topics[%d]", label, i), want.Topics[i], have.Topics[i])
		}
	}
	printIfDifferentBytes(fmt.Sprintf("%s.data", label), want.Data, have.Data)
}

func printAllocationDiffSummary(want, have *substate.SubstateAlloc) {
	printIfDifferent("substate alloc size", len(*want), len(*have))
	for key := range *want {
		_, present := (*have)[key]
		if !present {
			fmt.Printf("    missing key=%v\n", key)
		}
	}

	for key := range *have {
		_, present := (*want)[key]
		if !present {
			fmt.Printf("    extra key=%v\n", key)
		}
	}

	for key, is := range *have {
		should, present := (*want)[key]
		if present {
			printAccountDiffSummary(fmt.Sprintf("key=%v:", key), should, is)
		}
	}
}

func printAccountDiffSummary(label string, want, have *substate.SubstateAccount) {
	printIfDifferent(fmt.Sprintf("%s.Nonce", label), want.Nonce, have.Nonce)
	printIfDifferentBigInt(fmt.Sprintf("%s.Balance", label), want.Balance, have.Balance)
	printIfDifferentBytes(fmt.Sprintf("%s.Code", label), want.Code, have.Code)

	printIfDifferent(fmt.Sprintf("len(%s.Storage)", label), len(want.Storage), len(have.Storage))
	for key := range want.Storage {
		_, present := have.Storage[key]
		if !present {
			fmt.Printf("    %s.Storage misses key %v\n", label, key)
		}
	}

	for key := range have.Storage {
		_, present := want.Storage[key]
		if !present {
			fmt.Printf("    %s.Storage has extra key %v\n", label, key)
		}
	}

	for key, is := range have.Storage {
		should, present := want.Storage[key]
		if present {
			printIfDifferent(fmt.Sprintf("%s.Storage[%v]", label, key), should, is)
		}
	}
}

// record-replay: func replayAction for replay command
func replayAction(ctx *cli.Context) error {
	var err error

	if len(ctx.Args()) != 2 {
		return fmt.Errorf("substate-cli replay command requires exactly 2 arguments")
	}

	chainID = ctx.Int(ChainIDFlag.Name)
	fmt.Printf("chain-id: %v\n", chainID)
	fmt.Printf("git-date: %v\n", gitDate)
	fmt.Printf("git-commit: %v\n", gitCommit)

	first, ferr := strconv.ParseInt(ctx.Args().Get(0), 10, 64)
	last, lerr := strconv.ParseInt(ctx.Args().Get(1), 10, 64)
	if ferr != nil || lerr != nil {
		return fmt.Errorf("substate-cli replay: error in parsing parameters: block number not an integer")
	}
	if first < 0 || last < 0 {
		return fmt.Errorf("substate-cli replay: error: block number must be greater than 0")
	}
	if first > last {
		return fmt.Errorf("substate-cli replay: error: first block has larger number than last block")
	}

	if ctx.Bool(ProfileEVMCallFlag.Name) {
		vm.ProfileEVMCall = true
	}
	if ctx.Bool(ProfileEVMOpCodeFlag.Name) {
		vm.ProfileEVMOpCode = true
	}

	substate.SetSubstateFlags(ctx)
	substate.OpenSubstateDBReadOnly()
	defer substate.CloseSubstateDB()

	// Start CPU profiling if requested.
	profile_file_name := ctx.String(CpuProfilingFlag.Name)
	if profile_file_name != "" {
		f, err := os.Create(profile_file_name)
		if err != nil {
			return err
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var config = ReplayConfig{
		vm_impl:          ctx.String(InterpreterImplFlag.Name),
		only_successful:  ctx.Bool(OnlySuccessfulFlag.Name),
		use_in_memory_db: ctx.Bool(UseInMemoryStateDbFlag.Name),
	}

	task := func(block uint64, tx int, recording *substate.Substate, taskPool *substate.SubstateTaskPool) error {
		return replayTask(config, block, tx, recording, taskPool)
	}

	resetVmDuration()
	taskPool := substate.NewSubstateTaskPool("substate-cli replay", task, uint64(first), uint64(last), ctx)
	err = taskPool.Execute()

	fmt.Printf("substate-cli replay: net VM time: %v\n", getVmDuration())

	if ctx.Bool(ProfileEVMOpCodeFlag.Name) {
		vm.PrintStatistics()
	}
	if strings.HasSuffix(ctx.String(InterpreterImplFlag.Name), "-stats") {
		lfvm.PrintCollectedInstructionStatistics()
	}
	return err
}
