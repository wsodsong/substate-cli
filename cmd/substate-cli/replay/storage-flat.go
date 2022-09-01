package replay

import (
	"fmt"
	"strconv"

	"github.com/ethereum/go-ethereum/substate"
	cli "gopkg.in/urfave/cli.v1"
)

// record-replay: substate-cli storage command
var GetStorageFlatDataCommand = cli.Command{
	Action:    getStorageFlatDataAction,
	Name:      "storage-flat",
	Usage:     "returns storage in flat data format",
	ArgsUsage: "<blockNumFirst> <blockNumLast>",
	Flags: []cli.Flag{
		substate.WorkersFlag,
		substate.SubstateDirFlag,
		ChainIDFlag,
	},
	Description: `
The substate-cli storage-flat command requires two arguments:
<blockNumFirst> <blockNumLast>

<blockNumFirst> and <blockNumLast> are the first and
last block of the inclusive range of blocks to replay transactions.

Output log format: (block, timestamp, transaction, account, in/out, storage address, storage value)`,
}

// getStorageFlatDataTask replays storage access of accounts in each transaction
func getStorageFlatDataTask(block uint64, tx int, st *substate.Substate, taskPool *substate.SubstateTaskPool) error {
	timestamp := st.Env.Timestamp
	var msg string
	for wallet, outputAlloc := range st.OutputAlloc {
		for storageAddress, value := range outputAlloc.Storage {
			msg += 	fmt.Sprintf("metric: %v,%v,%v,%v,%v,%v\n",block, timestamp, tx, wallet.Hex(), storageAddress.Hex(), value)
		}
	}
	fmt.Printf("%v", msg)
	return nil
}

// func getStorageFlatDataAction for replay storage-flat command
func getStorageFlatDataAction(ctx *cli.Context) error {
	var err error

	if len(ctx.Args()) != 2 {
		return fmt.Errorf("substate-cli storage command requires exactly 2 arguments")
	}

	chainID = ctx.Int(ChainIDFlag.Name)
	fmt.Printf("chain-id: %v\n",chainID)
	fmt.Printf("git-date: %v\n", gitDate)
	fmt.Printf("git-commit: %v\n",gitCommit)

	//if db, err := sql.Open("sqlite3", "/var/data/storage.db"), err {
	//	panic(err)
	//}

	first, ferr := strconv.ParseInt(ctx.Args().Get(0), 10, 64)
	last, lerr := strconv.ParseInt(ctx.Args().Get(1), 10, 64)
	if ferr != nil || lerr != nil {
		return fmt.Errorf("substate-cli storage-flat: error in parsing parameters: block number not an integer")
	}
	if first < 0 || last < 0 {
		return fmt.Errorf("substate-cli storage-flat: error: block number must be greater than 0")
	}
	if first > last {
		return fmt.Errorf("substate-cli storage-flat: error: first block has larger number than last block")
	}

	substate.SetSubstateFlags(ctx)
	substate.OpenSubstateDBReadOnly()
	defer substate.CloseSubstateDB()

	taskPool := substate.NewSubstateTaskPool("substate-cli storage-flat", getStorageFlatDataTask, uint64(first), uint64(last), ctx)
	err = taskPool.Execute()
	return err
}
